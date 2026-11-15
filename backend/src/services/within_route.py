import math,random,json,logging
from pathlib import Path
from sqlalchemy import update,func
from sqlalchemy.exc import IntegrityError
from src.models.entities import Study,StudyBatch
ALGORITHM_VERSION='within-route-permutation-1.0'
BATCH_SIZE=500
DEFAULT_SEED=3102
DEFAULT_PERMUTATIONS=100000
MAX_PERMUTATIONS=100000
MIN_PERMUTATIONS=100
def pearson(xs,ys):
 n=len(xs);mx=math.fsum(xs)/n;my=math.fsum(ys)/n
 dx=[v-mx for v in xs];dy=[v-my for v in ys]
 sxx=math.fsum(v*v for v in dx);syy=math.fsum(v*v for v in dy)
 if sxx<=0 or syy<=0:return None
 return math.fsum(a*b for a,b in zip(dx,dy))/math.sqrt(sxx*syy)
def prepare(snapshot,feature):
 """按冻结快照分组线路：剔除缺失配对，逐线路扣除均值，给出纳入/排除原因与观测统计。"""
 routes={}
 order=[]
 for row in snapshot:
  key=row.get('route') or '未标注线路'
  if key not in routes:routes[key]=[];order.append(key)
  routes[key].append(row)
 stats=[];x_dm=[];y_dm=[];spans=[];raw_x=[];raw_y=[]
 for key in order:
  rows=routes[key]
  valid=[r for r in rows if r.get(feature) is not None and r.get('loss') is not None]
  missing=len(rows)-len(valid)
  xs=[float(r[feature]) for r in valid];ys=[float(r['loss']) for r in valid]
  raw_x+=xs;raw_y+=ys
  reason=None
  if len(valid)<2:reason='有效配对不足 2 条，无法在线路内扣除均值'
  elif len(set(xs))<2 or len(set(ys))<2:reason='线路内变量为常量，对线内相关无贡献'
  included=reason is None
  stats.append({'route':key,'pairs':len(valid),'missing':missing,'included':included,'reason':reason})
  if included:
   mx=math.fsum(xs)/len(xs);my=math.fsum(ys)/len(ys)
   start=len(x_dm)
   x_dm+=[v-mx for v in xs];y_dm+=[v-my for v in ys]
   spans.append((start,len(x_dm)))
 if len(x_dm)<3:raise ValueError('线路内有效配对不足 3 条，补齐样本后重新创建研究')
 if not spans:raise ValueError('没有可纳入的线路：所有线路均因配对不足或常量被排除')
 sxx=math.fsum(v*v for v in x_dm);syy=math.fsum(v*v for v in y_dm)
 if sxx<=0 or syy<=0:raise ValueError('扣除线路均值后变量为常量，无法计算线路内相关')
 sxy=math.fsum(a*b for a,b in zip(x_dm,y_dm))
 raw=pearson(raw_x,raw_y) if len(raw_x)>=3 else None
 return {'routes':stats,'x_dm':x_dm,'y_dm':y_dm,'spans':spans,'sxx':sxx,'syy':syy,'sxy_obs':sxy,
 'n':len(x_dm),'raw_n':len(raw_x),'raw_pearson':raw,'within_pearson':sxy/math.sqrt(sxx*syy),'total':len(snapshot)}
def batch_exceed(prep,seed,batch_index,count):
 """计算一个置换批次。批次只由 (seed, batch_index) 决定，与执行进程和顺序无关，保证断点续跑与并发领取结果一致。"""
 rng=random.Random(f'{seed}:{batch_index}')
 x=prep['x_dm'];base=prep['y_dm'];spans=prep['spans'];obs=abs(prep['sxy_obs']);exceed=0
 for _ in range(count):
  y=base[:]
  for a,b in spans:
   seg=y[a:b];rng.shuffle(seg);y[a:b]=seg
  if abs(math.fsum(a*b for a,b in zip(x,y)))>=obs:exceed+=1
 return exceed
def finalize_result(prep,cfg,exceed):
 p=cfg['permutations']
 return {'kind':'within_route','version':ALGORITHM_VERSION,'seed':cfg['seed'],'permutations':p,'batch_size':cfg['batch_size'],
 'n':prep['n'],'excluded':prep['total']-prep['n'],'total':prep['total'],'raw_n':prep['raw_n'],
 'raw_pearson':None if prep['raw_pearson'] is None else round(prep['raw_pearson'],6),
 'within_pearson':round(prep['within_pearson'],6),
 'exceed':exceed,'perm_p':(exceed+1)/(p+1),
 'routes':prep['routes'],
 'points':[{'x':a,'y':b} for a,b in zip(prep['x_dm'],prep['y_dm'])],
 'warning':'线路内口径：样本先扣除各自线路均值再聚合相关，显著性由线路内打乱货损的置换检验估计。原始关联为跨线路混算，两者方向可能相反（辛普森悖论）。相关性不代表因果。研究使用创建时冻结的样本与线路归属。'}
def progress_of(s,study_id,permutations,batch_size):
 c,sn=s.query(func.count(),func.coalesce(func.sum(StudyBatch.n),0)).filter(StudyBatch.study_id==study_id).one()
 done=min(int(sn),permutations)
 return {'done':done,'total':permutations,'batches_done':int(c),'batches_total':(permutations+batch_size-1)//batch_size}
def run_study(Session,study_id,should_stop=None,heartbeat=None,artifact_dir='/artifacts'):
 """分批执行置换并逐批落库；中断后重入跳过已提交批次；并发进程靠 (study_id,batch) 主键去重，只记一次。"""
 with Session() as s:
  row=s.get(Study,study_id)
  if not row:return None
  cfg=row.result or {}
  try:prep=prepare(cfg.get('snapshot') or [],row.feature)
  except ValueError as e:
   row.state='failed';row.error=str(e);s.commit();return None
 p=cfg['permutations'];b=cfg['batch_size'];nb=(p+b-1)//b
 while True:
  with Session() as s:
   done={r.batch for r in s.query(StudyBatch.batch).filter(StudyBatch.study_id==study_id)}
  missing=[i for i in range(nb) if i not in done]
  if not missing:break
  for i in missing:
   if should_stop and should_stop():return None
   exceed=batch_exceed(prep,cfg['seed'],i,min(b,p-i*b))
   with Session() as s:
    s.add(StudyBatch(study_id=study_id,batch=i,n=min(b,p-i*b),exceed=exceed))
    try:s.commit()
    except IntegrityError:s.rollback()
   if heartbeat:heartbeat()
 with Session() as s:
  total_n,exceed=s.query(func.coalesce(func.sum(StudyBatch.n),0),func.coalesce(func.sum(StudyBatch.exceed),0)).filter(StudyBatch.study_id==study_id).one()
  if int(total_n)<p:return None
  result=finalize_result(prep,cfg,int(exceed))
  claimed=s.execute(update(Study).where(Study.id==study_id,Study.state!='done').values(state='done',result={**result,'snapshot':cfg['snapshot']})).rowcount
  s.commit()
  if claimed:
   try:Path(artifact_dir,str(study_id)+'.json').write_text(json.dumps({**result,'snapshot':cfg['snapshot']},ensure_ascii=False))
   except Exception:logging.exception('artifact')
  return result
