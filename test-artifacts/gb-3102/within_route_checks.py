# 线路内关联研究验证：辛普森悖论方向反转、断点续跑、并发去重、旧口径兼容。
# 运行：python3 test-artifacts/gb-3102/within_route_checks.py  （证据写入同目录 within_route_evidence.json）
import os,sys,json,time,threading,tempfile,traceback
ROOT=os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..'))
TMP=tempfile.mkdtemp(prefix='within-route-')
os.environ.update(DATABASE_URL='sqlite:///'+os.path.join(TMP,'boot.db'),REDIS_URL='redis://localhost:6379/0',JWT_SECRET='checks',SEED_DEMO='false')
sys.path.insert(0,os.path.join(ROOT,'backend'))
from sqlalchemy import create_engine,text,update
from sqlalchemy.orm import sessionmaker
import src.config.database as dbmod
from src.models.entities import Base,User,Shipment,Sample,Study,StudyBatch
from src.services.within_route import run_study,batch_exceed,prepare,BATCH_SIZE
from src.services.analysis import analyze
from src.utils.security import hash_password
EVIDENCE={}
def fresh_db(name):
 eng=create_engine('sqlite:///'+os.path.join(TMP,name+'.db'),connect_args={'timeout':30})
 dbmod.engine=eng;dbmod.Session=sessionmaker(bind=eng,expire_on_commit=False)
 Base.metadata.create_all(eng)
 with eng.connect() as c:c.exec_driver_sql('PRAGMA journal_mode=WAL');c.commit()
 return dbmod.Session
def simpson_rows():
 """五条线路：A/B/C 线内斜率为正、线路基线随暴露升高而降低（混算反转）；D 配对不足；E 常量。"""
 rows=[]
 def add(route,x,y):
  rows.append({'route':route,'exposure':x,'excursions':None,'loss':y})
 for route,xs,base in [('验证线路 A',range(60,80,2),1.0),('验证线路 B',range(40,60,2),3.0),('验证线路 C',range(20,40,2),5.0)]:
  mid=sum(xs)/len(xs)
  for j,x in enumerate(xs):add(route,float(x),round(base+0.05*(x-mid)+((j*7)%5-2)*0.04,4))
 add('验证线路 D',30.0,2.0);add('验证线路 D',None,2.1)
 for y in (2.0,2.5,3.0):add('验证线路 E',20.0,y)
 return rows
def make_study(Session,rows,seed=7,permutations=2000,batch_size=500):
 cfg={'kind':'within_route','version':'within-route-permutation-1.0','seed':seed,'permutations':permutations,'batch_size':batch_size,'snapshot':rows}
 with Session() as s:
  r=Study(name='验证研究',zone='冷藏',feature='exposure',result=cfg);s.add(r);s.commit();return r.id
def batches(Session,sid):
 with Session() as s:return s.query(StudyBatch).filter_by(study_id=sid).all()
def result_of(Session,sid):
 with Session() as s:return dict(s.get(Study,sid).result)
def check_simpson():
 Session=fresh_db('simpson');rows=simpson_rows();sid=make_study(Session,rows,permutations=5000)
 t0=time.time();res=run_study(Session,sid,artifact_dir=TMP);dt=time.time()-t0
 assert res['raw_pearson']<0<res['within_pearson'],f"方向未反转: raw={res['raw_pearson']} within={res['within_pearson']}"
 routes={r['route']:r for r in res['routes']}
 assert routes['验证线路 A']['included'] and routes['验证线路 B']['included'] and routes['验证线路 C']['included']
 assert not routes['验证线路 D']['included'] and '不足 2 条' in routes['验证线路 D']['reason']
 assert not routes['验证线路 E']['included'] and '常量' in routes['验证线路 E']['reason']
 assert res['perm_p']<0.01 and res['n']==30 and res['excluded']==len(rows)-30
 assert os.path.exists(os.path.join(TMP,str(sid)+'.json'))
 EVIDENCE['simpson']={'raw_pearson':res['raw_pearson'],'within_pearson':res['within_pearson'],'perm_p':res['perm_p'],'permutations':5000,'seconds':round(dt,2),'routes':res['routes']}
def check_interrupt():
 Session=fresh_db('interrupt');rows=simpson_rows()
 ref=run_study(Session,make_study(Session,rows),artifact_dir=TMP)
 before=make_study(Session,rows)
 assert run_study(Session,before,should_stop=lambda:True,artifact_dir=TMP) is None
 assert len(batches(Session,before))==0,'保存进度前中断不应留下批次'
 assert run_study(Session,before,artifact_dir=TMP)==ref,'保存进度前中断后续跑结果不一致'
 after=make_study(Session,rows);calls={'n':0}
 def stop():calls['n']+=1;return calls['n']>2
 assert run_study(Session,after,should_stop=stop,artifact_dir=TMP) is None
 assert len(batches(Session,after))==2,'保存进度后中断应保留已提交批次'
 assert run_study(Session,after,artifact_dir=TMP)==ref,'保存进度后中断后续跑结果不一致'
 EVIDENCE['interrupt']={'batches_at_first_stop':0,'batches_at_second_stop':2,'resume_matches_uninterrupted':True}
def check_concurrency():
 Session=fresh_db('concurrent');rows=simpson_rows()
 ref=run_study(Session,make_study(Session,rows,permutations=2000,batch_size=100),artifact_dir=TMP)
 sid=make_study(Session,rows,permutations=2000,batch_size=100)
 out=[]
 def work():out.append(run_study(Session,sid,artifact_dir=TMP))
 ts=[threading.Thread(target=work) for _ in range(3)]
 [t.start() for t in ts];[t.join() for t in ts]
 bs=batches(Session,sid)
 assert len(bs)==20 and sum(b.n for b in bs)==2000,'并发领取出现重复或丢失批次'
 assert out[0]==out[1]==ref,'并发结果与同种子不中断运行不一致'
 again=run_study(Session,sid,artifact_dir=TMP)
 assert len(batches(Session,sid))==20 and again==ref,'重试重复累计'
 with Session() as s:
  won=s.execute(update(Study).where(Study.id==sid,Study.state=='queued').values(state='running')).rowcount;s.commit()
 assert won==0,'已完成研究不应被重复领取'
 EVIDENCE['concurrency']={'workers':3,'batches':len(bs),'permutations_counted':sum(b.n for b in bs),'matches_uninterrupted':True,'retry_double_count':False}
def check_api():
 import redis as redislib,fakeredis
 from fastapi.testclient import TestClient
 fake=fakeredis.FakeStrictRedis();redislib.Redis.from_url=lambda *a,**k:fake
 Session=fresh_db('api')
 with Session() as s:
  for role in ('admin','analyst','viewer'):s.add(User(email=role+'@demo.local',role=role,password=hash_password('Demo123!')))
  for i,row in enumerate(simpson_rows()):
   code=f'YZ-{i:03}'
   ship=s.query(Shipment).filter_by(code=code).first()
   if not ship:ship=Shipment(code=code,route=row['route'],zone='冷藏',driver='验证员');s.add(ship);s.flush()
   s.add(Sample(shipment_id=ship.id,code=code+'-S',exposure=row['exposure'],excursions=row['excursions'],loss=row['loss']))
  s.commit()
 from src.main import app
 with TestClient(app) as c:
  tok=c.post('/auth/login',json={'email':'analyst@demo.local','password':'Demo123!'}).json()['token']
  h={'Authorization':'Bearer '+tok}
  old=c.post('/studies',json={'name':'旧口径','zone':'冷藏','feature':'exposure'},headers=h).json()
  assert 'kind' not in old['result'] and 'route' not in old['result']['snapshot'][0],'旧口径快照结构被改变'
  new=c.post('/studies',json={'name':'线内校正','zone':'冷藏','feature':'exposure','kind':'within_route','seed':7,'permutations':1000},headers=h).json()
  assert new['result']['kind']=='within_route' and new['result']['snapshot'][0]['route'],'新口径未冻结线路归属'
  assert new['progress']=={'done':0,'total':1000,'batches_done':0,'batches_total':2}
  assert c.post('/studies',json={'name':'超限','zone':'冷藏','kind':'within_route','permutations':200000},headers=h).status_code==422
  listed=c.get('/studies',headers=h).json();item=[x for x in listed if x['id']==new['id']][0]
  assert item['progress']['done']==0 and item['progress']['total']==1000
  assert c.get(f"/studies/{new['id']}/export",headers=h).status_code==409,'置换未完成就开放下载'
  run_study(Session,new['id'],artifact_dir=TMP)
  done=c.get(f"/studies/{new['id']}",headers=h).json()
  assert done['state']=='done' and done['progress']['done']==1000
  assert done['result']['raw_pearson']<0<done['result']['within_pearson']
  exp=c.get(f"/studies/{new['id']}/export",headers=h)
  assert exp.status_code==200 and exp.json()['result']['within_pearson']==done['result']['within_pearson']
  oldres={**analyze(old['result']['snapshot'],'exposure'),'snapshot':old['result']['snapshot']}
  with Session() as s:
   r=s.get(Study,old['id']);r.result=oldres;r.state='done';s.commit()
  got=c.get(f"/studies/{old['id']}",headers=h).json()
  assert got['result']['version']=='pearson-spearman-1.0' and 'progress' not in got,'旧研究未按原口径读取'
  assert c.get(f"/studies/{old['id']}/export",headers=h).status_code==200
  vt=c.post('/auth/login',json={'email':'viewer@demo.local','password':'Demo123!'}).json()['token']
  assert c.post('/studies',json={'name':'只读','zone':'冷藏'},headers={'Authorization':'Bearer '+vt}).status_code==403
 EVIDENCE['api']={'old_shape_preserved':True,'route_frozen':True,'export_gated_until_done':True,'over_limit_rejected_422':True,'viewer_forbidden_403':True,'progress_visible':True}
def check_full_scale():
 Session=fresh_db('fullscale');sid=make_study(Session,simpson_rows(),permutations=100000)
 t0=time.time();res=run_study(Session,sid,artifact_dir=TMP);dt=time.time()-t0
 assert sum(b.n for b in batches(Session,sid))==100000
 EVIDENCE['full_scale']={'permutations':100000,'batches':len(batches(Session,sid)),'seconds':round(dt,2),'within_pearson':res['within_pearson'],'perm_p':res['perm_p']}
def check_worker():
 import redis as redislib,fakeredis,importlib
 fake=fakeredis.FakeStrictRedis();redislib.Redis.from_url=lambda *a,**k:fake
 os.environ['ARTIFACTS_DIR']=TMP
 Session=fresh_db('worker')
 rows=simpson_rows()
 with Session() as s:
  wr={'kind':'within_route','version':'within-route-permutation-1.0','seed':7,'permutations':2000,'batch_size':500,'snapshot':rows}
  s.add(Study(name='线内',zone='冷藏',feature='exposure',result=wr));s.add(Study(name='混算',zone='冷藏',feature='exposure',result={'snapshot':rows}));s.commit()
  ids=[r.id for r in s.query(Study).all()]
 def boot():
  import src.worker
 t=threading.Thread(target=boot,daemon=True);t.start()
 deadline=time.time()+60
 while time.time()<deadline:
  with Session() as s:
   states={r.id:r.state for r in s.query(Study).all()}
  if all(v=='done' for v in states.values()):break
  time.sleep(0.2)
 assert all(v=='done' for v in states.values()),f'worker 未完成: {states}'
 with Session() as s:
  pooled=s.get(Study,ids[1]).result;within=s.get(Study,ids[0]).result
 assert pooled['version']=='pearson-spearman-1.0' and 'pearson' in pooled,'旧口径计算路径被改变'
 assert within['kind']=='within_route' and within['raw_pearson']<0<within['within_pearson']
 assert sum(b.n for b in batches(Session,ids[0]))==2000
 assert os.path.exists(os.path.join(TMP,str(ids[0])+'.json')) and os.path.exists(os.path.join(TMP,str(ids[1])+'.json'))
 EVIDENCE['worker']={'recovery_and_claim':True,'pooled_path_unchanged':True,'within_path_done':True,'artifacts':2}
CHECKS=[('simpson',check_simpson),('interrupt',check_interrupt),('concurrency',check_concurrency),('api',check_api),('full_scale',check_full_scale),('worker',check_worker)]
if __name__=='__main__':
 failed=0
 for name,fn in CHECKS:
  try:fn();print(f'PASS {name}')
  except Exception:failed+=1;print(f'FAIL {name}');traceback.print_exc()
 out=os.path.join(os.path.dirname(__file__),'within_route_evidence.json')
 with open(out,'w') as f:json.dump({'checks':[n for n,_ in CHECKS],'evidence':EVIDENCE,'tmp':TMP},f,ensure_ascii=False,indent=2)
 print('evidence ->',out);sys.exit(1 if failed else 0)
