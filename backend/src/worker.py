import os,time,json,logging
from pathlib import Path
import redis
from sqlalchemy import update
from src.config.database import Session
from src.models.entities import Study
from src.services.analysis import analyze
from src.services.within_route import run_study
r=redis.Redis.from_url(os.environ['REDIS_URL'])
ART=os.environ.get('ARTIFACTS_DIR','/artifacts')
def heartbeat():Path('/tmp/heartbeat').touch()
with Session() as recovery:
 for pending in recovery.query(Study).filter(Study.state.in_(['queued','running'])).all():
  pending.state='queued';r.rpush('studies',pending.id)
 recovery.commit()
while True:
 heartbeat()
 try:
  item=r.blpop('studies',timeout=3)
  if not item:continue
  sid=int(item[1])
  with Session() as s:
   claimed=s.execute(update(Study).where(Study.id==sid,Study.state=='queued').values(state='running')).rowcount
   s.commit()
   if not claimed:continue
   row=s.get(Study,sid)
   cfg=row.result or {}
   if cfg.get('kind')=='within_route':
    run_study(Session,sid,heartbeat=heartbeat,artifact_dir=ART)
    continue
   snapshot=cfg['snapshot']
   try:
    row.result={**analyze(snapshot,row.feature),'snapshot':snapshot};row.state='done'
    Path(ART,str(row.id)+'.json').write_text(json.dumps(row.result,ensure_ascii=False))
   except Exception as e:row.state='failed';row.error=str(e)
   s.commit()
 except Exception:logging.exception('worker');time.sleep(2)
