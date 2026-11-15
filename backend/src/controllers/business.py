from fastapi import HTTPException
from src.models.entities import Shipment,Sample,Study,StudyBatch
from src.services.business import dump
from src.services.within_route import progress_of
def ensure_shipment(s,i):
 if not s.get(Shipment,i):raise HTTPException(404,'班次不存在')
def study_rows(s,zone):
 ids=[r.id for r in s.query(Shipment).filter_by(zone=zone).all()]
 return [dump(r) for r in s.query(Sample).filter(Sample.shipment_id.in_(ids)).all()]
def study_rows_route(s,zone):
 rows=s.query(Sample,Shipment.route).join(Shipment,Sample.shipment_id==Shipment.id).filter(Shipment.zone==zone).all()
 return [{**dump(sm),'route':route} for sm,route in rows]
def ensure_study(s,i):
 row=s.get(Study,i)
 if not row:raise HTTPException(404,'研究不存在')
 return row
def attach_progress(s,studies):
 for d in studies:
  cfg=d.get('result') or {}
  if cfg.get('kind')=='within_route':d['progress']=progress_of(s,d['id'],cfg['permutations'],cfg['batch_size'])
 return studies
