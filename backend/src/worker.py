import json,time,logging
from redis import Redis
from sqlalchemy import select
from .config.database import Session
from .config.settings import REDIS_URL,ARTIFACT_DIR
from .models.run import Run
from .models.federation import FedMerge
from .models import source,record,user,note,audit,federation
from .services.algorithm import calculate
from .services.audit import log
from .services.export import csv_response
from .services.federation import run_merge
queue=Redis.from_url(REDIS_URL)
ARTIFACT_DIR.mkdir(parents=True,exist_ok=True)
def execute(id):
    with Session() as db:
        r=db.get(Run,id)
        if not r or r.state!='queued':return
        r.state='running';db.commit()
        try:
            result=calculate(r.snapshot['records'],r.parameters)
            json.dumps(result,allow_nan=False)
            r.result=result;r.state='completed'
            response=csv_response(result,'run.csv')
            (ARTIFACT_DIR/f'run-{r.id}.csv').write_bytes(response.body)
            log(db,'worker','run.completed',r.id)
        except Exception as e:
            logging.exception('analysis failed');r.state='failed';r.error=str(e)[:2000]
            log(db,'worker','run.failed',r.id,r.error)
        db.commit()
def execute_merge(id):
    with Session() as db:run_merge(db,id)
def recover_merges():
    with Session() as db:
        stale=db.scalars(select(FedMerge).where(FedMerge.state.in_(('queued','running')))).all()
        for m in stale:m.state='queued'
        db.commit()
        for m in stale:
            try:queue.rpush('federation:queue',m.id)
            except Exception:logging.exception('federation requeue unavailable')
        if stale:logging.info('recovered %d interrupted federation merges',len(stale))
def main():
    recover_merges()
    while True:
        (ARTIFACT_DIR/'heartbeat').write_text(str(time.time()))
        value=queue.blpop(['analysis:queue','federation:queue'],timeout=2)
        if not value:continue
        if value[0].decode().endswith('federation:queue'):execute_merge(int(value[1]))
        else:execute(int(value[1]))
if __name__=='__main__':main()
