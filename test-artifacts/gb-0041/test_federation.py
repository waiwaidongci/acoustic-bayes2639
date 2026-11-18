# 联邦汇总训练端到端验证：oracle 一致性、分片版本语义、封轮/发布并发、中断恢复、数值稳定性、原研究回归。
# 运行：python3 test-artifacts/gb-0041/test_federation.py（依赖 fastapi/httpx/sqlalchemy/numpy/scikit-learn；使用临时 SQLite，队列以内存桩替代）
import os,sys,json,math,shutil,tempfile
TMP=tempfile.mkdtemp(prefix='fedtest-')
os.environ['DATABASE_URL']='sqlite:///'+TMP+'/test.db'
os.environ['ARTIFACT_DIR']=TMP+'/artifacts'
os.environ['SEED_DEMO']='false'
os.environ['JWT_SECRET']='federation-test-secret'
sys.path.insert(0,os.path.join(os.path.dirname(__file__),'..','..','backend'))
import numpy as np
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.naive_bayes import GaussianNB
from fastapi.testclient import TestClient
from src.config.database import Base,engine,Session
from src.models.user import User
from src.models.federation import FedMerge
from src.utils.security import hash_password
from src.services import federation,runs
import src.worker as worker
class FakeQueue:
    def __init__(self):self.items=[]
    def rpush(self,q,v):self.items.append((q,int(v)));return 1
    def blpop(self,keys,timeout=2):return self.items.pop(0) if self.items else None
    def ping(self):return True
federation.queue=runs.queue=worker.queue=FakeQueue()
Base.metadata.create_all(engine)
with Session() as db:
    for role in ('admin','analyst','viewer'):
        db.add(User(email=role+'@t.demo',password_hash=hash_password('Research2026!'),role=role))
    db.commit()
from src.main import app
client=TestClient(app)
def token(role):
    r=client.post('/auth/login',json={'email':role+'@t.demo','password':'Research2026!'})
    assert r.status_code==200,r.text
    return {'Authorization':'Bearer '+r.json()['token']}
ADMIN,ANALYST,VIEWER=token('admin'),token('analyst'),token('viewer')
FEATURES=['rms','centroid','kurtosis'];LABELS=['正常','轴承磨损','转子偏心']
def gen_rows(rng,per_class,means,stds):
    rows=[]
    for li,label in enumerate(LABELS):
        for _ in range(per_class):
            row={'label':label}
            for fi,f in enumerate(FEATURES):row[f]=float(rng.normal(means[li][fi],stds[li][fi]))
            rows.append(row)
    return rows
def stats_of(rows):return federation.local_stats([{'values':r} for r in rows],FEATURES)
def sklearn_posterior_max(all_rows,batch_rows,smoothing=1e-9):
    X=np.array([[r[f] for f in FEATURES] for r in all_rows]);y=np.array([r['label'] for r in all_rows])
    pipe=make_pipeline(StandardScaler(),GaussianNB(var_smoothing=smoothing));pipe.fit(X,y)
    proba=pipe.predict_proba(np.array([[r[f] for f in FEATURES] for r in batch_rows]))
    return proba.max(axis=1),np.array([pipe.classes_[i] for i in proba.argmax(axis=1)])
def welford_ref(all_rows):
    out={}
    for label in LABELS:
        sel=[r for r in all_rows if r['label']==label];out[label]={}
        for f in FEATURES:
            n=0;mean=np.longdouble(0);m2=np.longdouble(0)
            for r in sel:
                n+=1;x=np.longdouble(r[f]);delta=x-mean;mean+=delta/n;m2+=delta*(x-mean)
            out[label][f]=(n,mean,m2)
    return out
def make_source(name,rows):
    r=client.post('/sources',headers=ANALYST,json={'name':name,'filename':name+'.csv','description':'验证批次'})
    assert r.status_code==200,r.text
    sid=r.json()['id']
    recs=[{'reference':name+f'-{i+1:04}','values':row} for i,row in enumerate(rows)]
    r=client.post(f'/sources/{sid}/records',headers=ANALYST,json={'records':recs})
    assert r.status_code==200,r.text
    return sid
def create_round(name,stations,sids,smoothing=1e-9):
    r=client.post('/federation/rounds',headers=ANALYST,json={'name':name,'stations':stations,'interpret_source_ids':sids,'smoothing':smoothing})
    assert r.status_code==200,r.text
    return r.json()['id']
def contribute(rid,station,shard,version,coverage,stats,expect=200):
    r=client.post(f'/federation/rounds/{rid}/contributions',headers=ANALYST,json={'station':station,'shard':shard,'version':version,'coverage':coverage,'stats':stats})
    assert r.status_code==expect,(r.status_code,r.text)
    return r.json() if expect==200 else r.json()['detail']
def seal_merge(rid):
    r=client.post(f'/federation/rounds/{rid}/seal',headers=ANALYST)
    assert r.status_code==200,r.text
    mid=r.json()['id']
    assert ('federation:queue',mid) in federation.queue.items
    with Session() as db:federation.run_merge(db,mid)
    return mid
def publish(rid,expect=200):
    r=client.post(f'/federation/rounds/{rid}/publish',headers=ADMIN)
    assert r.status_code==expect,(r.status_code,r.text)
    return r.json() if expect==200 else r.json()['detail']
passed=[]
def check(name,cond,info=''):
    assert cond,f'{name}: {info}'
    passed.append(name);print('  ✓',name)
print('== A. 建轮与输入校验 ==')
r=client.post('/federation/rounds',headers=VIEWER,json={'name':'越权轮','stations':['甲站'],'interpret_source_ids':[1]})
check('阅读者建轮被拒 403',r.status_code==403)
rng=np.random.default_rng(7)
MEANS=[[1.2,900,2.8],[3.0,1600,5.2],[2.0,2300,3.9]];STDS=[[.35,170,.45],[.4,150,.5],[.3,160,.4]]
station_rows={'北郊站':gen_rows(rng,80,MEANS,STDS),'临港站':gen_rows(rng,90,[[m[0]+.05,m[1]+20,m[2]+.1] for m in MEANS],STDS),'山南站':gen_rows(rng,70,[[m[0]-.04,m[1]-15,m[2]-.05] for m in MEANS],STDS)}
batch_rows=gen_rows(np.random.default_rng(11),40,MEANS,STDS)
batch_id=make_source('判读批次A',batch_rows)
r=client.post('/federation/rounds',headers=ANALYST,json={'name':'坏平滑','stations':['甲站'],'interpret_source_ids':[batch_id],'smoothing':5})
check('平滑系数越界 422',r.status_code==422)
r=client.post('/federation/rounds',headers=ANALYST,json={'name':'坏批次','stations':['甲站'],'interpret_source_ids':[99999]})
check('判读批次不存在 404',r.status_code==404)
rid=create_round('验证轮一',list(station_rows),[batch_id])
d=client.get(f'/federation/rounds/{rid}',headers=VIEWER).json()
check('新轮缺站全部列出',d['missing_stations']==['北郊站','临港站','山南站'] and d['epoch']==0)
st=stats_of(station_rows['北郊站'])
bad=json.loads(json.dumps(st));bad['正常']['rms']['m2']=-1
check('负离均差平方和 422','离均差平方和' in contribute(rid,'北郊站','p1',1,[0,10],bad,422))
bad2=json.loads(json.dumps(st));del bad2['正常']['kurtosis']
check('特征口径不符 422','口径' in contribute(rid,'北郊站','p1',1,[0,10],bad2,422))
check('未冻结站点 422','冻结名单' in contribute(rid,'幽灵站','p1',1,[0,10],st,422))
check('样本数与覆盖不符 422','覆盖区间' in contribute(rid,'北郊站','p1',1,[0,10],st,422))
print('== B. 分片版本语义 ==')
shards={s:[rows[:len(rows)//2],rows[len(rows)//2:]] for s,rows in station_rows.items()}
n1=len(shards['北郊站'][0]);n2=len(shards['北郊站'][1])
c=contribute(rid,'北郊站','part-1',1,[0,n1],stats_of(shards['北郊站'][0]))
check('首片接收',c['state']=='accepted' and c['samples']==n1)
epoch1=client.get(f'/federation/rounds/{rid}',headers=VIEWER).json()['epoch']
c=contribute(rid,'北郊站','part-1',1,[0,n1],stats_of(shards['北郊站'][0]))
check('同版同内容重试幂等',c.get('deduplicated')==True)
check('重试不推进 epoch',client.get(f'/federation/rounds/{rid}',headers=VIEWER).json()['epoch']==epoch1)
check('同版改写被拒 409','版本号' in contribute(rid,'北郊站','part-1',1,[0,n1],stats_of(shards['北郊站'][1]),409))
check('重叠分片拒收 409','重叠' in contribute(rid,'北郊站','part-2',1,[n1-5,n1-5+n2],stats_of(shards['北郊站'][1]),409))
c=contribute(rid,'北郊站','part-2',1,[n1,n1+n2],stats_of(shards['北郊站'][1]))
check('相邻分片接收',c['samples']==n2)
c=contribute(rid,'北郊站','part-1',2,[0,n1],stats_of(shards['北郊站'][0]))
check('更正替换旧贡献',c['version']==2 and c['state']=='accepted')
d=client.get(f'/federation/rounds/{rid}',headers=VIEWER).json()
check('旧版本留痕 superseded',d['superseded']==1)
check('旧版本再报被拒 409','过旧' in contribute(rid,'北郊站','part-1',1,[0,n1],stats_of(shards['北郊站'][0]),409))
print('== C. 联合训练与判读一致性（领域量纲） ==')
for s in ('临港站','山南站'):
    contribute(rid,s,'part-1',1,[0,len(shards[s][0])],stats_of(shards[s][0]))
    contribute(rid,s,'part-2',1,[len(shards[s][0]),len(station_rows[s])],stats_of(shards[s][1]))
mid=seal_merge(rid)
with Session() as db:m=db.get(FedMerge,mid)
check('合并完成',m.state=='computed',m.error)
model=publish(rid)
check('发布成功',model['payload']['round_id']==rid)
all_rows=[r for rows in station_rows.values() for r in rows]
ref_max,ref_pred=sklearn_posterior_max(all_rows,batch_rows)
labels=model['payload']['labels']
got_max=np.array([row['posterior'] for row in model['payload']['interpretations'][str(batch_id)]['rows']])
got_pred=np.array([row['predicted'] for row in model['payload']['interpretations'][str(batch_id)]['rows']])
check('后验与合并原始样本训练一致 <1e-9',np.abs(got_max-ref_max).max()<1e-9,f'max diff {np.abs(got_max-ref_max).max():.3e}')
check('预测类别一致',(got_pred==ref_pred).all())
check('类先验一致',all(abs(model['payload']['classes'][l]['prior']-sum(1 for r in all_rows if r['label']==l)/len(all_rows))<1e-12 for l in LABELS))
check('总样本不重不漏',model['payload']['total_samples']==len(all_rows))
check('采用贡献可追查',len(model['payload']['manifest'])==6 and all('version' in m for m in model['payload']['manifest']))
d=client.get(f'/federation/rounds/{rid}',headers=VIEWER).json()
check('齐备后无缺站',d['missing_stations']==[] and d['state']=='published')
print('== D. 缺站门槛与已发布模型沿用 ==')
rid2=create_round('验证轮二',list(station_rows),[batch_id])
contribute(rid2,'北郊站','full',1,[0,len(station_rows['北郊站'])],stats_of(station_rows['北郊站']))
contribute(rid2,'临港站','full',1,[0,len(station_rows['临港站'])],stats_of(station_rows['临港站']))
seal_merge(rid2)
msg=publish(rid2,409)
check('缺站拒绝发布并点名','山南站' in msg,msg)
cur=client.get('/federation/model',headers=VIEWER).json()
check('未齐时沿用已发布模型',cur['round_id']==rid)
r=client.post('/federation/interpret',headers=VIEWER,json={'source_id':batch_id})
check('已发布模型临时判读可用',r.status_code==200 and r.json()['round_id']==rid and len(r.json()['rows'])==len(batch_rows))
print('== E. 晚到旧计算拒绝发布 ==')
contribute(rid2,'山南站','full',1,[0,len(station_rows['山南站'])],stats_of(station_rows['山南站']))
msg=publish(rid2,409)
check('旧合并拒绝发布','旧贡献' in msg,msg)
seal_merge(rid2)
model2=publish(rid2)
check('重封后发布成功',model2['payload']['round_id']==rid2)
cur=client.get('/federation/model',headers=VIEWER).json()
check('当前模型切换到新轮',cur['round_id']==rid2)
print('== F. 合并中断恢复 ==')
rid3=create_round('验证轮三',list(station_rows),[batch_id])
for s in station_rows:contribute(rid3,s,'full',1,[0,len(station_rows[s])],stats_of(station_rows[s]))
r=client.post(f'/federation/rounds/{rid3}/seal',headers=ANALYST)
mid3=r.json()['id']
with Session() as db:
    m=db.get(FedMerge,mid3);m.state='running';db.commit()
federation.queue.items.clear()
worker.recover_merges()
check('中断合并被重新入队',('federation:queue',mid3) in federation.queue.items)
worker.execute_merge(mid3)
with Session() as db:m=db.get(FedMerge,mid3)
check('恢复后合并完成',m.state=='computed',m.error)
model3=publish(rid3)
check('恢复后发布且权重不重复',model3['payload']['total_samples']==len(all_rows))
print('== G. 大偏移小方差数值稳定 ==')
BIG=1e8
smeans=[[BIG+1.0,BIG+2.0,3.0],[BIG+1.5,BIG+2.5,3.5],[BIG+1.2,BIG+2.2,3.2]]
sstds=[[1e-2,1e-2,.4],[1e-2,1e-2,.5],[1e-2,1e-2,.45]]
big_rows={'甲站':gen_rows(np.random.default_rng(21),60,smeans,sstds),'乙站':gen_rows(np.random.default_rng(22),50,[[m[0]+3e-3,m[1]+2e-3,m[2]] for m in smeans],sstds)}
big_batch=gen_rows(np.random.default_rng(23),30,smeans,sstds)
big_source=make_source('大偏移批次',big_batch)
rid4=create_round('验证轮四',list(big_rows),[big_source])
for s in big_rows:contribute(rid4,s,'full',1,[0,len(big_rows[s])],stats_of(big_rows[s]))
seal_merge(rid4)
model4=publish(rid4)
all_big=[r for rows in big_rows.values() for r in rows]
ref_big,_=sklearn_posterior_max(all_big,big_batch)
got_big=np.array([row['posterior'] for row in model4['payload']['interpretations'][str(big_source)]['rows']])
check('大偏移后验与一致训练相符 <1e-9',np.abs(got_big-ref_big).max()<1e-9,f'max diff {np.abs(got_big-ref_big).max():.3e}')
wref=welford_ref(all_big)
worst=0.0
for l in LABELS:
    for f in FEATURES:
        n,mean,m2=wref[l][f];got=model4['payload']['merged'][l][f]
        worst=max(worst,float(abs(got['mean']-mean)/max(abs(mean),1)),float(abs(got['m2']-m2)/max(m2,np.longdouble('1e-30'))))
check('合并均值/离均差平方和达float64精度极限 <1e-5',worst<1e-5,f'worst {worst:.3e}')
naive=[]
for l in LABELS:
    sel=[r for r in all_big if r['label']==l];sx=sum(r['rms'] for r in sel);sxx=sum(r['rms']**2 for r in sel)
    naive.append(sxx/len(sel)-(sx/len(sel))**2)
check('朴素平方和公式在同数据上失真（对照）',any(v<=0 or abs(v-1e-4)/1e-4>0.5 for v in naive),str(naive))
check('后验概率均在[0,1]',all(0<=row['posterior']<=1 for row in model4['payload']['interpretations'][str(big_source)]['rows']))
print('== H. 原研究流程回归 ==')
r=client.post('/runs',headers=ANALYST,json={'source_id':batch_id,'name':'回归研究','parameters':{'folds':3,'smoothing':1e-9}})
assert r.status_code==200,r.text
run_id=r.json()['id']
assert ('analysis:queue',run_id) in runs.queue.items
worker.execute(run_id)
r=client.get(f'/runs/{run_id}',headers=VIEWER)
check('原研究任务照常完成',r.json()['state']=='completed' and 'confusion_matrix' in r.json()['result'])
r=client.post('/notes',headers=ANALYST,json={'run_id':run_id,'title':'回归说明','body':'流程未受影响'})
note_id=r.json()['id']
r=client.post(f'/notes/{note_id}/approve',headers=ADMIN)
check('批注批准流程照常',r.status_code==200 and r.json()['state']=='approved')
r=client.get('/audit',headers=VIEWER)
acts={e['action'] for e in r.json()}
check('联邦事件已留痕',{'federation.round.created','federation.contribution.accepted','federation.round.sealed','federation.round.published'}<=acts)
print()
print(f'全部通过：{len(passed)} 项检查')
shutil.rmtree(TMP,ignore_errors=True)
