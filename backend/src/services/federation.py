import json,math,hashlib,logging
import numpy as np
from sqlalchemy import select
from fastapi import HTTPException
from redis import Redis
from ..config.settings import REDIS_URL,CONFIG
from ..models.federation import FedRound,FedContribution,FedMerge,FedModel
from ..models.record import Record
from ..utils.serialization import row,now
from .sources import source_or_404
from .audit import log
queue=Redis.from_url(REDIS_URL)
FEATURE_FIELDS=[f for f,_,kind in CONFIG['fields'] if kind=='number']
LABEL_FIELD=next(f for f,_,kind in CONFIG['fields'] if kind!='number')
def combine(cells):
    n,mean,m2=0,0.0,0.0
    for cn,cmean,cm2 in cells:
        if cn<1:continue
        if n==0:n,mean,m2=cn,float(cmean),float(cm2);continue
        total=n+cn;delta=float(cmean)-mean
        mean+=delta*cn/total;m2=m2+float(cm2)+delta*delta*n*cn/total;n=total
    return n,mean,m2
def local_stats(records,features):
    cells={}
    for rec in records:
        label=str(rec['values'][LABEL_FIELD]);per=cells.setdefault(label,{f:[0,0.0,0.0] for f in features})
        for f in features:
            x=float(rec['values'][f]);slot=per[f];slot[0]+=1;delta=x-slot[1];slot[1]+=delta/slot[0];slot[2]+=delta*(x-slot[1])
    return {label:{f:{'n':v[0],'mean':v[1],'m2':v[2]} for f,v in per.items()} for label,per in cells.items()}
def validate_stats(stats,features):
    if not isinstance(stats,dict) or not stats:raise ValueError('汇总统计不能为空')
    if len(stats)>200:raise ValueError('单个分片故障类型过多')
    out={}
    for label,fstats in stats.items():
        if not isinstance(label,str) or not label.strip() or len(label)>600:raise ValueError('故障类型名称不能为空且最多600字符')
        label=label.strip()
        if not isinstance(fstats,dict) or set(fstats)!=set(features):raise ValueError('分片特征口径与本轮冻结口径不一致：'+label)
        cells={}
        for f in features:
            s=fstats.get(f) or {}
            try:n,mean,m2=s['n'],float(s['mean']),float(s['m2'])
            except (KeyError,TypeError,ValueError):raise ValueError('统计需包含 n/mean/m2 数值：'+label)
            if isinstance(n,bool) or int(n)!=n or not 1<=int(n)<=10**9:raise ValueError('样本数需为1至1e9的整数：'+label)
            if not math.isfinite(mean):raise ValueError('均值必须是有限数值：'+label)
            if not math.isfinite(m2) or m2<0:raise ValueError('离均差平方和需为有限非负数值：'+label)
            cells[f]={'n':int(n),'mean':mean,'m2':m2}
        out[label]=cells
    return out
def build_model(contributions,features,smoothing):
    gathered={}
    for entry in contributions:
        for label,fstats in entry['stats'].items():
            for f in features:
                s=fstats[f];gathered.setdefault(label,{}).setdefault(f,[]).append((s['n'],s['mean'],s['m2']))
    labels=sorted(gathered)
    if len(labels)<2:raise ValueError('合并后不足两种故障类型，无法建立判读模型')
    merged={l:{f:combine(gathered[l][f]) for f in features} for l in labels}
    glob={f:combine([merged[l][f] for l in labels]) for f in features}
    total=glob[features[0]][0]
    if all(glob[f][2]==0 for f in features):raise ValueError('全部声学特征恒定，无法学习故障差异，请补充独立观测')
    scale={f:(math.sqrt(glob[f][2]/total) if glob[f][2]>0 else 1.0) for f in features}
    epsilon=smoothing*max((glob[f][2]/total)/(scale[f]**2) for f in features)
    classes={}
    for l in labels:
        count=merged[l][features[0]][0]
        classes[l]={'count':count,'prior':count/total,'theta':{f:(merged[l][f][1]-glob[f][1])/scale[f] for f in features},'var':{f:merged[l][f][2]/count/(scale[f]**2)+epsilon for f in features}}
    return {'features':list(features),'labels':labels,'smoothing':smoothing,'epsilon':epsilon,'scaler':{'mean':{f:glob[f][1] for f in features},'scale':scale},'classes':classes,'merged':{l:{f:{'n':merged[l][f][0],'mean':merged[l][f][1],'m2':merged[l][f][2]} for f in features} for l in labels},'total_samples':total}
def interpret(model,records):
    features=model['features'];labels=model['labels']
    mean=np.array([model['scaler']['mean'][f] for f in features]);scale=np.array([model['scaler']['scale'][f] for f in features])
    theta=np.array([[model['classes'][l]['theta'][f] for f in features] for l in labels])
    var=np.array([[model['classes'][l]['var'][f] for f in features] for l in labels])
    prior=np.array([model['classes'][l]['prior'] for l in labels])
    X=np.array([[float(r['values'][f]) for f in features] for r in records],float).reshape(len(records),len(features))
    Xs=(X-mean)/scale
    jll=np.log(prior)[None,:]-0.5*np.sum(np.log(2.0*np.pi*var),axis=1)[None,:]-0.5*((Xs[:,None,:]-theta[None,:,:])**2/var[None,:,:]).sum(axis=2)
    top=jll.max(axis=1,keepdims=True);logpx=top[:,0]+np.log(np.exp(jll-top).sum(axis=1));probs=np.exp(jll-logpx[:,None])
    pick=probs.argmax(axis=1)
    rows=[{'reference':records[i]['reference'],'actual':str(records[i]['values'].get(LABEL_FIELD,'')),'predicted':labels[int(pick[i])],'posterior':float(probs[i,pick[i]])} for i in range(len(records))]
    hits=sum(1 for r in rows if r['actual']==r['predicted'])
    return {'summary':{'samples':len(rows),'accuracy':(hits/len(rows)) if rows else 0.0},'rows':rows}
def manifest_samples(c,first):return sum(fstats[first]['n'] for fstats in c['stats'].values())
def get_round(db,id):
    r=db.get(FedRound,id)
    if not r:raise HTTPException(404,'未找到联邦轮次')
    return r
def lock_round(db,id):
    r=db.scalar(select(FedRound).where(FedRound.id==id).with_for_update())
    if not r:raise HTTPException(404,'未找到联邦轮次')
    return r
def create_round(db,user,data):
    stations=[]
    for s in data.stations:
        s=s.strip()
        if not s or len(s)>120:raise HTTPException(422,'站点名称不能为空且最多120字符')
        if s in stations:raise HTTPException(422,'站点名单重复：'+s)
        stations.append(s)
    features=data.features or list(FEATURE_FIELDS)
    if not features or len(set(features))!=len(features) or set(features)-set(FEATURE_FIELDS):raise HTTPException(422,'特征口径需为本域数值特征的子集且不重复')
    smoothing=CONFIG['default_parameters']['smoothing'] if data.smoothing is None else float(data.smoothing)
    if not math.isfinite(smoothing) or not 1e-12<=smoothing<=.01:raise HTTPException(422,'smoothing需在1e-12至0.01之间')
    if len(set(data.interpret_source_ids))!=len(data.interpret_source_ids):raise HTTPException(422,'判读批次重复')
    for sid in data.interpret_source_ids:source_or_404(db,sid)
    r=FedRound(name=data.name,features=features,stations=stations,smoothing=smoothing,interpret_source_ids=data.interpret_source_ids,state='collecting',epoch=0,created_at=now())
    db.add(r);db.flush();log(db,user,'federation.round.created',r.id,r.name);db.commit();return row(r)
def shard_brief(c):return {'shard':c.shard,'version':c.version,'coverage':c.coverage,'samples':c.samples,'cells':c.cells,'received_at':c.received_at}
def round_brief(db,r):
    accepted=db.scalars(select(FedContribution).where(FedContribution.round_id==r.id,FedContribution.state=='accepted')).all()
    reported={c.station for c in accepted}
    d={k:v for k,v in row(r).items()}
    d['accepted_shards']=len(accepted);d['missing_stations']=[s for s in r.stations if s not in reported]
    return d
def list_rounds(db):return [round_brief(db,r) for r in db.scalars(select(FedRound).order_by(FedRound.id.desc()))]
def round_detail(db,id):
    r=get_round(db,id);d=round_brief(db,r)
    contributions=db.scalars(select(FedContribution).where(FedContribution.round_id==id).order_by(FedContribution.id)).all()
    accepted=[c for c in contributions if c.state=='accepted']
    by_station={s:[] for s in r.stations}
    for c in accepted:by_station.setdefault(c.station,[]).append(c)
    d['station_status']=[{'station':s,'reported':bool(by_station.get(s)),'samples':sum(c.samples for c in by_station.get(s,[])),'shards':[shard_brief(c) for c in by_station.get(s,[])]} for s in r.stations]
    d['superseded']=sum(1 for c in contributions if c.state=='superseded')
    merges=db.scalars(select(FedMerge).where(FedMerge.round_id==id).order_by(FedMerge.id.desc())).all()
    d['merges']=[{k:v for k,v in row(m).items() if k not in ('manifest','result')} for m in merges]
    latest=next((m for m in merges if m.state=='computed'),None)
    d['candidate']=None
    if latest:
        d['candidate']={'id':latest.id,'epoch':latest.epoch,'current':latest.epoch==r.epoch,'manifest':[{'station':c['station'],'shard':c['shard'],'version':c['version'],'samples':manifest_samples(c,r.features[0])} for c in latest.manifest['contributions']],'interpretations':{sid:{'name':b['name'],'summary':b['summary']} for sid,b in latest.result['interpretations'].items()}}
    model=db.scalar(select(FedModel).where(FedModel.round_id==id))
    d['model']=row(model) if model else None
    return d
def submit_contribution(db,user,id,data):
    r=lock_round(db,id)
    if r.state!='collecting':raise HTTPException(409,'本轮已发布，贡献通道已关闭')
    if data.station not in r.stations:raise HTTPException(422,'站点不在本轮冻结名单：'+data.station)
    try:stats=validate_stats(data.stats,r.features)
    except ValueError as e:raise HTTPException(422,str(e))
    start,end=data.coverage
    if not 0<=start<end or end>10**9:raise HTTPException(422,'分片覆盖区间需满足 0≤start<end')
    first=r.features[0]
    samples=sum(fstats[first]['n'] for fstats in stats.values())
    if samples!=end-start:raise HTTPException(422,'分片样本数与覆盖区间长度不一致')
    existing=db.scalar(select(FedContribution).where(FedContribution.round_id==id,FedContribution.station==data.station,FedContribution.shard==data.shard,FedContribution.state=='accepted'))
    if existing and data.version<existing.version:raise HTTPException(409,'分片版本过旧：已接收版本 '+str(existing.version))
    if existing and data.version==existing.version:
        if existing.stats==stats and list(existing.coverage)==[start,end]:return {**row(existing),'deduplicated':True}
        raise HTTPException(409,'同一分片版本号不可改写，请提升版本号')
    others=db.scalars(select(FedContribution).where(FedContribution.round_id==id,FedContribution.station==data.station,FedContribution.state=='accepted',FedContribution.shard!=data.shard)).all()
    for o in others:
        if start<o.coverage[1] and o.coverage[0]<end:raise HTTPException(409,'分片覆盖区间与已接收分片重叠：'+o.shard)
    cells=len(stats)
    c=FedContribution(round_id=id,station=data.station,shard=data.shard,version=data.version,coverage=[start,end],stats=stats,cells=cells,samples=samples,state='accepted',received_at=now())
    action='federation.contribution.accepted'
    if existing:existing.state='superseded';action='federation.contribution.replaced'
    db.add(c);r.epoch+=1
    try:db.flush()
    except Exception:db.rollback();raise HTTPException(409,'相同分片版本已接收，请勿重复提交')
    log(db,user,action,c.id,data.station+'/'+data.shard+'@'+str(data.version));db.commit();return row(c)
def seal_round(db,user,id,enqueue=True):
    r=lock_round(db,id)
    if r.state!='collecting':raise HTTPException(409,'本轮已发布，不能再封轮')
    accepted=db.scalars(select(FedContribution).where(FedContribution.round_id==id,FedContribution.state=='accepted').order_by(FedContribution.station,FedContribution.shard)).all()
    if not accepted:raise HTTPException(422,'尚无已接收贡献，无法封轮合并')
    contributions=[{'station':c.station,'shard':c.shard,'version':c.version,'coverage':c.coverage,'stats':c.stats} for c in accepted]
    batches={}
    for sid in r.interpret_source_ids:
        s=source_or_404(db,sid)
        batches[str(sid)]={'source_id':sid,'name':s.name,'records':[{'reference':x.reference,'values':x.values} for x in db.scalars(select(Record).where(Record.source_id==sid).order_by(Record.id))]}
    m=FedMerge(round_id=id,epoch=r.epoch,manifest={'contributions':contributions,'batches':batches},state='queued',created_at=now())
    db.add(m);db.flush();log(db,user,'federation.round.sealed',id,'epoch='+str(r.epoch)+' 分片='+str(len(contributions)));db.commit()
    if enqueue:
        try:queue.rpush('federation:queue',m.id)
        except Exception:
            m.state='failed';m.error='队列暂不可用，请重新封轮';db.commit();raise HTTPException(503,m.error)
    return row(m)
def run_merge(db,merge_id):
    m=db.get(FedMerge,merge_id)
    if not m or m.state!='queued':return
    r=db.get(FedRound,m.round_id)
    m.state='running';db.commit()
    try:
        model=build_model(m.manifest['contributions'],r.features,r.smoothing)
        interpretations={sid:{'source_id':b['source_id'],'name':b['name'],**interpret(model,b['records'])} for sid,b in m.manifest['batches'].items()}
        result={'model':model,'interpretations':interpretations}
        json.dumps(result,allow_nan=False)
        m.result=result;m.state='computed';log(db,'worker','federation.merge.computed',m.id,'round='+str(m.round_id))
    except Exception as e:
        logging.exception('federation merge failed');m.state='failed';m.error=str(e)[:2000];log(db,'worker','federation.merge.failed',m.id,m.error)
    db.commit()
def publish_round(db,user,id):
    r=lock_round(db,id)
    if r.state!='collecting':raise HTTPException(409,'本轮已发布')
    m=db.scalar(select(FedMerge).where(FedMerge.round_id==id,FedMerge.state=='computed').order_by(FedMerge.id.desc()))
    if not m:raise HTTPException(409,'尚无已完成的合并，请先封轮')
    if m.epoch!=r.epoch:raise HTTPException(409,'合并基于旧贡献（epoch '+str(m.epoch)+'≠'+str(r.epoch)+'），晚到旧计算拒绝发布，请重新封轮合并')
    reported={c['station'] for c in m.manifest['contributions']}
    missing=[s for s in r.stations if s not in reported]
    if missing:raise HTTPException(409,'缺站：'+'、'.join(missing)+'；全部站点贡献齐备才能发布')
    if any(str(sid) not in m.result['interpretations'] for sid in r.interpret_source_ids):raise HTTPException(409,'判读结果未齐备，拒绝发布')
    manifest=[{'station':c['station'],'shard':c['shard'],'version':c['version'],'coverage':c['coverage'],'samples':manifest_samples(c,r.features[0])} for c in m.manifest['contributions']]
    checksum=hashlib.sha256(json.dumps(m.manifest,sort_keys=True).encode()).hexdigest()
    payload={**m.result['model'],'round_id':id,'round_name':r.name,'epoch':m.epoch,'manifest':manifest,'checksum':checksum,'interpretations':m.result['interpretations']}
    model=FedModel(round_id=id,merge_id=m.id,epoch=m.epoch,payload=payload,created_at=now())
    r.state='published';r.published_at=now()
    db.add(model);db.flush();log(db,user,'federation.round.published',id,r.name);db.commit();return row(model)
def current_model(db):
    m=db.scalar(select(FedModel).order_by(FedModel.id.desc()))
    if not m:raise HTTPException(404,'尚无已发布联邦模型')
    return m
def interpret_with_published(db,source_id):
    s=source_or_404(db,source_id);m=current_model(db)
    records=[{'reference':x.reference,'values':x.values} for x in db.scalars(select(Record).where(Record.source_id==source_id).order_by(Record.id))]
    out=interpret(m.payload,records)
    return {'model_id':m.id,'round_id':m.round_id,'round_name':m.payload['round_name'],'epoch':m.epoch,'source_id':source_id,'source_name':s.name,**out}
def preview_stats(db,source_id):
    s=source_or_404(db,source_id)
    records=[{'reference':x.reference,'values':x.values} for x in db.scalars(select(Record).where(Record.source_id==source_id).order_by(Record.id))]
    if not records:raise HTTPException(422,'该批次还没有样本')
    return {'source_id':source_id,'source_name':s.name,'features':list(FEATURE_FIELDS),'samples':len(records),'stats':local_stats(records,list(FEATURE_FIELDS))}
