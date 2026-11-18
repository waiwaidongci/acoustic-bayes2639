from sqlalchemy import Column,Integer,String,JSON,Text,Float,ForeignKey,UniqueConstraint
from ..config.database import Base
class FedRound(Base):
    __tablename__='fed_rounds'
    id=Column(Integer,primary_key=True)
    name=Column(String(160),nullable=False)
    features=Column(JSON,nullable=False)
    stations=Column(JSON,nullable=False)
    smoothing=Column(Float,nullable=False)
    interpret_source_ids=Column(JSON,nullable=False)
    state=Column(String(20),default='collecting')
    epoch=Column(Integer,default=0)
    created_at=Column(String(40),nullable=False)
    published_at=Column(String(40),default='')
class FedContribution(Base):
    __tablename__='fed_contributions'
    __table_args__=(UniqueConstraint('round_id','station','shard','version',name='uq_contribution_version'),)
    id=Column(Integer,primary_key=True)
    round_id=Column(Integer,ForeignKey('fed_rounds.id'),nullable=False)
    station=Column(String(120),nullable=False)
    shard=Column(String(120),nullable=False)
    version=Column(Integer,nullable=False)
    coverage=Column(JSON,nullable=False)
    stats=Column(JSON,nullable=False)
    cells=Column(Integer,nullable=False)
    samples=Column(Integer,nullable=False)
    state=Column(String(20),default='accepted')
    received_at=Column(String(40),nullable=False)
class FedMerge(Base):
    __tablename__='fed_merges'
    id=Column(Integer,primary_key=True)
    round_id=Column(Integer,ForeignKey('fed_rounds.id'),nullable=False)
    epoch=Column(Integer,nullable=False)
    manifest=Column(JSON,nullable=False)
    state=Column(String(20),default='queued')
    result=Column(JSON,nullable=True)
    error=Column(Text,default='')
    created_at=Column(String(40),nullable=False)
class FedModel(Base):
    __tablename__='fed_models'
    id=Column(Integer,primary_key=True)
    round_id=Column(Integer,ForeignKey('fed_rounds.id'),unique=True,nullable=False)
    merge_id=Column(Integer,ForeignKey('fed_merges.id'),nullable=False)
    epoch=Column(Integer,nullable=False)
    payload=Column(JSON,nullable=False)
    created_at=Column(String(40),nullable=False)
