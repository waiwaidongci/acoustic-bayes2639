from pydantic import BaseModel,Field
class RoundInput(BaseModel):
    name:str=Field(min_length=2,max_length=160)
    stations:list[str]=Field(min_length=1,max_length=50)
    features:list[str]|None=None
    smoothing:float|None=None
    interpret_source_ids:list[int]=Field(min_length=1,max_length=20)
class ContributionInput(BaseModel):
    station:str=Field(min_length=1,max_length=120)
    shard:str=Field(min_length=1,max_length=120)
    version:int=Field(ge=1,le=1000000)
    coverage:list[int]=Field(min_length=2,max_length=2)
    stats:dict
class PreviewInput(BaseModel):
    source_id:int
class InterpretInput(BaseModel):
    source_id:int
