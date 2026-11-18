from fastapi import APIRouter,Depends
from ..config.database import db_session
from ..schemas.federation import RoundInput,ContributionInput,PreviewInput,InterpretInput
from ..services.auth import current_user,writer,admin
from ..controllers.federation import FederationController as C
router=APIRouter()
@router.get('/federation/rounds')
def rounds(db=Depends(db_session),u=Depends(current_user)):return C.list_rounds(db)
@router.post('/federation/rounds')
def create(data:RoundInput,db=Depends(db_session),u=Depends(writer)):return C.create_round(db,u,data)
@router.get('/federation/rounds/{id}')
def detail(id:int,db=Depends(db_session),u=Depends(current_user)):return C.round_detail(db,id)
@router.post('/federation/rounds/{id}/contributions')
def contribute(id:int,data:ContributionInput,db=Depends(db_session),u=Depends(writer)):return C.submit(db,u,id,data)
@router.post('/federation/rounds/{id}/seal')
def seal(id:int,db=Depends(db_session),u=Depends(writer)):return C.seal(db,u,id)
@router.post('/federation/rounds/{id}/publish')
def publish(id:int,db=Depends(db_session),u=Depends(admin)):return C.publish(db,u,id)
@router.get('/federation/model')
def model(db=Depends(db_session),u=Depends(current_user)):return C.model(db)
@router.post('/federation/preview-stats')
def preview(data:PreviewInput,db=Depends(db_session),u=Depends(current_user)):return C.preview(db,data.source_id)
@router.post('/federation/interpret')
def interpret(data:InterpretInput,db=Depends(db_session),u=Depends(current_user)):return C.interpret(db,data.source_id)
