#OM VIGHNHARTAYE NAMO NAMAH :

import os
import shutil
import datetime
from pydantic import ValidationError
from ...schemas.masterSchema import SiteCreation , SiteUpdate
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Depends
from sqlalchemy.orm import Session
from ...modals.masters import Site, SiteDocument  # Import your ORM models
from ...database.session import getdb
from urllib.parse import unquote
from ...utils.utils import *
from starlette import status
from ..auth.authentication import user_dependency

router = APIRouter()


UPLOAD_FOLDER = "uploads/site_documents"

@router.post("/api/site/create", summary="Create a new site" , tags=['site'] )
async def create_site(
    user : user_dependency,
    site_name: str = Form(...),
    address: str = Form(... ),
    city: str = Form(...),
    state: str = Form(...),
    latitude: float = Form(...),
    longitude: float = Form(...),
    documents: list[UploadFile] = File(None),
    group_uuid: str = Form(None),
    authkey : str = Form(...),
    gangaBasin: Optional[str] = Form(None),
    db: Session = Depends(getdb),
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    last_site = db.query(Site).order_by(Site.id.desc()).first()

    try:
        validated_data = SiteCreation(
            site_name=site_name,
            address=address,
            city=city,
            state=state,
            latitude=latitude,
            longitude=longitude,
            group_uuid=group_uuid,
            auth_key=authkey
        )
    except ValidationError as e:
        return {"detail": e.errors()}
    if gangaBasin is None:
        gangaBasin = "false"
    new_id = (last_site.id + 1) if last_site else 1
    site_uid = f"EW_2526{new_id}"
    created_at = datetime.datetime.utcnow()
    auth_expiry = created_at + datetime.timedelta(days=365)
    group_id = None
    if group_uuid:
        group = db.query(Group).filter(Group.uuid == group_uuid).first()
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")
        group_id = group.id
    site = Site(
        siteuid=site_uid,
        site_name=site_name,
        address=address,
        city=city,
        state=state,
        latitude=latitude,
        longitude=longitude,
        created_at=datetime.datetime.utcnow(),
        created_by=1,
        ganga_basin = gangaBasin,
        group_id = group_id,
        authkey = authkey ,
        auth_expiry = auth_expiry
    )
    db.add(site)
    db.flush()
    db.refresh(site)
    site_data = {key: value for key, value in site.__dict__.items() if not key.startswith("_")}
    
    # Create document directory
    site_folder = os.path.join(UPLOAD_FOLDER, site_uid)
    Path(site_folder).mkdir(parents=True, exist_ok=True)
    
    uploaded_files = []
    if documents:
        for document in documents:
            file_path = os.path.join(site_folder, document.filename)
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(document.file, buffer)
            
            filename_lower = document.filename.lower()
            documet_type_id = 3
            if "_cto" in filename_lower:
                documet_type_id = 1
            elif "_pdc" in filename_lower:
                documet_type_id = 2
            
            site_doc = SiteDocument(
                site_id=site.id,
                document_name=document.filename,
                document_format=document.content_type,
                document_path=file_path,
                document_type=documet_type_id,  # Assuming document type as 1 (modify as needed)
                created_by=1,  # Hardcoded
            )
            db.add(site_doc)
            uploaded_files.append(file_path)
    
    db.commit()
    
    return response_strct(
        status_code=status.HTTP_201_CREATED,
        detail="site created successfully",
        data={"site" : site_data , "documents" : uploaded_files},
    )


@router.get("/api/sites/getall", tags=['site'])
def get_all_site(user: user_dependency, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    # Fetch sites along with group_uuid
    sites = (
        db.query(Site, Group.uuid)
        .join(Group, Site.group_id == Group.id)  # Joining Site with Group
        .all()
    )

    # Format response to include group_uuid
    site_list = [{**site.__dict__, "group_uuid": group_uuid} for site, group_uuid in sites]

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="All sites fetched successfully",
        data=site_list,
        error=""
    )
    

@router.get("/api/site/{site_uid}", tags=['site'] , summary="Get site by UID")
def get_site_by_uid(user : user_dependency, site_uid: str, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    site = db.query(Site).filter(Site.siteuid == site_uid).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")
    return site


@router.get("/api/site/siteid/{site_id}", tags=['site'] , summary="Get site by ID")
def get_site_by_uid(user: user_dependency, site_id: int, db: Session = Depends(getdb)):
   
    site = db.query(Site, Group.group_name).join(Group, Site.group_id == Group.id, isouter=True).filter(Site.id == site_id).first()

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    site_data = {
        "id": site.Site.id,
        "siteuid": site.Site.siteuid,
        "site_name": site.Site.site_name,
        "address": site.Site.address,
        "city": site.Site.city,
        "state": site.Site.state,
        "created_at": site.Site.created_at,
        "created_by": site.Site.created_by,
        "authkey": site.Site.authkey,
        "auth_expiry": site.Site.auth_expiry,
        "keyGeneratedDate": site.Site.keyGeneratedDate,
        "latitude": site.Site.latitude,
        "longitude": site.Site.longitude,
        "group_id": site.Site.group_id,
        "group_name": site.group_name  # Adding group name
    }

    return site_data


@router.put("/api/site/{site_uid}", summary="Update site by UID", tags=['site'])
async def update_site(
    user : user_dependency,
    site_uid: str,
    site_name: str = Form(None),
    address: str = Form(None),
    city: str = Form(None),
    state: str = Form(None),
    latitude: float = Form(None),
    longitude: float = Form(None),
    documents: list[UploadFile] = File(None),
    group_uuid: str = Form(None),
    gangaBasin : Optional[str] = Form(None),
    db: Session = Depends(getdb),
):
    try:
        validated_data = SiteUpdate(
            site_name=site_name,
            address=address,
            city=city,
            state=state,
            latitude=latitude,
            longitude=longitude,
            group_uuid=group_uuid
        )
    except ValidationError as e:
        return {"detail": e.errors()}
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    site = db.query(Site).filter(Site.siteuid == site_uid).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")
    if site_name is not None:
        site.site_name = site_name
    if address is not None:
        site.address = address
    if city is not None:
        site.city = city
    if state is not None:
        site.state = state
    if latitude is not None:
        site.latitude = latitude
    if longitude is not None:
        site.longitude = longitude
    if gangaBasin is not None:
        site.ganga_basin = gangaBasin
    if group_uuid is not None:
        group = db.query(Group).filter(Group.uuid == group_uuid).first()
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")
        site.group_id = group.id
        
    site_data = {key: value for key, value in site.__dict__.items() if not key.startswith("_")}
    site_folder = os.path.join(UPLOAD_FOLDER, site_uid)
    Path(site_folder).mkdir(parents=True, exist_ok=True)

    uploaded_files = []
    if documents:
        for document in documents:
            file_path = os.path.join(site_folder, document.filename)
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(document.file, buffer)
            filename_lower = document.filename.lower()
            documet_type_id = 3
            if "_cto" in filename_lower:
                documet_type_id = 1
            elif "_pdc" in filename_lower:
                documet_type_id = 2
            site_doc = SiteDocument(
                site_id=site.id,
                document_name=document.filename,
                document_format=document.content_type,
                document_path=file_path,
                document_type=documet_type_id,
                created_by=1,
            )
            db.add(site_doc)
            uploaded_files.append(file_path)

    db.commit()
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Site updated successfully",
        data={"site": site_data, "documents": uploaded_files}
    )



@router.delete("/api/site/{site_uid}/document/{document_name}", summary="Delete document by name and UID", tags=['site'])
def delete_document(user : user_dependency , site_uid: str, document_name: str, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    document_name = unquote(document_name)
    site = db.query(Site).filter(Site.siteuid == site_uid).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")
    
    document = db.query(SiteDocument).filter(
        SiteDocument.site_id == site.id, SiteDocument.document_name == document_name
    ).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    
    if os.path.exists(document.document_path):
        os.remove(document.document_path)
    
    db.delete(document)
    db.commit()
    
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Document deleted successfully"
    )

@router.get("/api/site/{site_id}/documents", summary="Get all document paths for a site", tags=["site"])
def get_site_documents(user: user_dependency, site_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    documents = db.query(SiteDocument).filter(SiteDocument.site_id == site.id).all()
    
    if not documents:
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail="No documents found for this site",
            data=[]
        )
    document_urls = [
        {
            "filename": doc.document_name,
            "url": f"/uploads/site_documents/{site.siteuid}/{doc.document_name}"
        }
        for doc in documents
    ]
    
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Documents fetched successfully",
        data=document_urls
    )
