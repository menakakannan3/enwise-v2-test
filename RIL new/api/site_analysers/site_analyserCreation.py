from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from ...modals.masters import SiteAnalyser, Site, Analyser
from ...database.session import getdb
from starlette import status
from ...utils.utils import *

from ..auth.authentication import user_dependency

router = APIRouter()

@router.post("/api/site-analyser/create", summary="Create a new site analyser", tags=["site_analyser"])
def create_site_analyser(
    user: user_dependency,
    site_id: int,
    analyser_ids: list[int],  
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    created_entries = []
    inserted_count = 0  # Track inserted records

    for analyser_id in analyser_ids:
        analyser = db.query(Analyser).filter(Analyser.id == analyser_id).first()
        if not analyser:
            continue

        # Check if the entry already exists
        existing_entry = db.query(SiteAnalyser).filter(
            SiteAnalyser.site_id == site_id,
            SiteAnalyser.analyser_id == analyser_id
        ).first()

        if existing_entry:
            continue  # Skip duplicate entries

        # Insert new record
        new_site_analyser = SiteAnalyser(
            site_id=site_id,
            analyser_id=analyser_id,
            created_by=1,
        )
        db.add(new_site_analyser)
        db.flush()
        inserted_count += 1  # Increment inserted count

        created_entries.append({
            "analyser_id": analyser_id,
            "analyser_name": analyser.analyser_name,
            "analyser_uid": analyser.analyser_uid,
            "site_id": site_id,
            "site_name": site.site_name
        })

    db.commit()

    return response_strct(
        status_code=status.HTTP_201_CREATED,
        detail=f"Site analyser added successfully! (Inserted: {inserted_count})",
        data={"inserted_count": inserted_count, "entries": created_entries}
    )


@router.get("/api/site-analysers", summary="Get all site analysers", tags=["site_analyser"])
def get_all_site_analysers( user : user_dependency ,db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    site_analysers = db.query(SiteAnalyser).all()
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Site analysers fetched successfully!",
        data=site_analysers
    )

@router.get("/api/site-analyser/{site_id}", summary="Get analyser for a site", tags=['site_analyser'])
def get_analyser_by_site(user: user_dependency, site_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    
    # Join SiteAnalyser with Analyser to get the analyser details
    site_analyser = db.query(SiteAnalyser, Analyser.analyser_name)\
                      .join(Analyser, SiteAnalyser.analyser_id == Analyser.id)\
                      .filter(SiteAnalyser.site_id == site_id)\
                      .all()
    
    if not site_analyser:
        raise HTTPException(status_code=404, detail="No analyser found for this site")
    
    # Format the response to include analyser details
    analyser_data = [{
        "site_analyser_id": sa.SiteAnalyser.id,
        "site_id": sa.SiteAnalyser.site_id,
        "analyser_id": sa.SiteAnalyser.analyser_id,
        "analyser_name": sa.analyser_name,
        "created_at": sa.SiteAnalyser.created_at,
        "created_by": sa.SiteAnalyser.created_by,
        "updated_at": sa.SiteAnalyser.updated_at,
        "updated_by": sa.SiteAnalyser.updated_by
    } for sa in site_analyser]
    
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Analyser for the given site fetched successfully",
        data=analyser_data
    )

@router.put("/api/site-analyser/{site_id}", summary="Update Site Analyser", tags=['site_analyser'])
def update_site_analyser(user : user_dependency , site_id: int, analyser_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    site_analyser = db.query(SiteAnalyser).filter(SiteAnalyser.site_id == site_id).first()
    if not site_analyser:
        raise HTTPException(status_code=404, detail="Site Analyser not found")
    
    analyser = db.query(Analyser).filter(Analyser.id == analyser_id).first()
    if not analyser:
        raise HTTPException(status_code=404, detail="Analyser not found")
    
    site_analyser.analyser_id = analyser_id
    site_analyser.updated_by = 1  # Example: Modify as needed
    db.commit()
    db.refresh(site_analyser)
    
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Site Analyser updated successfully",
        data=site_analyser
    )

@router.delete("/api/site-analyser/{site_id}/{analyser_id}", summary="Delete Site Analyser", tags=['site_analyser'])
def delete_site_analyser(user : user_dependency , site_id: int, analyser_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    site_analyser = db.query(SiteAnalyser).filter(
        SiteAnalyser.site_id == site_id,
        SiteAnalyser.analyser_id == analyser_id
    ).first()
    if not site_analyser:
        raise HTTPException(status_code=404, detail="Site Analyser not found")
    
    db.delete(site_analyser)
    db.commit()
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="The site analyser deleted successfully",
        data="Delete successful"
    )