#OM VIGHNHARTAYE NAMO NAMAH :



from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime
from ...modals.masters import Site, Group  # Ensure correct imports
from ...database.session import getdb
from ..auth.authentication import user_dependency
from ...utils.permissions import enforce_site_access

router = APIRouter()

@router.get("/api/sites/status/{site_id}", tags=['site - map'], summary="Get site map")
def get_site_status(user: user_dependency,site_id: int, db: Session = Depends(getdb)):
    enforce_site_access(user, site_id)
    # Fetch Site details
    site = (
        db.query(
            Site.id, Site.site_name, Site.latitude, Site.longitude, 
            Group.group_name, Site.auth_expiry
        )
        .outerjoin(Group, Group.id == Site.group_id)  # Corrected join condition
        .filter(Site.id == site_id)
        .first()
    )
    
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # Determine if the site is active or inactive based on auth_expiry
    current_date = datetime.utcnow()  # Corrected datetime usage
    site_status = "Active" if site.auth_expiry and site.auth_expiry >= current_date else "Inactive"

    # Construct response
    response = {
        "siteId": site.id,
        "siteName": site.site_name,
        "latitude": str(site.latitude),
        "longitude": str(site.longitude),
        "groupName": site.group_name,
        "authExpiry": str(site.auth_expiry),
        "status": site_status
    }

    return response
