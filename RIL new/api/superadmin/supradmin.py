import os
import shutil
import datetime
from pydantic import ValidationError
from ...schemas.masterSchema import SiteCreation , SiteUpdate
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Depends
from sqlalchemy.orm import Session
from ...modals.masters import Site, SiteDocument  
from ...database.session import getdb
from urllib.parse import unquote
from ...utils.utils import *
from starlette import status
from ..auth.authentication import get_current_user
from ..auth.authentication import user_dependency

router = APIRouter()

@router.get("/api/site/superAdmin", tags=['site_users'])
def get_id_at_login(user: dict = Depends(get_current_user), db: Session = Depends(getdb)):
    try:
        user_role = user['role']
        
        if user_role != "superAdmin":
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED , detail= "unauthorised")
        
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail="Sites associated with user fetched successfully",
            data={},
            error=""
        )
    except Exception as e:
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )

@router.get("/api/superAdmin/sites", tags=['superadmin'])
def get_sites(user: user_dependency,username: str = None, db: Session = Depends(getdb)):
    try:
        if username:
            user = db.query(User).filter(User.username == username).first()
            if not user:
                return response_strct(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found",
                    data={},
                    error=""
                )
            user_id = user.id
            sites = db.query(Site).join(SiteUser, SiteUser.site_id == Site.id).filter(SiteUser.user_id == user_id).all()
        else:
            sites = db.query(Site).all()

        result = []
        for site in sites:
            # Get group name
            group_name = db.query(Group.group_name).filter(Group.id == site.group_id).scalar()

            # Get all station_param_ids for this site via station -> stationParameter
            station_param_ids = (
                db.query(stationParameter.id)
                .join(Station, Station.id == stationParameter.station_id)
                .filter(Station.site_id == site.id)
                .all()
            )
            station_param_ids = [row.id for row in station_param_ids]

            # Get latest status for each station_param_id
            latest_statuses = (
                db.query(
                    SiteStatus.station_param_id,
                    SiteStatus.status,
                    func.max(SiteStatus.starttime).label("latest_time")
                )
                .filter(SiteStatus.station_param_id.in_(station_param_ids))
                .group_by(SiteStatus.station_param_id, SiteStatus.status)
                .all()
            )

            # Aggregate site status
            site_status = "Online"  # default
            for _, status, _ in latest_statuses:
                if status == "Offline":
                    site_status = "Offline"
                    break
                elif status == "Delay":
                    site_status = "Delay"

            # Response structure
            result.append({
                "id": site.id,
                "siteuid": site.siteuid,
                "site_name": site.site_name,
                "address": site.address,
                "city": site.city,
                "state": site.state,
                "created_at": site.created_at,
                "created_by": site.created_by,
                "authkey": site.authkey,
                "auth_expiry": site.auth_expiry,
                "keyGeneratedDate": site.keyGeneratedDate,
                "latitude": float(site.latitude) if site.latitude else None,
                "longitude": float(site.longitude) if site.longitude else None,
                "group_id": site.group_id,
                "group_name": group_name,
                "site_status": site_status,
                "is_active": site.auth_expiry is None or site.auth_expiry > datetime.datetime.utcnow()
            })

        return result

    except Exception as e:
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )

@router.get("/api/superAdmin/stats", tags=['superadmin'])
def get_statistics(user: user_dependency,username: str = None, db: Session = Depends(getdb)):
    try:
        site_query = db.query(Site)

        if username:
            user = db.query(User).filter(User.username == username).first()
            if not user:
                return response_strct(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found",
                    data={},
                    error=""
                )

            site_ids = db.query(SiteUser.site_id).filter(SiteUser.user_id == user.id).subquery()
            site_query = site_query.filter(Site.id.in_(site_ids))

        filtered_sites = site_query.all()
        filtered_site_ids = [site.id for site in filtered_sites]

        total_sites = len(filtered_site_ids)

        total_stations = db.query(Station).filter(Station.site_id.in_(filtered_site_ids)).count()
        total_configured_parameters = db.query(stationParameter).join(Station, stationParameter.station_id == Station.id)\
            .filter(Station.site_id.in_(filtered_site_ids)).count()
        total_configured_devices = db.query(Device).filter(Device.site_id.in_(filtered_site_ids)).count()

        total_active_sites = db.query(Site).filter(
            Site.id.in_(filtered_site_ids),
            (Site.auth_expiry.is_(None)) | (Site.auth_expiry > datetime.datetime.utcnow())
        ).count()

        total_inactive_sites = total_sites - total_active_sites

        return {
            "total_sites": total_sites,
            "total_stations": total_stations,
            "total_parameters": total_configured_parameters,
            "total_devices": total_configured_devices,
            "total_active_sites": total_active_sites,
            "total_inactive_sites": total_inactive_sites
        }

    except Exception as e:
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )
