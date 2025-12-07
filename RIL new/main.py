from app.api.parameter.parameterCreation import router as parameterRouter
from app.api.group.groupCreation import router as groupRouter
from app.api.users.userCreation import router as userRouter
from app.api.monitoring_types.monitoring_type_creation import router as monitoringTypeRouter
from app.api.site.site_creation import router as siteRouter
from app.api.analysers.analyserCreation import router as analyserRouter
from app.api.station.stationCreation import router as stationRouter
from app.api.analyser_param.analyser_paramCreation import router as analyserParameterRouter
from app.api.site_analysers.site_analyserCreation import router as siteAnalyserRouter
from app.api.station_parameter.station_parameter import router as stationParameterRouter
from app.api.device.deviceCreation import router as deviceRouter
from app.api.camera.cameraCRUD import router as cameraRouter
from app.api.roles.role_CRUD import router as roleRouter
from app.api.auth.authentication import router as authRouter
from app.api.site.site_dashboard import router as siteDashboardRouter
from app.api.site_user.site_user_CRUD import router as siteUserRouter
from app.api.aggrgatedData.chart import router as ChartRouter
from app.api.realtime.realtimeData import router as realTimeRouter
from app.api.site_dashboard.getCameras import router as dashCamRouter
from app.api.site_dashboard.avg_report import router as avgReport
from app.api.site_dashboard.site_report import router as siteReportRouter
from app.api.site.siteStatus import router as siteStatusRouter
from app.api.reportgenerator.offline_report import router as offlinereportRouter
from app.api.site.stations_parameters import router as siteStationParameterRouter
from app.api.reportgenerator.offline_report import router as offlinereportRouter
from app.api.reportgenerator.offlineworking_report import router as offlineworkingreportRouter
from app.api.camera.cameraParameter import router as camParaRouter
from app.api.site.siteMapView import router as mapviewRouter
from app.api.superadmin.supradmin import router as suprAdminRouter
from app.api.reportgenerator.para_offline_report import router as paraOfflineReport
from app.api.reportgenerator.raw_data_api import router as rawdataReport
from app.api.reportgenerator.raw_data import router as rawdataExportReport
from app.api.document_type.documentTypeCRUD import router as documentTypeRouter
from app.api.reportgenerator.graph_report import router as graphreportRouter
from app.api.reportgenerator.data_availability import router as dataAvleportRouter
from app.api.ptz.ptzControl import router as ptzControlRouter
from app.api.waterBalance.waterBalance import router as waterBalanceRouter
from app.api.station_formula.station_formula import router as stationFormulaRouter
from app.api.parameter.alerts_api import router as alertRouter
from app.api.stationCalibration.station_calibration import router as stationCalibrationRouter
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware

from app.core.config import settings


from fastapi import FastAPI , APIRouter
from starlette.middleware.cors import CORSMiddleware
import os

def include_routers(app):
    app.include_router(documentTypeRouter)
    app.include_router(paraOfflineReport)
    app.include_router(suprAdminRouter)
    app.include_router(mapviewRouter)
    app.include_router(camParaRouter)
    app.include_router(offlinereportRouter)
    app.include_router(siteStationParameterRouter)
    app.include_router(siteStatusRouter)
    app.include_router(siteReportRouter)
    app.include_router(realTimeRouter)
    app.include_router(dashCamRouter)
    app.include_router(ChartRouter)
    app.include_router(authRouter)
    app.include_router(siteDashboardRouter)
    app.include_router(siteUserRouter)
    app.include_router(parameterRouter)
    app.include_router(groupRouter)
    app.include_router(userRouter)
    app.include_router(monitoringTypeRouter)
    app.include_router(siteRouter)
    app.include_router(analyserRouter)
    app.include_router(stationRouter)
    app.include_router(analyserParameterRouter)
    app.include_router(siteAnalyserRouter)
    app.include_router(stationParameterRouter)
    app.include_router(deviceRouter)
    app.include_router(cameraRouter)
    app.include_router(roleRouter)
    app.include_router(offlineworkingreportRouter)
    app.include_router(rawdataReport)
    app.include_router(rawdataExportReport)
    # app.include_router(ptzControlRouter)
    app.include_router(waterBalanceRouter)
    app.include_router(graphreportRouter)
    app.include_router(stationFormulaRouter)
    app.include_router(alertRouter)
    app.include_router(avgReport)
    app.include_router(dataAvleportRouter)
    app.include_router(stationCalibrationRouter)
    return

def include_static_files(app):
    """
    Mount static files like logos and documents to serve them publicly.
    """
    # Mount the 'uploads' directory to serve files at /uploads/
    app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

def start_application():
    app = FastAPI(docs_url="/api/docs")
    
    app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cems.ril.com"],  
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],  
    expose_headers=["Content-Disposition"],  
)


    include_routers(app)
    include_static_files(app)
    app.add_middleware(GZipMiddleware, minimum_size=512)

    return app

app = start_application()