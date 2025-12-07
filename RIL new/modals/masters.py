from sqlalchemy import Column , Integer , String , ForeignKey , DateTime , Float , DECIMAL , Boolean , Numeric, Text, Date,JSON,TIMESTAMP,UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database.base_class import Base , mapper_registry
import datetime

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False)
    username = Column(String(20), nullable=False)
    password_hash = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    phone = Column(String(10), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))


class Role(Base):
    __tablename__ = "role"
    id = Column(Integer, primary_key=True)
    role_name = Column(String(50))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))


class UserRole(Base):
    __tablename__ = "user_roles"
    id = Column(Integer, primary_key=True)
    role_id = Column(Integer, ForeignKey("role.id"))
    user_id = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))


class Site(Base):
    __tablename__ = "site"
    id = Column(Integer, primary_key=True)
    siteuid = Column(String(20))
    site_name = Column(String)
    address = Column(String)
    city = Column(String)
    state = Column(String)
    ganga_basin=Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    authkey = Column(String)
    auth_expiry = Column(DateTime)
    keyGeneratedDate = Column(DateTime)
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    group_id = Column(Integer, ForeignKey("group.id"))


class SiteUser(Base):
    __tablename__ = "site_users"
    id = Column(Integer, primary_key=True)
    site_id = Column(Integer, ForeignKey("site.id"))
    user_id = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))
    is_active = Column(Boolean, default=True)


class SiteDocument(Base):
    __tablename__ = "site_documents"
    id = Column(Integer, primary_key=True)
    site_id = Column(Integer, ForeignKey("site.id"))
    document_name = Column(String)
    document_format = Column(String)
    document_path = Column(String)
    document_type = Column(Integer , ForeignKey("document_type.id"))
    uploaded_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

class DocumentType(Base):
    __tablename__ = "document_type"
    id = Column(Integer, primary_key=True)
    document_type = Column(String)
    mandatory = Column(Boolean)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))


class MonitoringType(Base):
    __tablename__ = "monitoring_types"
    id = Column(Integer, primary_key=True)
    monitoring_type = Column(String(255))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))


class Parameter(Base):
    __tablename__ = "parameters"
    id = Column(Integer, primary_key=True)
    uuid = Column(String(255))
    name = Column(String(255))
    label = Column(String(255))
    unit = Column(String(50))
    min_thershold = Column(Float)
    max_thershold = Column(Float)
    monitoring_type_id = Column(Integer, ForeignKey("monitoring_types.id"))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))


class Analyser(Base):
    __tablename__ = "analysers"
    id = Column(Integer, primary_key=True)
    analyser_name = Column(String(250))
    analyser_uid = Column(String(250))
    make = Column(String(255))
    model = Column(String(255))
    description = Column(String(255))
    monitoring_type_id = Column(Integer, ForeignKey("monitoring_types.id"))  # Add this line
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))
    monitoring_type = relationship("MonitoringType", backref="analysers")


class Group(Base):
    __tablename__ = "group"
    id = Column(Integer, primary_key=True)
    group_name = Column(String)
    uuid = Column(String(255))
    ind_code=Column(String(100))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))


class Station(Base):
    __tablename__ = "stations"

    id = Column(Integer, primary_key=True)
    station_uid = Column(String(255), unique=True)
    name = Column(String(255))
    calibration_expiry_date = Column(DateTime)
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    site_id = Column(Integer, ForeignKey("site.id"))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))
    calib_from_lst = Column(DateTime, nullable=True)
    calib_to_lst = Column(DateTime, nullable=True)
    calib_ack = Column(DateTime, nullable=True)

    # 🔥 REQUIRED FIX
    calib_histories = relationship(
        "CalibrationHistory",
        back_populates="station",
        cascade="all, delete-orphan"
    )

    devices = relationship(
        "Device",
        secondary="device_station",
        back_populates="stations",
    )


class SiteLevelParameterThreshold(Base):
    __tablename__ = "site_level_parameter_threshold"
    id = Column(Integer, primary_key=True)
    parameter_id = Column(Integer, ForeignKey("parameters.id"))
    site_id = Column(Integer, ForeignKey("site.id"))
    site_level_threshold = Column(Float)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))

class AnalyserParameter(Base):
    __tablename__ = "analyser_parameter"
    
    id = Column(Integer, primary_key=True , autoincrement=True)
    analyser_id = Column(Integer, ForeignKey("analysers.id"))
    parameter_id = Column(Integer, ForeignKey("parameters.id"))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))


class stationParameter(Base):
    __tablename__ = "station_parameters"
    id = Column(Integer , primary_key=True , autoincrement=True)
    station_id = Column(Integer , ForeignKey("stations.id"))
    analyser_param_id = Column(Integer , ForeignKey("analyser_parameter.id"))
    pram_lable = Column(String)
    para_unit      = Column(String, nullable=True)
    para_threshold = Column(Float,  nullable=True)
    is_editable     = Column(Boolean, nullable=False, server_default="true")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))
    param_interval = Column(Integer, nullable=True)


class SiteAnalyser(Base):
    __tablename__ = "site_analyser"
    id = Column(Integer , primary_key=True)
    site_id = Column(Integer , ForeignKey("site.id"))
    analyser_id = Column(Integer , ForeignKey("analysers.id"))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))


class Device(Base):
   # __tablename__ = "device"
    id = Column(Integer, primary_key=True)
    device_uid = Column(String(20), unique=True)
    device_name = Column(String(200), unique=True)
    device_type = Column(String)
    chip_id = Column(String)
    latitude = Column(Numeric(18, 14)) 
    longitute = Column(Numeric(18, 14))  
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now())
    site_id = Column(Integer, ForeignKey("site.id", ondelete="SET NULL"), nullable=True)
    last_ping = Column(DateTime, nullable=True)
    status = Column(String)
    device_status = Column(String(20), nullable=True)
    device_authkey = Column(String)
    # created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    # updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))

    site = relationship("Site", backref="devices")
    stations = relationship(
        "Station",
        secondary="device_station",
        back_populates="devices",
    )


class SensorData(Base):
    __tablename__ = "sensor_data"

    time = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    site_id = Column(Integer, ForeignKey("site.id", ondelete="CASCADE"), nullable=False)
    station_id = Column(Integer, ForeignKey("stations.id", ondelete="CASCADE"), nullable=False)
    station_param_id = Column(Integer , ForeignKey("station_parameters.id" , ondelete="CASCADE" ))
    device_id = Column(Integer, ForeignKey("device.id", ondelete="CASCADE"), nullable=False)
    analyser_id = Column(Integer, ForeignKey("analysers.id", ondelete="CASCADE"), nullable=False)
    parameter_id = Column(Integer, ForeignKey("parameters.id", ondelete="CASCADE"), nullable=False)
    param_label = Column(String)
    qualityCode = Column(String)
    value = Column(Numeric(10, 2), nullable=False)


class Camera(Base):
    __tablename__ = "camera"
    id = Column(Integer , primary_key=True)
    station_id = Column(Integer , ForeignKey("stations.id", ondelete="CASCADE")  , nullable=False)
    make = Column(String(255))
    modal = Column(String(255))
    rtsp_link = Column(String(255))
    connectivity_type = Column(String(255))
    location = Column(String(255))
    bandwidth = Column(String)
    night_vision = Column(Boolean)
    ptz = Column(Boolean)
    zoom = Column(Boolean)
    ipc_camera = Column(Boolean)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"))    

class CameraParameter(Base):
    __tablename__ = "camera_parameter"

    id = Column(Integer, primary_key=True, autoincrement=True)
    camera_id = Column(Integer, ForeignKey("camera.id", ondelete="CASCADE"), nullable=False)
    station_parameter_id = Column(Integer, ForeignKey("station_parameters.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    updated_by = Column(Integer, ForeignKey("users.id"))

class SiteStatus(Base):
    __tablename__ = "site_status"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    starttime = Column(DateTime(timezone=True), nullable=False)
    endtime = Column(DateTime(timezone=True), nullable=True)
    station_param_id = Column(Integer, ForeignKey("station_parameters.id"), nullable=False)
    status = Column(String(10), nullable=False)

class DailyTotaliserBase(Base):
    __tablename__ = "daily_totaliser_base"

    station_param_id = Column(Integer, primary_key=True)
    base_date = Column(Date, primary_key=True)
    base_value = Column(Numeric(12, 2), nullable=False)

class TotaliserData(Base):
    __tablename__ = "totaliser_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(Integer, nullable=False)
    parameter_name = Column(String(50), nullable=False)  # T1, T2, etc
    kld_value = Column(Numeric(12, 2), nullable=True)     # Today's 6AM value
    kld_time = Column(DateTime(timezone=True), nullable=True)  # Time of first value after 6AM
    klm_value = Column(Numeric(12, 2), nullable=True)     # First of month 6AM value
    klm_time = Column(DateTime(timezone=True), nullable=True)  # Time of first value after 6AM of month start
    tot_last        = Column(Numeric(12, 2), nullable=True)     # Most recent totaliser value
    tot_time        = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class DashboardPageFormulas(Base):
    __tablename__ = "dashboard_page_formulas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(Integer, ForeignKey("site.id"), nullable=False)
    page_name = Column(Text, nullable=False)
    formulas = Column(JSON, nullable=False)
    positions = Column(JSON, nullable=True)
    connections = Column(JSON, nullable=True)
    table_formulae = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class DailyTotaliserUsage(Base):
    __tablename__ = "daily_totaliser_usage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(Integer, nullable=False)
    parameter_name = Column(String(50), nullable=False)
    date = Column(Date, nullable=False)
    value_6am = Column(Numeric(12, 2), nullable=True)
    time_6am = Column(TIMESTAMP(timezone=True), nullable=True)
    value_end_of_day = Column(Numeric(12, 2), nullable=True)
    time_end_of_day = Column(TIMESTAMP(timezone=True), nullable=True)
    usage = Column(Numeric(12, 2), nullable=True)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class StationFormula(Base):
    __tablename__ = "station_formulas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_name = Column(String(100), nullable=False)
    station_name = Column(String(100), nullable=False)
    formula = Column(String, nullable=False)  # e.g., 'T10 + T9 - T2'
    cfo_limit_kld = Column(Numeric(10, 2), nullable=True)
    cfo_limit_klm = Column(Numeric(12, 2), nullable=True)
    is_active = Column(Boolean, default=True)
    is_alarm = Column(Boolean, default=True)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class DeviceStation(Base):
    __tablename__ = "device_station"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    device_id  = Column(Integer, ForeignKey("device.id", ondelete="CASCADE"), nullable=False)
    station_id = Column(Integer, ForeignKey("stations.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class LatestSensorData(Base):
    __tablename__ = "latest_sensor_data"

    site_id = Column(Integer, ForeignKey("site.id"), nullable=False)
    station_param_id = Column(Integer, primary_key=True)
    value = Column(Numeric(10, 2))
    time = Column(DateTime(timezone=True))

class CalibrationHistory(Base):
    __tablename__ = "calib_history"

    id = Column(Integer, primary_key=True)
    site_id = Column(Integer, ForeignKey("site.id"), nullable=False)
    station_id = Column(Integer, ForeignKey("stations.id"))
    calib_from = Column(DateTime, nullable=True)   # Stored in UTC
    calib_to = Column(DateTime, nullable=True)     # Stored in UTC
    created_at = Column(DateTime, server_default=func.now())

    station = relationship("Station", back_populates="calib_histories")
