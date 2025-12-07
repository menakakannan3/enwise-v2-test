# from sqlalchemy import Column, Integer, Numeric, DateTime, ForeignKey
# from ..database.base_class import Base

# class LatestSensorData(Base):
#     __tablename__ = "latest_sensor_data"

#     site_id = Column(Integer, ForeignKey("site.id"), nullable=False)
#     station_param_id = Column(Integer, primary_key=True)
#     value = Column(Numeric(10, 2))
#     time = Column(DateTime(timezone=True))
