
from sqlalchemy import Column, Integer, String,Boolean
from sqlalchemy.ext.declarative import declarative_base




Base = declarative_base()

class Camera(Base):
    __tablename__ ="camera"
    
    id= Column(Integer,primary_key=True,autoincrement=True)
    url = Column(String,unique=True)
    activated= Column(Boolean,default=False)