from sqlalchemy import create_engine, Column, Integer, String, Date, Text, ForeignKey, Boolean
from sqlalchemy.orm import relationship, declarative_base, Mapped
from typing import List

Base = declarative_base()



class HTAAgency(Base):
    __tablename__ = 'HTA_agency'

    idHTA_agency = Column(Integer, primary_key=True)
    name = Column(String(45))

    personnel: Mapped[List["Personal"]] = relationship(back_populates="HTA_agency")




class Personal(Base):
    __tablename__ = 'personal'
    idpersonal = Column(Integer, primary_key=True)
    title = Column(String(45))
    name = Column(String(45))
    id_agency = Column(ForeignKey("HTA_agency.idHTA_agency"))
    HTA_agency: Mapped["HTAAgency"] = relationship(back_populates="personnel")

