import sqlalchemy as sqlal
import pandas as pd

from sqlalchemy import ForeignKey, String
from sqlalchemy import Column, Integer, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
     pass


class HTADocument(Base):
     __tablename__ = "HTA_document"
     
     id = mapped_column(Integer, primary_key=True)
     title = mapped_column(String(100))
     diarie = mapped_column(String(10))
     date = mapped_column(DateTime)
     company = relationship("Company", back_populates="HTADocument")
     id_company = mapped_column(ForeignKey("company.id"))
     id_product
     indications

class Company(Base):
     __tablename__ = "company"
     HTADocuments = relationship("HTADocument", back_populates="Company")

class Product(Base):
     __tablename__ = "product"
     HTADocuments = relationship("HTADocument", back_populates="Product")