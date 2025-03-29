from __future__ import annotations
from typing import Dict
from __future__ import annotations
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_keyed_dict


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "product"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(64))

    product_company_associations: Mapped[Dict[str, ProductCompanyAssociation]] = relationship(
        back_populates="product",
        collection_class=attribute_keyed_dict("special_key"),
        cascade="all, delete-orphan",
    )
    # the same 'product_company_associations'->'company' proxy as in
    # the basic dictionary example.
    companys: AssociationProxy[Dict[str, str]] = association_proxy(
        "product_company_associations",
        "company",
        creator=lambda k, v: ProductCompanyAssociation(special_key=k, company=v),
    )

    def __init__(self, name: str):
        self.name = name


class ProductCompanyAssociation(Base):
    __tablename__ = "product_company"
    product_id: Mapped[int] = mapped_column(ForeignKey("product.id"), primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("company.id"), primary_key=True)
    special_key: Mapped[str] = mapped_column(String(64))
    product: Mapped[Product] = relationship(
        back_populates="product_company_associations",
    )

    # the relationship to company is now called
    # 'kw'
    kw: Mapped[Company] = relationship()

    # 'company' is changed to be a proxy to the
    # 'company' attribute of 'company'
    company: AssociationProxy[Dict[str, str]] = association_proxy("kw", "company")


class Company(Base):
    __tablename__ = "company"
    id: Mapped[int] = mapped_column(primary_key=True)
    company: Mapped[str] = mapped_column(String(64))

    def __init__(self, company: str):
        self.company = company