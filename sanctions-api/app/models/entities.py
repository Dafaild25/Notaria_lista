# app/models/entities.py
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, ForeignKey, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()

class Entity(Base):
    __tablename__ = "entities"
    
    id = Column(Integer, primary_key=True, index=True)
    source = Column(String(10), nullable=False)  # 'OFAC', 'UN'
    source_id = Column(String(50), nullable=False)  # partyID de OFAC o EntityNumber ONU
    name = Column(Text, nullable=False)
    title = Column(String(50))  # Sr., Dr., etc.
    type = Column(String(20), nullable=False)  # INDIVIDUAL, ENTITY
    gender = Column(String(10))
    listed_on = Column(Date)
    last_updated = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    remarks = Column(Text)
    other_info = Column(Text)
    committee = Column(String(50))  # ONU
    resolution = Column(String(50))  # ONU
    status = Column(String(20), default='ACTIVE')  # ACTIVE, DELISTED, UPDATED
    hash_signature = Column(String(64))  # Para detectar cambios
    
    # Relaciones
    aliases = relationship("Alias", back_populates="entity", cascade="all, delete-orphan")
    documents = relationship("Document", back_populates="entity", cascade="all, delete-orphan")
    addresses = relationship("Address", back_populates="entity", cascade="all, delete-orphan")
    births = relationship("Birth", back_populates="entity", cascade="all, delete-orphan")
    nationalities = relationship("Nationality", back_populates="entity", cascade="all, delete-orphan")
    sanctions = relationship("Sanction", back_populates="entity", cascade="all, delete-orphan")
    
    # Índices para optimización
    __table_args__ = (
        Index('idx_entities_source', 'source', 'source_id'),
        Index('idx_entities_name', 'name'),
        Index('idx_entities_status', 'status'),
        Index('idx_entities_type', 'type'),
        Index('idx_entities_updated', 'last_updated'),
    )

class Alias(Base):
    __tablename__ = "aliases"
    
    id = Column(Integer, primary_key=True, index=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    alias_name = Column(Text, nullable=False)
    quality = Column(String(20))  # strong, weak, good, etc.
    language = Column(String(10))
    
    # Relación
    entity = relationship("Entity", back_populates="aliases")
    
    # Índices
    __table_args__ = (
        Index('idx_aliases_name', 'alias_name'),
        Index('idx_aliases_entity', 'entity_id'),
    )

class Document(Base):
    __tablename__ = "documents"
    
    id = Column(Integer, primary_key=True, index=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    doc_type = Column(String(30), nullable=False)
    doc_number = Column(String(100), nullable=False)
    issuer = Column(String(50))
    expiration_date = Column(Date)
    
    # Relación
    entity = relationship("Entity", back_populates="documents")
    
    # Índices
    __table_args__ = (
        Index('idx_documents_number', 'doc_number'),
        Index('idx_documents_type', 'doc_type'),
        Index('idx_documents_entity', 'entity_id'),
    )

class Address(Base):
    __tablename__ = "addresses"
    
    id = Column(Integer, primary_key=True, index=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    full_address = Column(Text)
    city = Column(String(50))
    region = Column(String(50))
    country = Column(String(50))
    postal_code = Column(String(20))
    
    # Relación
    entity = relationship("Entity", back_populates="addresses")
    
    # Índices
    __table_args__ = (
        Index('idx_addresses_country', 'country'),
        Index('idx_addresses_city', 'city'),
        Index('idx_addresses_entity', 'entity_id'),
    )

class Birth(Base):
    __tablename__ = "births"
    
    id = Column(Integer, primary_key=True, index=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    dob = Column(String(50))  # Puede ser fecha parcial o texto
    pob = Column(Text)  # Place of birth
    
    # Relación
    entity = relationship("Entity", back_populates="births")

class Nationality(Base):
    __tablename__ = "nationalities"
    
    id = Column(Integer, primary_key=True, index=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    country = Column(String(50), nullable=False)
    
    # Relación
    entity = relationship("Entity", back_populates="nationalities")
    
    # Índices
    __table_args__ = (
        Index('idx_nationalities_country', 'country'),
        Index('idx_nationalities_entity', 'entity_id'),
    )

class Sanction(Base):
    __tablename__ = "sanctions"
    
    id = Column(Integer, primary_key=True, index=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    program = Column(String(100), nullable=False)
    authority = Column(String(50))  # OFAC, UN, etc.
    listing_date = Column(Date)
    source_url = Column(Text)
    comments = Column(Text)
    
    # Relación
    entity = relationship("Entity", back_populates="sanctions")
    
    # Índices
    __table_args__ = (
        Index('idx_sanctions_program', 'program'),
        Index('idx_sanctions_authority', 'authority'),
        Index('idx_sanctions_entity', 'entity_id'),
    )

class Relationship(Base):
    """Tabla para manejar relaciones entre entidades (especialmente OFAC)"""
    __tablename__ = "relationships"
    
    id = Column(Integer, primary_key=True, index=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    related_entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    relationship_type = Column(String(50), nullable=False)  # OWNED_BY, ALIAS_OF, etc.
    source = Column(String(10), nullable=False)
    
    # Relaciones
    entity = relationship("Entity", foreign_keys=[entity_id])
    related_entity = relationship("Entity", foreign_keys=[related_entity_id])
    
    # Índices
    __table_args__ = (
        Index('idx_relationships_entity', 'entity_id'),
        Index('idx_relationships_related', 'related_entity_id'),
        Index('idx_relationships_type', 'relationship_type'),
    )

class UpdateLog(Base):
    """Control de actualizaciones automáticas"""
    __tablename__ = "update_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    source = Column(String(10), nullable=False)  # OFAC, UN
    update_date = Column(DateTime, default=datetime.utcnow)
    records_added = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_deleted = Column(Integer, default=0)
    file_hash = Column(String(64))
    status = Column(String(20), default='SUCCESS')  # SUCCESS, FAILED, PARTIAL
    error_message = Column(Text)
    
    # Índices
    __table_args__ = (
        Index('idx_update_logs_source', 'source'),
        Index('idx_update_logs_date', 'update_date'),
        Index('idx_update_logs_status', 'status'),
    )

class ApiUsage(Base):
    """Control de uso de API para suscripciones"""
    __tablename__ = "api_usage"
    
    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(String(50), nullable=False)
    query_date = Column(Date, default=func.current_date())
    queries_count = Column(Integer, default=0)
    plan_type = Column(String(20), nullable=False)
    endpoint = Column(String(100))
    
    # Índices
    __table_args__ = (
        Index('idx_api_usage_client', 'client_id'),
        Index('idx_api_usage_date', 'query_date'),
        Index('idx_api_usage_plan', 'plan_type'),
    )

class Client(Base):
    """Tabla de clientes y suscripciones"""
    __tablename__ = "clients"
    
    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(String(50), unique=True, nullable=False)
    api_key = Column(String(100), unique=True, nullable=False)
    client_name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False)
    plan_type = Column(String(20), nullable=False)
    monthly_quota = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    
    # Índices
    __table_args__ = (
        Index('idx_clients_api_key', 'api_key'),
        Index('idx_clients_client_id', 'client_id'),
        Index('idx_clients_active', 'is_active'),
    )