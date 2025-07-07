# app/schemas/entities.py
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import date, datetime
from enum import Enum

class SourceEnum(str, Enum):
    OFAC = "OFAC"
    UN = "UN"

class EntityTypeEnum(str, Enum):
    INDIVIDUAL = "INDIVIDUAL"
    ENTITY = "ENTITY"
    VESSEL = "VESSEL"
    AIRCRAFT = "AIRCRAFT"

class StatusEnum(str, Enum):
    ACTIVE = "ACTIVE"
    DELISTED = "DELISTED"
    UPDATED = "UPDATED"

class AliasQualityEnum(str, Enum):
    STRONG = "STRONG"
    WEAK = "WEAK"
    GOOD = "GOOD"
    UNKNOWN = "UNKNOWN"
    AKA = "AKA"
    STRONG_AKA = "STRONG_AKA"
    LOW = "LOW"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"

# Schemas base para los componentes
class AliasBase(BaseModel):
    alias_name: str
    quality: Optional[str] = None  # Cambiar a str flexible
    language: Optional[str] = None

class Alias(AliasBase):
    id: int
    entity_id: int
    
    class Config:
        from_attributes = True

class DocumentBase(BaseModel):
    doc_type: str
    doc_number: str
    issuer: Optional[str] = None
    expiration_date: Optional[date] = None

class Document(DocumentBase):
    id: int
    entity_id: int
    
    class Config:
        from_attributes = True

class AddressBase(BaseModel):
    full_address: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None
    postal_code: Optional[str] = None

class Address(AddressBase):
    id: int
    entity_id: int
    
    class Config:
        from_attributes = True

class BirthBase(BaseModel):
    dob: Optional[str] = None
    pob: Optional[str] = None

class Birth(BirthBase):
    id: int
    entity_id: int
    
    class Config:
        from_attributes = True

class NationalityBase(BaseModel):
    country: str

class Nationality(NationalityBase):
    id: int
    entity_id: int
    
    class Config:
        from_attributes = True

class SanctionBase(BaseModel):
    program: str
    authority: Optional[str] = None
    listing_date: Optional[date] = None
    source_url: Optional[str] = None
    comments: Optional[str] = None

class Sanction(SanctionBase):
    id: int
    entity_id: int
    
    class Config:
        from_attributes = True

# Schema principal para entidades
class EntityBase(BaseModel):
    source: SourceEnum
    source_id: str
    name: str
    title: Optional[str] = None
    type: EntityTypeEnum
    gender: Optional[str] = None
    listed_on: Optional[date] = None
    remarks: Optional[str] = None
    other_info: Optional[str] = None
    committee: Optional[str] = None
    resolution: Optional[str] = None
    status: StatusEnum = StatusEnum.ACTIVE

class Entity(EntityBase):
    id: int
    created_at: datetime
    last_updated: datetime
    
    # Relaciones
    aliases: List[Alias] = []
    documents: List[Document] = []
    addresses: List[Address] = []
    births: List[Birth] = []
    nationalities: List[Nationality] = []
    sanctions: List[Sanction] = []
    
    class Config:
        from_attributes = True

# Schema para respuestas de búsqueda
class SearchMatch(BaseModel):
    score: float = Field(..., description="Puntuación de coincidencia (0-1)")
    match_type: str = Field(..., description="Tipo de coincidencia: exact, fuzzy, alias")
    matched_field: str = Field(..., description="Campo que coincidió")
    matched_value: str = Field(..., description="Valor que coincidió")

class EntitySearchResult(BaseModel):
    entity: Entity
    match_info: SearchMatch
    
    class Config:
        from_attributes = True

# Schema para respuestas de búsqueda
class SearchResponse(BaseModel):
    query: str
    total_results: int
    results: List[EntitySearchResult]
    search_time_ms: float
    filters_applied: Optional[Dict[str, Any]] = None

# Schema para estadísticas
class DatabaseStats(BaseModel):
    total_entities: int
    entities_by_source: Dict[str, int]
    entities_by_type: Dict[str, int]
    last_update: Optional[datetime] = None
    total_aliases: int
    total_addresses: int
    total_documents: int

# Schema para información del cliente
class ClientInfo(BaseModel):
    client_id: str
    client_name: str
    plan_type: str
    monthly_quota: int
    queries_used_this_month: int
    queries_remaining: int
    
    class Config:
        from_attributes = True

# Schema para respuestas de error
class ErrorResponse(BaseModel):
    error: str
    detail: str
    timestamp: datetime
    path: Optional[str] = None

# Schema para filtros de búsqueda
class SearchFilters(BaseModel):
    source: Optional[SourceEnum] = None
    entity_type: Optional[EntityTypeEnum] = None
    country: Optional[str] = None
    status: Optional[StatusEnum] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None

# Schema para parámetros de búsqueda
class SearchParams(BaseModel):
    q: str = Field(..., description="Término de búsqueda")
    limit: int = Field(default=20, ge=1, le=100, description="Número máximo de resultados")
    offset: int = Field(default=0, ge=0, description="Número de resultados a saltar")
    filters: Optional[SearchFilters] = None
    include_aliases: bool = Field(default=True, description="Incluir búsqueda en aliases")
    min_score: float = Field(default=0.5, ge=0.0, le=1.0, description="Puntuación mínima")

# Schema para respuestas de health check
class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    database_status: str
    last_update_check: Optional[datetime] = None