# app/api/routes.py
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Header
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, func, desc
from typing import Optional, List
import time
import logging
from datetime import date, datetime
from sqlalchemy import func

from app.models.database import get_db
from app.models.entities import Entity, Alias, Address, Document, Client, ApiUsage, UpdateLog
from app.schemas.entities import (
    Entity as EntitySchema,
    SearchResponse, 
    EntitySearchResult,
    SearchMatch,
    DatabaseStats,
    ClientInfo,
    SearchFilters
)
from app.core.config import settings

# Configurar logging
logger = logging.getLogger(__name__)

# Crear router
router = APIRouter()

# Importar dependencia de autenticación
async def get_authenticated_client(
    x_api_key: str = Header(..., description="API Key para autenticación"),
    db: Session = Depends(get_db)
) -> Client:
    """Dependencia para obtener cliente autenticado"""
    from app.models.entities import Client
    
    # Buscar cliente por API Key
    client = db.query(Client).filter(
        Client.api_key == x_api_key,
        Client.is_active == True
    ).first()
    
    if not client:
        raise HTTPException(
            status_code=401,
            detail="API Key inválida o inactiva"
        )
    
    return client

# Funciones auxiliares para búsqueda
def calculate_similarity_score(query: str, text: str) -> float:
    """Calcular puntuación de similitud simple"""
    if not query or not text:
        return 0.0
    
    query_lower = query.lower()
    text_lower = text.lower()
    
    # Coincidencia exacta
    if query_lower == text_lower:
        return 1.0
    
    # Coincidencia parcial
    if query_lower in text_lower:
        return 0.8
    
    # Coincidencia por palabras
    query_words = set(query_lower.split())
    text_words = set(text_lower.split())
    
    if query_words & text_words:
        intersection = len(query_words & text_words)
        union = len(query_words | text_words)
        return intersection / union * 0.6
    
    return 0.0

def build_search_query(db: Session, query: str, filters: Optional[SearchFilters] = None):
    """Construir consulta de búsqueda con filtros"""
    
    # Consulta base
    base_query = db.query(Entity)
    
    # Aplicar filtros
    if filters:
        if filters.source:
            base_query = base_query.filter(Entity.source == filters.source.value)
        if filters.entity_type:
            base_query = base_query.filter(Entity.type == filters.entity_type.value)
        if filters.status:
            base_query = base_query.filter(Entity.status == filters.status.value)
        if filters.date_from:
            base_query = base_query.filter(Entity.listed_on >= filters.date_from)
        if filters.date_to:
            base_query = base_query.filter(Entity.listed_on <= filters.date_to)
    
    # Búsqueda por texto
    search_conditions = []
    
    # Búsqueda en nombre principal
    search_conditions.append(Entity.name.ilike(f"%{query}%"))
    
    # Búsqueda en aliases
    alias_subquery = db.query(Alias.entity_id).filter(
        Alias.alias_name.ilike(f"%{query}%")
    )
    search_conditions.append(Entity.id.in_(alias_subquery))
    
    # Búsqueda en direcciones
    address_subquery = db.query(Address.entity_id).filter(
        or_(
            Address.full_address.ilike(f"%{query}%"),
            Address.city.ilike(f"%{query}%"),
            Address.country.ilike(f"%{query}%")
        )
    )
    search_conditions.append(Entity.id.in_(address_subquery))
    
    # Búsqueda en documentos
    doc_subquery = db.query(Document.entity_id).filter(
        Document.doc_number.ilike(f"%{query}%")
    )
    search_conditions.append(Entity.id.in_(doc_subquery))
    
    # Aplicar condiciones de búsqueda
    base_query = base_query.filter(or_(*search_conditions))
    
    return base_query

@router.get("/search", response_model=SearchResponse)
async def search_entities(
    q: str = Query(..., description="Término de búsqueda"),
    limit: int = Query(default=20, ge=1, le=100, description="Número máximo de resultados"),
    offset: int = Query(default=0, ge=0, description="Número de resultados a saltar"),
    source: Optional[str] = Query(None, description="Filtrar por fuente: OFAC, UN"),
    entity_type: Optional[str] = Query(None, description="Filtrar por tipo: INDIVIDUAL, ENTITY"),
    country: Optional[str] = Query(None, description="Filtrar por país"),
    min_score: float = Query(default=0.5, ge=0.0, le=1.0, description="Puntuación mínima"),
    db: Session = Depends(get_db)
):
    """Buscar entidades en las listas de sanciones"""
    
    start_time = time.time()
    
    try:
        # Crear filtros
        filters = SearchFilters()
        if source:
            filters.source = source
        if entity_type:
            filters.entity_type = entity_type
        if country:
            filters.country = country
        
        # Construir consulta
        query_obj = build_search_query(db, q, filters)
        
        # Ejecutar consulta con paginación
        entities = query_obj.offset(offset).limit(limit).all()
        total_count = query_obj.count()
        
        # Calcular puntuaciones y crear resultados
        results = []
        for entity in entities:
            # Calcular puntuación de coincidencia
            name_score = calculate_similarity_score(q, entity.name)
            
            # Verificar coincidencia en aliases
            alias_score = 0.0
            matched_alias = None
            for alias in entity.aliases:
                score = calculate_similarity_score(q, alias.alias_name)
                if score > alias_score:
                    alias_score = score
                    matched_alias = alias.alias_name
            
            # Determinar mejor coincidencia
            if name_score >= alias_score:
                best_score = name_score
                match_type = "exact" if name_score == 1.0 else "fuzzy"
                matched_field = "name"
                matched_value = entity.name
            else:
                best_score = alias_score
                match_type = "alias"
                matched_field = "alias"
                matched_value = matched_alias
            
            # Filtrar por puntuación mínima
            if best_score >= min_score:
                match_info = SearchMatch(
                    score=best_score,
                    match_type=match_type,
                    matched_field=matched_field,
                    matched_value=matched_value
                )
                
                results.append(EntitySearchResult(
                    entity=entity,
                    match_info=match_info
                ))
        
        # Ordenar por puntuación
        results.sort(key=lambda x: x.match_info.score, reverse=True)
        
        search_time = (time.time() - start_time) * 1000  # en milisegundos
        
        return SearchResponse(
            query=q,
            total_results=len(results),
            results=results,
            search_time_ms=round(search_time, 2),
            filters_applied=filters.dict() if filters else None
        )
        
    except Exception as e:
        logger.error(f"Error en búsqueda: {e}")
        raise HTTPException(status_code=500, detail="Error interno en la búsqueda")

@router.get("/entity/{entity_id}", response_model=EntitySchema)
async def get_entity(
    entity_id: int = Path(..., description="ID de la entidad"),
    db: Session = Depends(get_db)
):
    """Obtener detalles completos de una entidad"""
    
    entity = db.query(Entity).filter(Entity.id == entity_id).first()
    
    if not entity:
        raise HTTPException(status_code=404, detail="Entidad no encontrada")
    
    return entity

@router.get("/stats", response_model=DatabaseStats)
async def get_database_stats(db: Session = Depends(get_db)):
    """Obtener estadísticas de la base de datos"""
    
    try:
        # Estadísticas generales
        total_entities = db.query(Entity).count()
        total_aliases = db.query(Alias).count()
        total_addresses = db.query(Address).count()
        total_documents = db.query(Document).count()
        
        # Estadísticas por fuente
        entities_by_source = {}
        for source in ["OFAC", "UN"]:
            count = db.query(Entity).filter(Entity.source == source).count()
            entities_by_source[source] = count
        
        # Estadísticas por tipo
        entities_by_type = {}
        for entity_type in ["INDIVIDUAL", "ENTITY", "VESSEL", "AIRCRAFT"]:
            count = db.query(Entity).filter(Entity.type == entity_type).count()
            entities_by_type[entity_type] = count
        
        # Última actualización
        last_update = db.query(UpdateLog).order_by(desc(UpdateLog.update_date)).first()
        last_update_date = last_update.update_date if last_update else None
        
        return DatabaseStats(
            total_entities=total_entities,
            entities_by_source=entities_by_source,
            entities_by_type=entities_by_type,
            last_update=last_update_date,
            total_aliases=total_aliases,
            total_addresses=total_addresses,
            total_documents=total_documents
        )
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo estadísticas")

@router.get("/client-info", response_model=ClientInfo)
async def get_client_info(
    client: Client = Depends(get_authenticated_client),
    db: Session = Depends(get_db)
):
    """Obtener información del cliente y uso de API"""
    
    try:
        # Calcular uso del mes actual
        today = date.today()
        month_start = today.replace(day=1)
        
        # CORREGIDO: Usar func.sum() en lugar de count()
        queries_used_result = db.query(func.sum(ApiUsage.queries_count)).filter(
            ApiUsage.client_id == client.client_id,
            ApiUsage.query_date >= month_start
        ).scalar()
        
        queries_used = int(queries_used_result) if queries_used_result is not None else 0
        queries_remaining = max(0, client.monthly_quota - queries_used) if client.monthly_quota > 0 else -1
        
        return ClientInfo(
            client_id=client.client_id,
            client_name=client.client_name,
            plan_type=client.plan_type,
            monthly_quota=client.monthly_quota,
            queries_used_this_month=queries_used,
            queries_remaining=queries_remaining
        )
        
    except Exception as e:
        logger.error(f"Error obteniendo info del cliente: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo información del cliente")

@router.get("/sources")
async def get_data_sources():
    """Obtener información sobre las fuentes de datos"""
    
    return {
        "sources": [
            {
                "name": "OFAC",
                "description": "Office of Foreign Assets Control (US Treasury)",
                "url": "https://sanctionslistservice.ofac.treas.gov/",
                "update_frequency": "Daily",
                "lists": ["SDN", "Consolidated"]
            },
            {
                "name": "UN",
                "description": "United Nations Security Council",
                "url": "https://scsanctions.un.org/",
                "update_frequency": "Weekly",
                "lists": ["Consolidated"]
            }
        ],
        "last_updated": datetime.now().isoformat()
    }