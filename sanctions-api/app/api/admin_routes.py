# app/api/admin_routes.py
from fastapi import APIRouter, Depends, HTTPException, Query, Header
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_
from typing import Optional, List
from datetime import date, datetime, timedelta
import uuid
import logging

from app.models.database import get_db
from app.models.entities import Client, ApiUsage, Entity, UpdateLog
from app.schemas.entities import ClientInfo
from pydantic import BaseModel

# Configurar logging
logger = logging.getLogger(__name__)

# Crear router para admin
admin_router = APIRouter(prefix="/admin", tags=["admin"])

# Schemas para admin
class CreateClientRequest(BaseModel):
    client_name: str
    email: str
    plan_type: str = "starter"  # starter, premium, enterprise
    monthly_quota: int = 100

class ClientResponse(BaseModel):
    client_id: str
    client_name: str
    email: str
    plan_type: str
    monthly_quota: int
    is_active: bool
    api_key: str
    created_at: datetime
    queries_used_this_month: int
    queries_remaining: int

class UsageStats(BaseModel):
    client_id: str
    client_name: str
    plan_type: str
    queries_today: int
    queries_this_week: int
    queries_this_month: int
    last_query: Optional[datetime]
    quota_utilization: float  # Porcentaje de quota usado

class ApiUsageDetail(BaseModel):
    date: date
    client_id: str
    client_name: str
    query_count: int
    endpoint: str

class AdminStats(BaseModel):
    total_clients: int
    active_clients: int
    total_queries_today: int
    total_queries_this_month: int
    top_clients: List[UsageStats]
    recent_activities: List[ApiUsageDetail]

# Middleware de autenticación para admin
async def get_admin_access(
    x_admin_key: str = Header(..., description="Admin API Key"),
    db: Session = Depends(get_db)
):
    """Dependencia para verificar acceso de administrador"""
    
    # Por ahora usamos una clave fija, después puedes implementar usuarios admin
    ADMIN_API_KEY = "admin_key_12345"  # Cambiar por variable de entorno
    
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Acceso denegado: Clave de administrador inválida"
        )
    
    return True

@admin_router.get("/clients", response_model=List[ClientResponse])
async def get_all_clients(
    _: bool = Depends(get_admin_access),
    db: Session = Depends(get_db),
    active_only: bool = Query(False, description="Solo clientes activos"),
    limit: int = Query(100, le=500, description="Límite de resultados")
):
    """Obtener lista de todos los clientes"""
    
    try:
        # Consulta base
        query = db.query(Client)
        
        if active_only:
            query = query.filter(Client.is_active == True)
        
        clients = query.limit(limit).all()
        
        # Calcular estadísticas de uso para cada cliente
        result = []
        today = date.today()
        month_start = today.replace(day=1)
        
        for client in clients:
            # Sumar consultas del mes actual (manejar caso sin registros)
            queries_used_result = db.query(func.sum(ApiUsage.queries_count)).filter(
                ApiUsage.client_id == client.client_id,
                ApiUsage.query_date >= month_start
            ).scalar()
            queries_used = int(queries_used_result) if queries_used_result is not None else 0
            
            queries_remaining = max(0, client.monthly_quota - queries_used) if client.monthly_quota > 0 else -1
            
            result.append(ClientResponse(
                client_id=client.client_id,
                client_name=client.client_name,
                email=client.email,
                plan_type=client.plan_type,
                monthly_quota=client.monthly_quota,
                is_active=client.is_active,
                api_key=client.api_key,
                created_at=client.created_at,
                queries_used_this_month=queries_used,
                queries_remaining=queries_remaining
            ))
        
        return result
        
    except Exception as e:
        logger.error(f"Error obteniendo clientes: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo lista de clientes")

@admin_router.post("/clients", response_model=ClientResponse)
async def create_client(
    client_data: CreateClientRequest,
    _: bool = Depends(get_admin_access),
    db: Session = Depends(get_db)
):
    """Crear un nuevo cliente"""
    
    try:
        # Verificar si el email ya existe
        existing_client = db.query(Client).filter(Client.email == client_data.email).first()
        if existing_client:
            raise HTTPException(status_code=400, detail="El email ya está en uso")
        
        # Generar client_id y API key únicos
        client_id = f"client_{uuid.uuid4().hex[:8]}"
        api_key = str(uuid.uuid4())
        
        # Configurar quota según el plan
        quota_map = {
            "starter": 100,
            "premium": 1000,
            "enterprise": 10000
        }
        monthly_quota = quota_map.get(client_data.plan_type, 100)
        
        # Crear cliente
        new_client = Client(
            client_id=client_id,
            api_key=api_key,
            client_name=client_data.client_name,
            email=client_data.email,
            plan_type=client_data.plan_type,
            monthly_quota=monthly_quota,
            is_active=True
        )
        
        db.add(new_client)
        db.commit()
        db.refresh(new_client)
        
        return ClientResponse(
            client_id=new_client.client_id,
            client_name=new_client.client_name,
            email=new_client.email,
            plan_type=new_client.plan_type,
            monthly_quota=new_client.monthly_quota,
            is_active=new_client.is_active,
            api_key=new_client.api_key,
            created_at=new_client.created_at,
            queries_used_this_month=0,
            queries_remaining=new_client.monthly_quota
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creando cliente: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Error creando cliente")

@admin_router.get("/usage-stats", response_model=List[UsageStats])
async def get_usage_statistics(
    _: bool = Depends(get_admin_access),
    db: Session = Depends(get_db),
    days: int = Query(30, description="Días a analizar")
):
    """Obtener estadísticas de uso por cliente"""
    
    try:
        # Calcular fechas
        today = date.today()
        week_start = today - timedelta(days=7)
        month_start = today.replace(day=1)
        period_start = today - timedelta(days=days)
        
        # Obtener todos los clientes activos
        clients = db.query(Client).filter(Client.is_active == True).all()
        
        result = []
        
        for client in clients:
            # Consultas hoy (manejar casos sin registros)
            queries_today_result = db.query(func.sum(ApiUsage.queries_count)).filter(
                ApiUsage.client_id == client.client_id,
                ApiUsage.query_date == today
            ).scalar()
            queries_today = int(queries_today_result) if queries_today_result is not None else 0
            
            # Consultas esta semana
            queries_this_week_result = db.query(func.sum(ApiUsage.queries_count)).filter(
                ApiUsage.client_id == client.client_id,
                ApiUsage.query_date >= week_start
            ).scalar()
            queries_this_week = int(queries_this_week_result) if queries_this_week_result is not None else 0
            
            # Consultas este mes
            queries_this_month_result = db.query(func.sum(ApiUsage.queries_count)).filter(
                ApiUsage.client_id == client.client_id,
                ApiUsage.query_date >= month_start
            ).scalar()
            queries_this_month = int(queries_this_month_result) if queries_this_month_result is not None else 0
            
            # Última consulta (fecha más reciente con consultas > 0)
            last_usage = db.query(ApiUsage).filter(
                ApiUsage.client_id == client.client_id,
                ApiUsage.queries_count > 0
            ).order_by(desc(ApiUsage.query_date)).first()
            
            # Calcular utilización de quota
            quota_utilization = 0.0
            if client.monthly_quota > 0:
                quota_utilization = (queries_this_month / client.monthly_quota) * 100
            
            result.append(UsageStats(
                client_id=client.client_id,
                client_name=client.client_name,
                plan_type=client.plan_type,
                queries_today=queries_today,
                queries_this_week=queries_this_week,
                queries_this_month=queries_this_month,
                last_query=last_usage.query_date if last_usage else None,
                quota_utilization=min(quota_utilization, 100.0)
            ))
        
        # Ordenar por utilización de quota descendente
        result.sort(key=lambda x: x.quota_utilization, reverse=True)
        
        return result
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas de uso: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo estadísticas")

@admin_router.get("/api-usage", response_model=AdminStats)
async def get_api_usage_overview(
    _: bool = Depends(get_admin_access),
    db: Session = Depends(get_db)
):
    """Obtener overview general del uso de la API"""
    
    try:
        today = date.today()
        month_start = today.replace(day=1)
        
        # Estadísticas generales
        total_clients = db.query(Client).count()
        active_clients = db.query(Client).filter(Client.is_active == True).count()
        
        # Estadísticas generales (manejar casos sin registros)
        total_queries_today_result = db.query(func.sum(ApiUsage.queries_count)).filter(
            ApiUsage.query_date == today
        ).scalar()
        total_queries_today = int(total_queries_today_result) if total_queries_today_result is not None else 0
        
        total_queries_this_month_result = db.query(func.sum(ApiUsage.queries_count)).filter(
            ApiUsage.query_date >= month_start
        ).scalar()
        total_queries_this_month = int(total_queries_this_month_result) if total_queries_this_month_result is not None else 0
        
        # Top 5 clientes por uso este mes
        top_clients_query = db.query(
            Client.client_id,
            Client.client_name,
            Client.plan_type,
            func.sum(ApiUsage.queries_count).label('query_count')
        ).join(
            ApiUsage, Client.client_id == ApiUsage.client_id
        ).filter(
            ApiUsage.query_date >= month_start
        ).group_by(
            Client.client_id, Client.client_name, Client.plan_type
        ).order_by(
            desc('query_count')
        ).limit(5).all()
        
        top_clients = []
        for client_data in top_clients_query:
            # Obtener más detalles para cada top client
            client = db.query(Client).filter(Client.client_id == client_data.client_id).first()
            
            queries_today_result = db.query(func.sum(ApiUsage.queries_count)).filter(
                ApiUsage.client_id == client_data.client_id,
                ApiUsage.query_date == today
            ).scalar()
            queries_today = int(queries_today_result) if queries_today_result is not None else 0
            
            queries_this_week_result = db.query(func.sum(ApiUsage.queries_count)).filter(
                ApiUsage.client_id == client_data.client_id,
                ApiUsage.query_date >= today - timedelta(days=7)
            ).scalar()
            queries_this_week = int(queries_this_week_result) if queries_this_week_result is not None else 0
            
            last_usage = db.query(ApiUsage).filter(
                ApiUsage.client_id == client_data.client_id,
                ApiUsage.queries_count > 0
            ).order_by(desc(ApiUsage.query_date)).first()
            
            quota_utilization = 0.0
            if client and client.monthly_quota > 0:
                quota_utilization = (client_data.query_count / client.monthly_quota) * 100
            
            top_clients.append(UsageStats(
                client_id=client_data.client_id,
                client_name=client_data.client_name,
                plan_type=client_data.plan_type,
                queries_today=queries_today,
                queries_this_week=queries_this_week,
                queries_this_month=client_data.query_count,
                last_query=last_usage.query_date if last_usage else None,
                quota_utilization=min(quota_utilization, 100.0)
            ))
        
        # Actividades recientes (últimas 10)
        recent_activities_query = db.query(
            ApiUsage.query_date,
            ApiUsage.client_id,
            Client.client_name,
            ApiUsage.queries_count
        ).join(
            Client, ApiUsage.client_id == Client.client_id
        ).filter(
            ApiUsage.query_date >= today - timedelta(days=7),
            ApiUsage.queries_count > 0
        ).order_by(
            desc(ApiUsage.query_date)
        ).limit(10).all()
        
        recent_activities = [
            ApiUsageDetail(
                date=activity.query_date,
                client_id=activity.client_id,
                client_name=activity.client_name,
                query_count=activity.queries_count,
                endpoint="/search"  # Por ahora fijo, después puedes agregar el campo endpoint
            ) for activity in recent_activities_query
        ]
        
        return AdminStats(
            total_clients=total_clients,
            active_clients=active_clients,
            total_queries_today=total_queries_today,
            total_queries_this_month=total_queries_this_month,
            top_clients=top_clients,
            recent_activities=recent_activities
        )
        
    except Exception as e:
        logger.error(f"Error obteniendo overview de API: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo datos de la API")

# Endpoints adicionales útiles
@admin_router.put("/clients/{client_id}/toggle")
async def toggle_client_status(
    client_id: str,
    _: bool = Depends(get_admin_access),
    db: Session = Depends(get_db)
):
    """Activar/desactivar un cliente"""
    
    try:
        client = db.query(Client).filter(Client.client_id == client_id).first()
        
        if not client:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        client.is_active = not client.is_active
        db.commit()
        
        return {
            "client_id": client_id,
            "is_active": client.is_active,
            "message": f"Cliente {'activado' if client.is_active else 'desactivado'}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cambiando estado del cliente: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Error cambiando estado del cliente")

@admin_router.post("/clients/{client_id}/regenerate-key")
async def regenerate_api_key(
    client_id: str,
    _: bool = Depends(get_admin_access),
    db: Session = Depends(get_db)
):
    """Regenerar API Key de un cliente"""
    
    try:
        client = db.query(Client).filter(Client.client_id == client_id).first()
        
        if not client:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        # Generar nueva API key
        old_key = client.api_key
        client.api_key = str(uuid.uuid4())
        
        db.commit()
        
        return {
            "client_id": client_id,
            "old_api_key": old_key,
            "new_api_key": client.api_key,
            "message": "API Key regenerada exitosamente"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error regenerando API key: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Error regenerando API key")