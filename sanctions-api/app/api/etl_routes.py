# app/api/etl_routes.py
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Header
from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import logging

from app.models.database import get_db
from app.models.entities import UpdateLog, Entity, Client
from app.etl.scheduler import etl_scheduler, run_manual_update
from app.schemas.entities import ClientInfo
from pydantic import BaseModel

# Configurar logging
logger = logging.getLogger(__name__)

# Crear router para ETL
etl_router = APIRouter()

# Schemas para ETL
class UpdateLogSchema(BaseModel):
    id: int
    source: str
    update_date: datetime
    records_added: int
    records_updated: int
    records_deleted: int
    status: str
    error_message: Optional[str] = None
    
    class Config:
        from_attributes = True

class ManualUpdateRequest(BaseModel):
    source: str  # 'OFAC' o 'UN'
    
class ManualUpdateResponse(BaseModel):
    status: str
    message: str
    task_id: Optional[str] = None
    stats: Optional[Dict] = None

class SchedulerStatusResponse(BaseModel):
    scheduler_running: bool
    jobs: List[Dict]
    recent_updates: List[Dict]
    last_update_ofac: Optional[datetime] = None
    last_update_un: Optional[datetime] = None

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

# Middleware para verificar permisos de admin
async def verify_admin_permissions(
    client: Client = Depends(get_authenticated_client),
    db: Session = Depends(get_db)
):
    """Verificar que el cliente tenga permisos de admin"""
    # Por ahora, solo el plan 'unlimited' tiene permisos de admin
    # En producción, implementar tabla de permisos más granular
    if client.plan_type != 'unlimited':
        raise HTTPException(
            status_code=403,
            detail="Se requieren permisos de administrador para acceder a esta función"
        )
    return client

@etl_router.get("/logs", response_model=List[UpdateLogSchema])
async def get_update_logs(
    limit: int = 50,
    source: Optional[str] = None,
    admin_client: Client = Depends(verify_admin_permissions),
    db: Session = Depends(get_db)
):
    """Obtener logs de actualizaciones ETL"""
    
    try:
        query = db.query(UpdateLog)
        
        if source:
            query = query.filter(UpdateLog.source == source.upper())
        
        logs = query.order_by(desc(UpdateLog.update_date)).limit(limit).all()
        
        return logs
        
    except Exception as e:
        logger.error(f"Error obteniendo logs: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo logs de actualización")

@etl_router.get("/status", response_model=SchedulerStatusResponse)
async def get_scheduler_status(
    admin_client: Client = Depends(verify_admin_permissions),
    db: Session = Depends(get_db)
):
    """Obtener estado del scheduler y últimas actualizaciones"""
    
    try:
        # Obtener estado del scheduler
        scheduler_status = etl_scheduler.get_job_status()
        
        # Obtener últimas actualizaciones por fuente
        last_ofac = db.query(UpdateLog).filter(
            UpdateLog.source == 'OFAC',
            UpdateLog.status == 'SUCCESS'
        ).order_by(desc(UpdateLog.update_date)).first()
        
        last_un = db.query(UpdateLog).filter(
            UpdateLog.source == 'UN',
            UpdateLog.status == 'SUCCESS'
        ).order_by(desc(UpdateLog.update_date)).first()
        
        return SchedulerStatusResponse(
            scheduler_running=scheduler_status['scheduler_running'],
            jobs=scheduler_status['jobs'],
            recent_updates=scheduler_status['recent_updates'],
            last_update_ofac=last_ofac.update_date if last_ofac else None,
            last_update_un=last_un.update_date if last_un else None
        )
        
    except Exception as e:
        logger.error(f"Error obteniendo estado: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo estado del scheduler")

@etl_router.post("/update", response_model=ManualUpdateResponse)
async def trigger_manual_update(
    request: ManualUpdateRequest,
    background_tasks: BackgroundTasks,
    admin_client: Client = Depends(verify_admin_permissions),
    db: Session = Depends(get_db)
):
    """Ejecutar actualización manual de datos"""
    
    try:
        # Validar fuente
        source = request.source.upper()
        if source not in ['OFAC', 'UN']:
            raise HTTPException(
                status_code=400,
                detail="Fuente debe ser 'OFAC' o 'UN'"
            )
        
        # Verificar si ya hay una actualización en progreso
        recent_update = db.query(UpdateLog).filter(
            UpdateLog.source == source,
            UpdateLog.update_date >= datetime.utcnow() - timedelta(minutes=30)
        ).first()
        
        if recent_update and recent_update.status == 'IN_PROGRESS':
            raise HTTPException(
                status_code=409,
                detail=f"Ya hay una actualización de {source} en progreso"
            )
        
        # Ejecutar actualización en background
        task_id = f"manual_update_{source}_{datetime.utcnow().timestamp()}"
        
        async def run_update():
            try:
                logger.info(f"Iniciando actualización manual de {source}")
                result = await run_manual_update(source)
                logger.info(f"Actualización manual de {source} completada: {result}")
                return result
            except Exception as e:
                logger.error(f"Error en actualización manual de {source}: {e}")
                raise
        
        background_tasks.add_task(run_update)
        
        return ManualUpdateResponse(
            status="started",
            message=f"Actualización de {source} iniciada",
            task_id=task_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error iniciando actualización manual: {e}")
        raise HTTPException(status_code=500, detail="Error iniciando actualización manual")

@etl_router.post("/scheduler/start")
async def start_scheduler(
    admin_client: Client = Depends(verify_admin_permissions)
):
    """Iniciar scheduler automático"""
    
    try:
        if etl_scheduler.is_running:
            return {"status": "already_running", "message": "Scheduler ya está corriendo"}
        
        etl_scheduler.start()
        
        return {"status": "started", "message": "Scheduler iniciado exitosamente"}
        
    except Exception as e:
        logger.error(f"Error iniciando scheduler: {e}")
        raise HTTPException(status_code=500, detail="Error iniciando scheduler")

@etl_router.post("/scheduler/stop")
async def stop_scheduler(
    admin_client: Client = Depends(verify_admin_permissions)
):
    """Detener scheduler automático"""
    
    try:
        if not etl_scheduler.is_running:
            return {"status": "already_stopped", "message": "Scheduler ya está detenido"}
        
        etl_scheduler.stop()
        
        return {"status": "stopped", "message": "Scheduler detenido exitosamente"}
        
    except Exception as e:
        logger.error(f"Error deteniendo scheduler: {e}")
        raise HTTPException(status_code=500, detail="Error deteniendo scheduler")

@etl_router.get("/database/stats")
async def get_database_detailed_stats(
    admin_client: Client = Depends(verify_admin_permissions),
    db: Session = Depends(get_db)
):
    """Obtener estadísticas detalladas de la base de datos"""
    
    try:
        # Estadísticas generales
        total_entities = db.query(Entity).count()
        
        # Por fuente
        ofac_count = db.query(Entity).filter(Entity.source == 'OFAC').count()
        un_count = db.query(Entity).filter(Entity.source == 'UN').count()
        
        # Por tipo
        individual_count = db.query(Entity).filter(Entity.type == 'INDIVIDUAL').count()
        entity_count = db.query(Entity).filter(Entity.type == 'ENTITY').count()
        vessel_count = db.query(Entity).filter(Entity.type == 'VESSEL').count()
        aircraft_count = db.query(Entity).filter(Entity.type == 'AIRCRAFT').count()
        
        # Por estado
        active_count = db.query(Entity).filter(Entity.status == 'ACTIVE').count()
        updated_count = db.query(Entity).filter(Entity.status == 'UPDATED').count()
        delisted_count = db.query(Entity).filter(Entity.status == 'DELISTED').count()
        
        # Últimas actualizaciones
        recent_entities = db.query(Entity).filter(
            Entity.last_updated >= datetime.utcnow() - timedelta(days=7)
        ).count()
        
        # Estadísticas de actualización
        total_updates = db.query(UpdateLog).count()
        successful_updates = db.query(UpdateLog).filter(UpdateLog.status == 'SUCCESS').count()
        failed_updates = db.query(UpdateLog).filter(UpdateLog.status == 'FAILED').count()
        
        return {
            "total_entities": total_entities,
            "by_source": {
                "OFAC": ofac_count,
                "UN": un_count
            },
            "by_type": {
                "INDIVIDUAL": individual_count,
                "ENTITY": entity_count,
                "VESSEL": vessel_count,
                "AIRCRAFT": aircraft_count
            },
            "by_status": {
                "ACTIVE": active_count,
                "UPDATED": updated_count,
                "DELISTED": delisted_count
            },
            "recent_updates": {
                "entities_updated_last_week": recent_entities,
                "total_update_runs": total_updates,
                "successful_updates": successful_updates,
                "failed_updates": failed_updates
            }
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas detalladas: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo estadísticas detalladas")

@etl_router.delete("/database/cleanup")
async def cleanup_old_data(
    days: int = 30,
    admin_client: Client = Depends(verify_admin_permissions),
    db: Session = Depends(get_db)
):
    """Limpiar datos antiguos y logs"""
    
    try:
        if days < 7:
            raise HTTPException(
                status_code=400,
                detail="No se pueden eliminar datos con menos de 7 días de antigüedad"
            )
        
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Eliminar logs antiguos
        old_logs = db.query(UpdateLog).filter(
            UpdateLog.update_date < cutoff_date
        ).delete()
        
        # Eliminar entidades marcadas como DELISTED hace más de X días
        old_delisted = db.query(Entity).filter(
            Entity.status == 'DELISTED',
            Entity.last_updated < cutoff_date
        ).count()
        
        # No eliminar entidades activas, solo logs
        db.commit()
        
        return {
            "status": "completed",
            "deleted_logs": old_logs,
            "old_delisted_entities": old_delisted,
            "cutoff_date": cutoff_date.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error en limpieza: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Error en limpieza de datos")

@etl_router.get("/health")
async def etl_health_check(
    admin_client: Client = Depends(verify_admin_permissions),
    db: Session = Depends(get_db)
):
    """Health check específico para ETL"""
    
    try:
        # Verificar última actualización exitosa
        last_successful_update = db.query(UpdateLog).filter(
            UpdateLog.status == 'SUCCESS'
        ).order_by(desc(UpdateLog.update_date)).first()
        
        # Verificar si hay actualizaciones recientes
        days_since_last_update = None
        if last_successful_update:
            days_since_last_update = (datetime.utcnow() - last_successful_update.update_date).days
        
        # Verificar estado del scheduler
        scheduler_status = etl_scheduler.get_job_status()
        
        # Determinar estado general
        health_status = "healthy"
        warnings = []
        
        if days_since_last_update and days_since_last_update > 7:
            health_status = "warning"
            warnings.append(f"Última actualización exitosa hace {days_since_last_update} días")
        
        if not scheduler_status['scheduler_running']:
            health_status = "warning"
            warnings.append("Scheduler no está corriendo")
        
        # Verificar errores recientes
        recent_errors = db.query(UpdateLog).filter(
            UpdateLog.status == 'FAILED',
            UpdateLog.update_date >= datetime.utcnow() - timedelta(days=1)
        ).count()
        
        if recent_errors > 0:
            health_status = "warning"
            warnings.append(f"{recent_errors} errores en las últimas 24 horas")
        
        return {
            "status": health_status,
            "scheduler_running": scheduler_status['scheduler_running'],
            "last_successful_update": last_successful_update.update_date if last_successful_update else None,
            "days_since_last_update": days_since_last_update,
            "recent_errors": recent_errors,
            "warnings": warnings,
            "active_jobs": len(scheduler_status['jobs']),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error en health check ETL: {e}")
        raise HTTPException(status_code=500, detail="Error en health check ETL")