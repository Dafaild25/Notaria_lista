# app/main.py
from fastapi import FastAPI, HTTPException, Depends, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import Optional, List
import logging
from datetime import datetime, date
from sqlalchemy import func

from app.models.database import get_db
from app.core.config import settings
from app.api.routes import router as api_router
from app.api.etl_routes import etl_router
from app.api.admin_routes import admin_router  # NUEVO: Importar rutas de admin
from app.models.entities import ApiUsage, Client

# Configurar logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Crear la aplicación FastAPI
app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="API para consulta de listas de sanciones OFAC y ONU",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar dominios exactos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# NUEVO: Middleware para rastrear uso de la API
@app.middleware("http")
async def track_api_usage(request, call_next):
    """Middleware para registrar uso de la API"""
    
    # Procesar la request
    response = await call_next(request)
    
    # Solo rastrear endpoints de búsqueda exitosos
    if (request.url.path.startswith(f"{settings.api_v1_prefix}/search") and 
        response.status_code == 200 and
        request.method == "GET"):
        
        try:
            # Obtener API Key
            api_key = request.headers.get("X-API-Key")
            
            if api_key:
                # Crear nueva sesión de DB para el middleware
                from app.models.database import SessionLocal
                db = SessionLocal()
                
                try:
                    # Buscar cliente
                    client = db.query(Client).filter(
                        Client.api_key == api_key,
                        Client.is_active == True
                    ).first()
                    
                    if client:
                        today = date.today()
                        
                        # Buscar registro existente para hoy
                        usage_record = db.query(ApiUsage).filter(
                            ApiUsage.client_id == client.client_id,
                            ApiUsage.query_date == today
                        ).first()
                        
                        if usage_record:
                            # Incrementar contador existente
                            usage_record.queries_count += 1
                        else:
                            # Crear nuevo registro para hoy
                            usage_record = ApiUsage(
                                client_id=client.client_id,
                                query_date=today,
                                queries_count=1,
                                plan_type=client.plan_type,
                                endpoint=str(request.url.path)
                            )
                            db.add(usage_record)
                        
                        db.commit()
                        logger.info(f"Uso registrado para cliente: {client.client_id} (total hoy: {usage_record.queries_count})")
                        
                except Exception as e:
                    logger.error(f"Error registrando uso: {e}")
                    db.rollback()
                finally:
                    db.close()
                    
        except Exception as e:
            logger.error(f"Error en middleware de tracking: {e}")
    
    return response

# Middleware para logging de requests (mantener el existente)
@app.middleware("http")
async def log_requests(request, call_next):
    start_time = datetime.now()
    response = await call_next(request)
    process_time = (datetime.now() - start_time).total_seconds()
    
    logger.info(
        f"{request.method} {request.url.path} - "
        f"Status: {response.status_code} - "
        f"Time: {process_time:.3f}s"
    )
    return response

# Función para validar API Key (mantener la existente)
async def validate_api_key(
    x_api_key: str = Header(..., description="API Key para autenticación"),
    db: Session = Depends(get_db)
) -> Client:
    """Validar API Key y verificar límites de suscripción"""
    
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
    
    # Verificar límites de uso mensual
    if client.monthly_quota > 0:  # -1 significa ilimitado
        today = date.today()
        month_start = today.replace(day=1)
        
        # Contar uso del mes actual
        monthly_usage = db.query(func.sum(ApiUsage.queries_count)).filter(
            ApiUsage.client_id == client.client_id,
            ApiUsage.query_date >= month_start
        ).scalar() or 0
        
        if monthly_usage >= client.monthly_quota:
            raise HTTPException(
                status_code=429,
                detail=f"Límite mensual alcanzado ({client.monthly_quota} consultas)"
            )
    
    return client

# Incluir rutas de la API (mantener las existentes)
app.include_router(
    api_router,
    prefix=settings.api_v1_prefix,
    dependencies=[Depends(validate_api_key)]
)

# Incluir rutas de ETL (mantener existentes)
app.include_router(
    etl_router,
    prefix=f"{settings.api_v1_prefix}/etl",
    tags=["ETL Administration"],
    dependencies=[Depends(validate_api_key)]
)

# NUEVO: Incluir rutas de administración (SIN validación de API Key normal)
app.include_router(
    admin_router,
    prefix=settings.api_v1_prefix,
    tags=["Administration"]
)

# Rutas básicas (mantener las existentes)
@app.get("/")
async def root():
    return {
        "message": "Sanctions API",
        "version": settings.version,
        "docs": "/docs",
        "status": "active"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": settings.version
    }

@app.get("/info")
async def api_info():
    return {
        "name": settings.app_name,
        "version": settings.version,
        "description": "API para consulta de listas de sanciones internacionales",
        "sources": ["OFAC", "UN"],
        "endpoints": {
            "search": f"{settings.api_v1_prefix}/search",
            "entity": f"{settings.api_v1_prefix}/entity/{{id}}",
            "stats": f"{settings.api_v1_prefix}/stats",
            "admin": f"{settings.api_v1_prefix}/admin"  # NUEVO
        },
        "authentication": "API Key required in X-API-Key header (X-Admin-Key for admin endpoints)"
    }

# Manejo de errores (mantener existentes)
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"detail": "Recurso no encontrado"}
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error(f"Error interno: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Error interno del servidor"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )