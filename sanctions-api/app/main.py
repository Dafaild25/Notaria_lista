# app/main.py
from fastapi import FastAPI, HTTPException, Depends, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import Optional, List
import logging
from datetime import datetime, date

from app.models.database import get_db
from app.core.config import settings
from app.api.routes import router as api_router
from app.api.etl_routes import etl_router
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

# Middleware para logging de requests
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

# Función para validar API Key
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
        monthly_usage = db.query(ApiUsage).filter(
            ApiUsage.client_id == client.client_id,
            ApiUsage.query_date >= month_start
        ).count()
        
        if monthly_usage >= client.monthly_quota:
            raise HTTPException(
                status_code=429,
                detail=f"Límite mensual alcanzado ({client.monthly_quota} consultas)"
            )
    
    return client

# Incluir rutas de la API
app.include_router(
    api_router,
    prefix=settings.api_v1_prefix,
    dependencies=[Depends(validate_api_key)]
)

# Incluir rutas de ETL (solo para admins)
app.include_router(
    etl_router,
    prefix=f"{settings.api_v1_prefix}/etl",
    tags=["ETL Administration"],
    dependencies=[Depends(validate_api_key)]
)

# Ruta raíz
@app.get("/")
async def root():
    return {
        "message": "Sanctions API",
        "version": settings.version,
        "docs": "/docs",
        "status": "active"
    }

# Ruta de health check
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": settings.version
    }

# Ruta para información de la API
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
            "stats": f"{settings.api_v1_prefix}/stats"
        },
        "authentication": "API Key required in X-API-Key header"
    }

# Manejo de errores
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