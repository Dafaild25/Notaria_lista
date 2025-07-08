# app/api/etl_routes.py
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Dict, Any
import logging
from datetime import datetime

from app.models.database import get_db
from app.core.config import settings

# Imports corregidos para tus archivos ETL existentes
try:
    from app.etl.ofac.parser_final import OFACParser
    OFAC_AVAILABLE = True
except ImportError:
    OFAC_AVAILABLE = False
    logging.warning("OFAC parser no disponible")

try:
    from app.etl.un_parser_final import UNParser  # Ajustar si está en otra ubicación
    UN_AVAILABLE = True
except ImportError:
    UN_AVAILABLE = False
    logging.warning("UN parser no disponible")

# Si tienes un scheduler en la raíz
try:
    import sys
    import os
    # Agregar el directorio raíz al path para importar etl_cli
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    from etl_cli import run_ofac_update, run_un_update
    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False
    logging.warning("ETL CLI no disponible")

# Configurar logging
logger = logging.getLogger(__name__)

# Crear router para ETL
etl_router = APIRouter()

# Función para validar acceso admin (simplificada)
async def validate_admin_access(
    db: Session = Depends(get_db)
):
    """Validar acceso de administrador para ETL"""
    # Por ahora permitir acceso, después agregar validación
    return True

@etl_router.post("/run-ofac")
async def run_ofac_extraction(
    background_tasks: BackgroundTasks,
    _: bool = Depends(validate_admin_access)
):
    """Ejecutar extracción de datos OFAC"""
    
    try:
        if CLI_AVAILABLE:
            # Usar el CLI existente en background
            background_tasks.add_task(run_ofac_update)
            return {
                "status": "started",
                "message": "Extracción OFAC iniciada en background",
                "timestamp": datetime.now().isoformat()
            }
        elif OFAC_AVAILABLE:
            # Usar el parser directamente
            parser = OFACParser()
            background_tasks.add_task(parser.run_full_update)
            return {
                "status": "started", 
                "message": "Extracción OFAC iniciada con parser",
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(
                status_code=503,
                detail="Parser OFAC no disponible"
            )
            
    except Exception as e:
        logger.error(f"Error iniciando extracción OFAC: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando extracción: {str(e)}"
        )

@etl_router.post("/run-un")
async def run_un_extraction(
    background_tasks: BackgroundTasks,
    _: bool = Depends(validate_admin_access)
):
    """Ejecutar extracción de datos UN"""
    
    try:
        if CLI_AVAILABLE:
            # Usar el CLI existente
            background_tasks.add_task(run_un_update)
            return {
                "status": "started",
                "message": "Extracción UN iniciada en background", 
                "timestamp": datetime.now().isoformat()
            }
        elif UN_AVAILABLE:
            # Usar el parser directamente
            parser = UNParser()
            background_tasks.add_task(parser.run_full_update)
            return {
                "status": "started",
                "message": "Extracción UN iniciada con parser",
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(
                status_code=503,
                detail="Parser UN no disponible"
            )
            
    except Exception as e:
        logger.error(f"Error iniciando extracción UN: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando extracción: {str(e)}"
        )

@etl_router.post("/run-full")
async def run_full_extraction(
    background_tasks: BackgroundTasks,
    _: bool = Depends(validate_admin_access)
):
    """Ejecutar extracción completa (OFAC + UN)"""
    
    try:
        results = []
        
        # OFAC
        if CLI_AVAILABLE or OFAC_AVAILABLE:
            if CLI_AVAILABLE:
                background_tasks.add_task(run_ofac_update)
            else:
                parser = OFACParser()
                background_tasks.add_task(parser.run_full_update)
            results.append("OFAC iniciado")
        
        # UN
        if CLI_AVAILABLE or UN_AVAILABLE:
            if CLI_AVAILABLE:
                background_tasks.add_task(run_un_update)
            else:
                parser = UNParser()
                background_tasks.add_task(parser.run_full_update)
            results.append("UN iniciado")
        
        if not results:
            raise HTTPException(
                status_code=503,
                detail="No hay parsers disponibles"
            )
        
        return {
            "status": "started",
            "message": f"Extracción completa iniciada: {', '.join(results)}",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error iniciando extracción completa: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando extracción: {str(e)}"
        )

@etl_router.get("/status")
async def get_etl_status():
    """Obtener estado de los módulos ETL"""
    
    return {
        "modules": {
            "ofac_parser": OFAC_AVAILABLE,
            "un_parser": UN_AVAILABLE,
            "etl_cli": CLI_AVAILABLE
        },
        "available_operations": [
            "run-ofac" if (CLI_AVAILABLE or OFAC_AVAILABLE) else None,
            "run-un" if (CLI_AVAILABLE or UN_AVAILABLE) else None,
            "run-full" if (CLI_AVAILABLE or OFAC_AVAILABLE or UN_AVAILABLE) else None
        ],
        "timestamp": datetime.now().isoformat()
    }

@etl_router.get("/logs")
async def get_etl_logs():
    """Obtener logs recientes de ETL"""
    
    # Placeholder - implementar lectura de logs si es necesario
    return {
        "message": "Feature no implementado aún",
        "suggestion": "Revisar logs del servidor o archivos de log"
    }