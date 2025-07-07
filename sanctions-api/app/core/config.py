# app/core/config.py
from pydantic_settings import BaseSettings
from typing import Dict, Any
import os

class Settings(BaseSettings):
    # Configuración de la aplicación
    app_name: str = "Sanctions API"
    version: str = "1.0.0"
    debug: bool = True
    
    # Base de datos - SQLite por defecto para desarrollo
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./sanctions.db")
    
    # API Configuration
    api_v1_prefix: str = "/api/v1"
    
    # Planes de suscripción
    subscription_plans: Dict[str, Dict[str, Any]] = {
        "starter": {
            "queries_per_month": 100,
            "price": 29,
            "description": "Para pequeños negocios"
        },
        "business": {
            "queries_per_month": 1000,
            "price": 99,
            "description": "Para empresas medianas"
        },
        "enterprise": {
            "queries_per_month": 10000,
            "price": 299,
            "description": "Para grandes corporaciones"
        },
        "unlimited": {
            "queries_per_month": -1,
            "price": 599,
            "description": "Sin límites"
        }
    }
    
    # URLs de las fuentes de datos
    ofac_sdn_url: str = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"
    ofac_consolidated_url: str = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/CONSOLIDATED.XML"
    un_consolidated_url: str = "https://scsanctions.un.org/resources/xml/sp/consolidated.xml"
    
    # Configuración de actualización
    update_schedule_ofac: str = "0 8 * * *"  # Diario a las 8 AM
    update_schedule_un: str = "0 9 * * 1"    # Lunes a las 9 AM
    
    # Configuración de logs
    log_level: str = "INFO"
    log_file: str = "logs/sanctions_api.log"
    
    # Configuración de rate limiting
    rate_limit_per_minute: int = 60
    
    # Configuración de búsqueda
    search_similarity_threshold: float = 0.7
    max_search_results: int = 100
    
    class Config:
        env_file = ".env"
        extra = "ignore"  # Ignorar campos extra del .env

# Instancia global de configuración
settings = Settings()

# Constantes para tipos de entidades
ENTITY_TYPES = {
    "INDIVIDUAL": "Persona física",
    "ENTITY": "Entidad/Organización",
    "VESSEL": "Embarcación",
    "AIRCRAFT": "Aeronave"
}

# Constantes para fuentes de datos
DATA_SOURCES = {
    "OFAC": "Office of Foreign Assets Control (US)",
    "UN": "United Nations Security Council"
}

# Constantes para estados
ENTITY_STATUS = {
    "ACTIVE": "Activo en lista",
    "DELISTED": "Removido de lista",
    "UPDATED": "Actualizado recientemente"
}

# Constantes para calidad de aliases
ALIAS_QUALITY = {
    "STRONG": "Coincidencia fuerte",
    "WEAK": "Coincidencia débil", 
    "GOOD": "Buena coincidencia",
    "UNKNOWN": "Calidad desconocida",
    "AKA": "También conocido como",
    "STRONG_AKA": "También conocido como (fuerte)",
    "LOW": "Baja calidad",
    "HIGH": "Alta calidad",
    "MEDIUM": "Calidad media"
}