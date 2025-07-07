# setup_database.py
"""
Script para configurar la base de datos inicial
Ejecutar: python setup_database.py
"""

import sys
import os
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent))

from app.models.database import create_tables, reset_database, engine
from app.models.entities import *
from app.core.config import settings
from sqlalchemy.orm import sessionmaker
import hashlib
import uuid

def create_sample_client():
    """Crear un cliente de ejemplo para pruebas"""
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Verificar si ya existe un cliente de prueba
        existing_client = session.query(Client).filter_by(client_id="test_client").first()
        if existing_client:
            print("✅ Cliente de prueba ya existe")
            return existing_client.api_key
        
        # Crear cliente de prueba
        api_key = str(uuid.uuid4())
        test_client = Client(
            client_id="test_client",
            api_key=api_key,
            client_name="Cliente de Prueba",
            email="test@example.com",
            plan_type="starter",
            monthly_quota=100,
            is_active=True
        )
        
        session.add(test_client)
        session.commit()
        
        print(f"✅ Cliente de prueba creado")
        print(f"   Client ID: test_client")
        print(f"   API Key: {api_key}")
        print(f"   Plan: starter (100 consultas/mes)")
        
        return api_key
        
    except Exception as e:
        print(f"❌ Error creando cliente de prueba: {e}")
        session.rollback()
        return None
    finally:
        session.close()

def insert_sample_data():
    """Insertar datos de ejemplo para pruebas"""
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Verificar si ya hay datos
        existing_count = session.query(Entity).count()
        if existing_count > 0:
            print(f"✅ Ya existen {existing_count} entidades en la base de datos")
            return
        
        # Crear entidad de ejemplo
        sample_entity = Entity(
            source="OFAC",
            source_id="12345",
            name="EJEMPLO SANCIONADO",
            type="INDIVIDUAL",
            gender="Male",
            status="ACTIVE",
            hash_signature=hashlib.md5("test_data".encode()).hexdigest()
        )
        
        session.add(sample_entity)
        session.flush()  # Para obtener el ID
        
        # Agregar alias
        sample_alias = Alias(
            entity_id=sample_entity.id,
            alias_name="ALIAS DE EJEMPLO",
            quality="STRONG"
        )
        session.add(sample_alias)
        
        # Agregar dirección
        sample_address = Address(
            entity_id=sample_entity.id,
            full_address="123 Main St, Example City",
            city="Example City",
            country="US",
            postal_code="12345"
        )
        session.add(sample_address)
        
        # Agregar sanción
        sample_sanction = Sanction(
            entity_id=sample_entity.id,
            program="IRAN",
            authority="OFAC",
            comments="Entidad de ejemplo para pruebas"
        )
        session.add(sample_sanction)
        
        session.commit()
        print("✅ Datos de ejemplo insertados")
        
    except Exception as e:
        print(f"❌ Error insertando datos de ejemplo: {e}")
        session.rollback()
    finally:
        session.close()

def main():
    """Función principal de configuración"""
    print("🚀 Configurando base de datos para Sanctions API")
    print("=" * 50)
    
    # Verificar configuración
    print(f"📍 Base de datos: {settings.database_url}")
    
    try:
        # Crear tablas
        print("\n1. Creando tablas...")
        create_tables()
        
        # Crear cliente de prueba
        print("\n2. Creando cliente de prueba...")
        api_key = create_sample_client()
        
        # Insertar datos de ejemplo
        print("\n3. Insertando datos de ejemplo...")
        insert_sample_data()
        
        print("\n" + "=" * 50)
        print("✅ CONFIGURACIÓN COMPLETADA")
        print("=" * 50)
        
        if api_key:
            print(f"\n📝 Para probar tu API, usa:")
            print(f"   curl -H 'X-API-Key: {api_key}' http://localhost:8000/api/v1/search?q=ejemplo")
        
        print(f"\n🔧 Para iniciar el servidor:")
        print(f"   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000")
        
    except Exception as e:
        print(f"❌ Error en la configuración: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()