# app/models/database.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
import os
from dotenv import load_dotenv

load_dotenv()

# Configuración de la base de datos
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./sanctions.db")

# Para producción con PostgreSQL:
# DATABASE_URL = "postgresql://user:password@localhost/sanctions_db"

# Configuración específica para SQLite vs PostgreSQL
if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(
        DATABASE_URL,
        echo=False,  # Cambiar a True para ver las consultas SQL
        connect_args={"check_same_thread": False},  # Para SQLite
        poolclass=StaticPool,
    )
else:
    engine = create_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=300,
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependency para obtener la sesión de BD
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Función para crear las tablas
def create_tables():
    from app.models.entities import Base
    Base.metadata.create_all(bind=engine)
    print("✅ Tablas creadas exitosamente")

# Función para resetear la BD (solo para desarrollo)
def reset_database():
    from app.models.entities import Base
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    print("🔄 Base de datos reseteada")