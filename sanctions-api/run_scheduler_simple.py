# run_scheduler_simple.py - Scheduler simplificado sin async
"""
Scheduler simplificado para ETL sin problemas de async
Uso: python run_scheduler_simple.py
"""

import sys
import signal
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent))

from app.etl.ofac_parser_final import run_ofac_update
from app.etl.un_parser_final import run_un_update
from app.models.database import SessionLocal
from app.models.entities import UpdateLog
from sqlalchemy import text

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SimpleETLScheduler:
    """Scheduler simplificado para ETL"""
    
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.update_history = []
        
    def send_notification(self, subject: str, body: str, is_error: bool = False):
        """Enviar notificación simple por log"""
        level = logging.ERROR if is_error else logging.INFO
        logger.log(level, f"NOTIFICACIÓN: {subject}")
        logger.log(level, body)
    
    def run_ofac_update_job(self):
        """Job para actualizar datos de OFAC"""
        logger.info("🔄 Iniciando actualización programada de OFAC")
        
        try:
            start_time = datetime.utcnow()
            result = run_ofac_update()
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            # Registrar resultado
            self.update_history.append({
                'source': 'OFAC',
                'timestamp': datetime.utcnow(),
                'result': result,
                'success': result.get('status') == 'success',
                'duration': duration
            })
            
            # Notificar según resultado
            if result.get('status') == 'success':
                stats = result.get('stats', {})
                message = f"""
✅ Actualización OFAC exitosa
- Entidades agregadas: {stats.get('entities_added', 0)}
- Entidades actualizadas: {stats.get('entities_updated', 0)}
- Aliases agregados: {stats.get('aliases_added', 0)}
- Direcciones agregadas: {stats.get('addresses_added', 0)}
- Documentos agregados: {stats.get('documents_added', 0)}
- Duración: {duration:.2f} segundos
"""
                self.send_notification("Actualización OFAC exitosa", message)
                
            elif result.get('status') == 'no_changes':
                logger.info("ℹ️ No hay cambios en datos de OFAC")
                
            else:
                error_msg = f"""
❌ Error en actualización OFAC
- Error: {result.get('error', 'Error desconocido')}
- Duración: {duration:.2f} segundos
"""
                self.send_notification("Error en actualización OFAC", error_msg, is_error=True)
            
            logger.info(f"Actualización OFAC completada: {result.get('status')}")
            
        except Exception as e:
            logger.error(f"Error crítico en job de OFAC: {e}")
            self.send_notification(
                "Error crítico en actualización OFAC",
                f"Error crítico: {str(e)}",
                is_error=True
            )
    
    def run_un_update_job(self):
        """Job para actualizar datos de ONU"""
        logger.info("🔄 Iniciando actualización programada de ONU")
        
        try:
            start_time = datetime.utcnow()
            result = run_un_update()
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            # Registrar resultado
            self.update_history.append({
                'source': 'UN',
                'timestamp': datetime.utcnow(),
                'result': result,
                'success': result.get('status') == 'success',
                'duration': duration
            })
            
            # Notificar según resultado
            if result.get('status') == 'success':
                stats = result.get('stats', {})
                message = f"""
✅ Actualización ONU exitosa
- Entidades agregadas: {stats.get('entities_added', 0)}
- Entidades actualizadas: {stats.get('entities_updated', 0)}
- Aliases agregados: {stats.get('aliases_added', 0)}
- Direcciones agregadas: {stats.get('addresses_added', 0)}
- Documentos agregados: {stats.get('documents_added', 0)}
- Duración: {duration:.2f} segundos
"""
                self.send_notification("Actualización ONU exitosa", message)
                
            elif result.get('status') == 'no_changes':
                logger.info("ℹ️ No hay cambios en datos de ONU")
                
            else:
                error_msg = f"""
❌ Error en actualización ONU
- Error: {result.get('error', 'Error desconocido')}
- Duración: {duration:.2f} segundos
"""
                self.send_notification("Error en actualización ONU", error_msg, is_error=True)
            
            logger.info(f"Actualización ONU completada: {result.get('status')}")
            
        except Exception as e:
            logger.error(f"Error crítico en job de ONU: {e}")
            self.send_notification(
                "Error crítico en actualización ONU",
                f"Error crítico: {str(e)}",
                is_error=True
            )
    
    def health_check_job(self):
        """Job para verificar salud del sistema"""
        logger.info("🏥 Ejecutando health check")
        
        try:
            db = SessionLocal()
            
            # Verificar conexión a BD
            db.execute(text("SELECT 1"))
            
            # Verificar actualizaciones recientes
            recent_updates = db.query(UpdateLog).filter(
                UpdateLog.update_date >= datetime.utcnow() - timedelta(days=7)
            ).count()
            
            if recent_updates == 0:
                logger.warning("⚠️ No hay actualizaciones en los últimos 7 días")
                self.send_notification(
                    "Sin actualizaciones recientes",
                    "No se han registrado actualizaciones en los últimos 7 días.",
                    is_error=True
                )
            else:
                logger.info(f"✅ Health check OK. Actualizaciones recientes: {recent_updates}")
            
            db.close()
            
        except Exception as e:
            logger.error(f"❌ Error en health check: {e}")
            self.send_notification(
                "Error en health check",
                f"Error verificando salud del sistema: {str(e)}",
                is_error=True
            )
    
    def setup_jobs(self):
        """Configurar jobs programados"""
        try:
            # Job para OFAC - Diario a las 8:00 AM UTC
            self.scheduler.add_job(
                self.run_ofac_update_job,
                trigger=CronTrigger(hour=8, minute=0),
                id='ofac_update',
                name='Actualización OFAC',
                replace_existing=True
            )
            
            # Job para ONU - Lunes a las 9:00 AM UTC
            self.scheduler.add_job(
                self.run_un_update_job,
                trigger=CronTrigger(day_of_week='mon', hour=9, minute=0),
                id='un_update',
                name='Actualización ONU',
                replace_existing=True
            )
            
            # Health check - Cada 6 horas
            self.scheduler.add_job(
                self.health_check_job,
                trigger=IntervalTrigger(hours=6),
                id='health_check',
                name='Health Check',
                replace_existing=True
            )
            
            logger.info("✅ Jobs programados configurados exitosamente")
            
        except Exception as e:
            logger.error(f"❌ Error configurando jobs: {e}")
            raise
    
    def start(self):
        """Iniciar scheduler"""
        try:
            logger.info("🚀 Configurando scheduler...")
            self.setup_jobs()
            
            # Mostrar jobs configurados
            jobs = self.scheduler.get_jobs()
            logger.info(f"📋 Jobs configurados: {len(jobs)}")
            for job in jobs:
                logger.info(f"   - {job.name} (ID: {job.id})")
            
            logger.info("🎯 Iniciando scheduler...")
            logger.info("⏰ El scheduler ahora está corriendo y ejecutará los jobs según programación")
            
            # Ejecutar health check inicial
            logger.info("🏥 Ejecutando health check inicial...")
            self.health_check_job()
            
            self.scheduler.start()
            
        except Exception as e:
            logger.error(f"❌ Error iniciando scheduler: {e}")
            raise
    
    def stop(self):
        """Detener scheduler"""
        try:
            logger.info("🛑 Deteniendo scheduler...")
            self.scheduler.shutdown()
            logger.info("✅ Scheduler detenido")
        except Exception as e:
            logger.error(f"❌ Error deteniendo scheduler: {e}")

# Crear instancia del scheduler
scheduler = SimpleETLScheduler()

def signal_handler(sig, frame):
    """Manejar señales de terminación"""
    logger.info(f"Recibida señal {sig}. Terminando scheduler...")
    scheduler.stop()
    sys.exit(0)

def main():
    """Función principal"""
    print("=" * 60)
    print("   🚀 SANCTIONS API - ETL SCHEDULER")
    print("=" * 60)
    print()
    
    # Configurar manejadores de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        print("✅ Iniciando scheduler ETL...")
        print()
        print("📅 Programación configurada:")
        print("   • OFAC: Diario a las 8:00 AM UTC")
        print("   • ONU: Lunes a las 9:00 AM UTC")  
        print("   • Health Check: Cada 6 horas")
        print()
        print("🔧 Para detener el scheduler, presiona Ctrl+C")
        print("📊 Logs disponibles en: logs/scheduler.log")
        print("=" * 60)
        
        # Iniciar scheduler (esto bloquea hasta que se detenga)
        scheduler.start()
        
    except KeyboardInterrupt:
        logger.info("Terminando scheduler por interrupción del usuario...")
        return 0
        
    except Exception as e:
        logger.error(f"Error en scheduler: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())