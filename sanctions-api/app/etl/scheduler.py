# app/etl/scheduler.py
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from app.etl.ofac_parser import OFACParser, run_ofac_update
from app.etl.un_parser import UNParser, run_un_update
from app.core.config import settings
from app.models.database import SessionLocal
from app.models.entities import UpdateLog

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLScheduler:
    """Scheduler para ejecutar actualizaciones automáticas de datos"""
    
    def __init__(self):
        # Configurar scheduler
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': ThreadPoolExecutor(max_workers=2)
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 1,
            'misfire_grace_time': 300  # 5 minutos
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
        
        self.is_running = False
        self.update_history = []
    
    def send_notification(self, subject: str, body: str, is_error: bool = False):
        """Enviar notificación por email (opcional)"""
        try:
            # Configurar SMTP (opcional)
            smtp_server = getattr(settings, 'smtp_server', None)
            smtp_port = getattr(settings, 'smtp_port', 587)
            smtp_user = getattr(settings, 'smtp_user', None)
            smtp_password = getattr(settings, 'smtp_password', None)
            
            if not all([smtp_server, smtp_user, smtp_password]):
                logger.info("SMTP no configurado, saltando notificación por email")
                return
            
            # Crear mensaje
            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = smtp_user  # Enviar a uno mismo
            msg['Subject'] = f"[Sanctions API] {subject}"
            
            # Agregar timestamp
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            full_body = f"Timestamp: {timestamp}\n\n{body}"
            
            msg.attach(MIMEText(full_body, 'plain'))
            
            # Enviar email
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Notificación enviada: {subject}")
            
        except Exception as e:
            logger.error(f"Error enviando notificación: {e}")
    
    async def run_ofac_update_job(self):
        """Job para actualizar datos de OFAC"""
        logger.info("Iniciando actualización programada de OFAC")
        
        try:
            # Ejecutar actualización
            result = await asyncio.get_event_loop().run_in_executor(
                None, run_ofac_update
            )
            
            # Registrar resultado
            self.update_history.append({
                'source': 'OFAC',
                'timestamp': datetime.utcnow(),
                'result': result,
                'success': result.get('status') == 'success'
            })
            
            # Notificar según resultado
            if result.get('status') == 'success':
                stats = result.get('stats', {})
                subject = "✅ Actualización OFAC exitosa"
                body = f"""
Actualización de OFAC completada exitosamente.

Estadísticas:
- Entidades agregadas: {stats.get('entities_added', 0)}
- Entidades actualizadas: {stats.get('entities_updated', 0)}
- Aliases agregados: {stats.get('aliases_added', 0)}
- Direcciones agregadas: {stats.get('addresses_added', 0)}
- Documentos agregados: {stats.get('documents_added', 0)}
- Duración: {result.get('duration', 0):.2f} segundos

Estado: {result.get('status', 'unknown')}
"""
                self.send_notification(subject, body)
                
            elif result.get('status') == 'no_changes':
                logger.info("No hay cambios en datos de OFAC")
                
            else:
                subject = "❌ Error en actualización OFAC"
                body = f"""
Error en la actualización de OFAC:

Error: {result.get('error', 'Error desconocido')}
Duración: {result.get('duration', 0):.2f} segundos

Revisar logs para más detalles.
"""
                self.send_notification(subject, body, is_error=True)
            
            logger.info(f"Actualización OFAC completada: {result.get('status')}")
            
        except Exception as e:
            logger.error(f"Error en job de OFAC: {e}")
            self.send_notification(
                "❌ Error crítico en actualización OFAC",
                f"Error crítico: {str(e)}",
                is_error=True
            )
    
    async def run_un_update_job(self):
        """Job para actualizar datos de ONU"""
        logger.info("Iniciando actualización programada de ONU")
        
        try:
            # Ejecutar actualización
            result = await asyncio.get_event_loop().run_in_executor(
                None, run_un_update
            )
            
            # Registrar resultado
            self.update_history.append({
                'source': 'UN',
                'timestamp': datetime.utcnow(),
                'result': result,
                'success': result.get('status') == 'success'
            })
            
            # Notificar según resultado
            if result.get('status') == 'success':
                stats = result.get('stats', {})
                subject = "✅ Actualización ONU exitosa"
                body = f"""
Actualización de ONU completada exitosamente.

Estadísticas:
- Entidades agregadas: {stats.get('entities_added', 0)}
- Entidades actualizadas: {stats.get('entities_updated', 0)}
- Aliases agregados: {stats.get('aliases_added', 0)}
- Direcciones agregadas: {stats.get('addresses_added', 0)}
- Documentos agregados: {stats.get('documents_added', 0)}
- Duración: {result.get('duration', 0):.2f} segundos

Estado: {result.get('status', 'unknown')}
"""
                self.send_notification(subject, body)
                
            elif result.get('status') == 'no_changes':
                logger.info("No hay cambios en datos de ONU")
                
            else:
                subject = "❌ Error en actualización ONU"
                body = f"""
Error en la actualización de ONU:

Error: {result.get('error', 'Error desconocido')}
Duración: {result.get('duration', 0):.2f} segundos

Revisar logs para más detalles.
"""
                self.send_notification(subject, body, is_error=True)
            
            logger.info(f"Actualización ONU completada: {result.get('status')}")
            
        except Exception as e:
            logger.error(f"Error en job de ONU: {e}")
            self.send_notification(
                "❌ Error crítico en actualización ONU",
                f"Error crítico: {str(e)}",
                is_error=True
            )
    
    async def health_check_job(self):
        """Job para verificar salud del sistema"""
        try:
            db = SessionLocal()
            
            # Verificar conexión a BD
            db.execute("SELECT 1")
            
            # Verificar actualizaciones recientes
            recent_updates = db.query(UpdateLog).filter(
                UpdateLog.update_date >= datetime.utcnow() - timedelta(days=7)
            ).count()
            
            if recent_updates == 0:
                logger.warning("No hay actualizaciones en los últimos 7 días")
                self.send_notification(
                    "⚠️ Sin actualizaciones recientes",
                    "No se han registrado actualizaciones en los últimos 7 días. "
                    "Verificar que el scheduler esté funcionando correctamente.",
                    is_error=True
                )
            
            db.close()
            logger.info("Health check completado")
            
        except Exception as e:
            logger.error(f"Error en health check: {e}")
            self.send_notification(
                "❌ Error en health check",
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
            
            logger.info("Jobs programados configurados exitosamente")
            
        except Exception as e:
            logger.error(f"Error configurando jobs: {e}")
            raise
    
    def start(self):
        """Iniciar scheduler"""
        try:
            if not self.is_running:
                self.setup_jobs()
                self.scheduler.start()
                self.is_running = True
                logger.info("Scheduler iniciado exitosamente")
                
                # Enviar notificación de inicio
                self.send_notification(
                    "🚀 Scheduler iniciado",
                    "El scheduler de actualizaciones ETL ha sido iniciado exitosamente.\n\n"
                    "Programación:\n"
                    "- OFAC: Diario a las 8:00 AM UTC\n"
                    "- ONU: Lunes a las 9:00 AM UTC\n"
                    "- Health Check: Cada 6 horas"
                )
            else:
                logger.warning("Scheduler ya está corriendo")
                
        except Exception as e:
            logger.error(f"Error iniciando scheduler: {e}")
            raise
    
    def stop(self):
        """Detener scheduler"""
        try:
            if self.is_running:
                self.scheduler.shutdown()
                self.is_running = False
                logger.info("Scheduler detenido")
                
                # Enviar notificación de parada
                self.send_notification(
                    "🛑 Scheduler detenido",
                    "El scheduler de actualizaciones ETL ha sido detenido."
                )
            else:
                logger.warning("Scheduler no está corriendo")
                
        except Exception as e:
            logger.error(f"Error deteniendo scheduler: {e}")
    
    def get_job_status(self) -> Dict:
        """Obtener estado de los jobs"""
        jobs = []
        
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        
        return {
            'scheduler_running': self.is_running,
            'jobs': jobs,
            'recent_updates': self.update_history[-10:] if self.update_history else []
        }
    
    async def run_manual_update(self, source: str) -> Dict:
        """Ejecutar actualización manual"""
        logger.info(f"Iniciando actualización manual de {source}")
        
        try:
            if source.upper() == 'OFAC':
                result = await asyncio.get_event_loop().run_in_executor(
                    None, run_ofac_update
                )
            elif source.upper() == 'UN':
                result = await asyncio.get_event_loop().run_in_executor(
                    None, run_un_update
                )
            else:
                raise ValueError(f"Fuente no válida: {source}")
            
            # Registrar resultado
            self.update_history.append({
                'source': source.upper(),
                'timestamp': datetime.utcnow(),
                'result': result,
                'success': result.get('status') == 'success',
                'manual': True
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Error en actualización manual de {source}: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'source': source.upper()
            }

# Instancia global del scheduler
etl_scheduler = ETLScheduler()

# Funciones de conveniencia
def start_scheduler():
    """Iniciar scheduler"""
    etl_scheduler.start()

def stop_scheduler():
    """Detener scheduler"""
    etl_scheduler.stop()

def get_scheduler_status():
    """Obtener estado del scheduler"""
    return etl_scheduler.get_job_status()

async def run_manual_update(source: str):
    """Ejecutar actualización manual"""
    return await etl_scheduler.run_manual_update(source)

if __name__ == "__main__":
    # Ejecutar scheduler en modo standalone
    import signal
    import sys
    
    def signal_handler(sig, frame):
        logger.info("Recibida señal de terminación")
        etl_scheduler.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Iniciar scheduler
    etl_scheduler.start()
    
    # Mantener el programa corriendo
    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        logger.info("Terminando scheduler...")
        etl_scheduler.stop()