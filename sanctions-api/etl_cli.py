# etl_cli.py
"""
CLI para administrar el ETL de Sanctions API
Uso: python etl_cli.py [comando] [opciones]
"""

import argparse
import sys
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent))

from app.etl.ofac_parser_final import OFACParserFinal, run_ofac_update
from app.etl.un_parser_final import  UNParserFinal, run_un_update
from app.etl.scheduler import ETLScheduler
from app.models.database import SessionLocal
from app.models.entities import Entity, UpdateLog
from sqlalchemy import func, desc

def print_banner():
    """Mostrar banner de la aplicación"""
    print("=" * 60)
    print("   🚀 SANCTIONS API - ETL COMMAND LINE INTERFACE")
    print("=" * 60)
    print()

def print_stats(stats):
    """Mostrar estadísticas de actualización"""
    print(f"📊 Estadísticas de actualización:")
    print(f"   • Entidades agregadas: {stats.get('entities_added', 0)}")
    print(f"   • Entidades actualizadas: {stats.get('entities_updated', 0)}")
    print(f"   • Aliases agregados: {stats.get('aliases_added', 0)}")
    print(f"   • Direcciones agregadas: {stats.get('addresses_added', 0)}")
    print(f"   • Documentos agregados: {stats.get('documents_added', 0)}")
    print(f"   • Sanciones agregadas: {stats.get('sanctions_added', 0)}")
    
    if stats.get('errors'):
        print(f"   ⚠️  Errores encontrados: {len(stats['errors'])}")
        for error in stats['errors'][:5]:  # Mostrar solo los primeros 5
            print(f"      - {error}")

def update_ofac(args):
    """Actualizar datos de OFAC"""
    print("🔄 Iniciando actualización de OFAC...")
    source = getattr(args, 'source', None)
    print(f"   Fuente: {source or 'Consolidated'}")
    print()
    
    start_time = datetime.now()
    
    try:
        result = run_ofac_update()
        
        duration = (datetime.now() - start_time).total_seconds()
        
        if result.get('status') == 'success':
            print("✅ Actualización OFAC completada exitosamente")
            print(f"   Duración: {duration:.2f} segundos")
            print()
            print_stats(result.get('stats', {}))
            
        elif result.get('status') == 'no_changes':
            print("ℹ️  No hay cambios en los datos de OFAC")
            print(f"   Hash del archivo: {result.get('hash', 'N/A')}")
            
        else:
            print("❌ Error en actualización de OFAC")
            print(f"   Error: {result.get('error', 'Error desconocido')}")
            print(f"   Duración: {duration:.2f} segundos")
            
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        return 1
    
    return 0

def update_un(args):
    """Actualizar datos de ONU"""
    print("🔄 Iniciando actualización de ONU...")
    print()
    
    start_time = datetime.now()
    
    try:
        result = run_un_update()
        
        duration = (datetime.now() - start_time).total_seconds()
        
        if result.get('status') == 'success':
            print("✅ Actualización ONU completada exitosamente")
            print(f"   Duración: {duration:.2f} segundos")
            print()
            print_stats(result.get('stats', {}))
            
        elif result.get('status') == 'no_changes':
            print("ℹ️  No hay cambios en los datos de ONU")
            print(f"   Hash del archivo: {result.get('hash', 'N/A')}")
            
        else:
            print("❌ Error en actualización de ONU")
            print(f"   Error: {result.get('error', 'Error desconocido')}")
            print(f"   Duración: {duration:.2f} segundos")
            
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        return 1
    
    return 0

def update_all(args):
    """Actualizar ambas fuentes"""
    print("🔄 Iniciando actualización completa (OFAC + ONU)...")
    print()
    
    # Crear un objeto args mock para las funciones individuales
    class MockArgs:
        def __init__(self):
            self.source = None
            self.limit = 10
            self.status = None
    
    mock_args = MockArgs()
    
    # Actualizar OFAC
    print("1️⃣ Actualizando OFAC...")
    ofac_result = update_ofac(mock_args)
    print()
    
    # Actualizar ONU
    print("2️⃣ Actualizando ONU...")
    un_result = update_un(mock_args)
    print()
    
    if ofac_result == 0 and un_result == 0:
        print("✅ Actualización completa exitosa")
        return 0
    else:
        print("❌ Hubo errores en la actualización")
        return 1

def show_stats(args):
    """Mostrar estadísticas de la base de datos"""
    print("📊 Estadísticas de la base de datos:")
    print()
    
    try:
        db = SessionLocal()
        
        # Estadísticas generales
        total_entities = db.query(Entity).count()
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
        
        print(f"📈 Totales:")
        print(f"   • Total de entidades: {total_entities:,}")
        print(f"   • OFAC: {ofac_count:,}")
        print(f"   • ONU: {un_count:,}")
        print()
        
        print(f"👥 Por tipo:")
        print(f"   • Individuos: {individual_count:,}")
        print(f"   • Entidades: {entity_count:,}")
        print(f"   • Embarcaciones: {vessel_count:,}")
        print(f"   • Aeronaves: {aircraft_count:,}")
        print()
        
        print(f"🎯 Por estado:")
        print(f"   • Activos: {active_count:,}")
        print(f"   • Actualizados: {updated_count:,}")
        print()
        
        # Últimas actualizaciones
        print(f"🕐 Últimas actualizaciones:")
        recent_updates = db.query(UpdateLog).order_by(desc(UpdateLog.update_date)).limit(5).all()
        
        for update in recent_updates:
            status_icon = "✅" if update.status == 'SUCCESS' else "❌"
            print(f"   {status_icon} {update.source} - {update.update_date.strftime('%Y-%m-%d %H:%M:%S')}")
            if update.status == 'SUCCESS':
                print(f"      Agregadas: {update.records_added}, Actualizadas: {update.records_updated}")
        
        db.close()
        
    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
        return 1
    
    return 0

def show_logs(args):
    """Mostrar logs de actualización"""
    print(f"📄 Logs de actualización (últimas {args.limit}):")
    print()
    
    try:
        db = SessionLocal()
        
        query = db.query(UpdateLog).order_by(desc(UpdateLog.update_date))
        
        if args.source:
            query = query.filter(UpdateLog.source == args.source.upper())
        
        if args.status:
            query = query.filter(UpdateLog.status == args.status.upper())
        
        logs = query.limit(args.limit).all()
        
        for log in logs:
            status_icon = "✅" if log.status == 'SUCCESS' else "❌" if log.status == 'FAILED' else "⏳"
            print(f"{status_icon} {log.source} - {log.update_date.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Estado: {log.status}")
            
            if log.status == 'SUCCESS':
                print(f"   Agregadas: {log.records_added}, Actualizadas: {log.records_updated}")
            
            if log.error_message:
                print(f"   Error: {log.error_message[:100]}...")
            
            print()
        
        db.close()
        
    except Exception as e:
        print(f"❌ Error obteniendo logs: {e}")
        return 1
    
    return 0

def run_scheduler(args):
    """Ejecutar scheduler"""
    print("🚀 Iniciando scheduler ETL...")
    print()
    
    try:
        scheduler = ETLScheduler()
        
        # Configurar señales para terminación limpia
        import signal
        
        def signal_handler(sig, frame):
            print("\n🛑 Deteniendo scheduler...")
            scheduler.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Iniciar scheduler
        scheduler.start()
        
        print("✅ Scheduler iniciado exitosamente")
        print("📅 Programación:")
        print("   • OFAC: Diario a las 8:00 AM UTC")
        print("   • ONU: Lunes a las 9:00 AM UTC")
        print("   • Health Check: Cada 6 horas")
        print()
        print("Presiona Ctrl+C para detener...")
        
        # Mantener el programa corriendo
        asyncio.get_event_loop().run_forever()
        
    except KeyboardInterrupt:
        print("\n🛑 Scheduler detenido por el usuario")
        return 0
    except Exception as e:
        print(f"❌ Error en scheduler: {e}")
        return 1

def main():
    """Función principal"""
    print_banner()
    
    parser = argparse.ArgumentParser(
        description="CLI para administrar el ETL de Sanctions API",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Comandos disponibles')
    
    # Comando update-ofac
    ofac_parser = subparsers.add_parser('update-ofac', help='Actualizar datos de OFAC')
    ofac_parser.add_argument('--source', choices=['sdn', 'consolidated'], 
                           help='Fuente específica a actualizar')
    
    # Comando update-un
    un_parser = subparsers.add_parser('update-un', help='Actualizar datos de ONU')
    
    # Comando update-all
    all_parser = subparsers.add_parser('update-all', help='Actualizar todas las fuentes')
    
    # Comando stats
    stats_parser = subparsers.add_parser('stats', help='Mostrar estadísticas')
    
    # Comando logs
    logs_parser = subparsers.add_parser('logs', help='Mostrar logs')
    logs_parser.add_argument('--limit', type=int, default=10, help='Número de logs a mostrar')
    logs_parser.add_argument('--source', choices=['ofac', 'un'], help='Filtrar por fuente')
    logs_parser.add_argument('--status', choices=['success', 'failed'], help='Filtrar por estado')
    
    # Comando scheduler
    scheduler_parser = subparsers.add_parser('scheduler', help='Ejecutar scheduler')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Ejecutar comando
    if args.command == 'update-ofac':
        return update_ofac(args)
    elif args.command == 'update-un':
        return update_un(args)
    elif args.command == 'update-all':
        return update_all(args)
    elif args.command == 'stats':
        return show_stats(args)
    elif args.command == 'logs':
        return show_logs(args)
    elif args.command == 'scheduler':
        return run_scheduler(args)
    else:
        print(f"❌ Comando no reconocido: {args.command}")
        return 1

if __name__ == "__main__":
    sys.exit(main())