# app/etl/un_parser_final.py
import requests
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional
import hashlib
import logging
from datetime import datetime, date
from sqlalchemy.orm import Session

from app.models.database import SessionLocal
from app.models.entities import (
    Entity, Alias, Document, Address, Birth, 
    Nationality, Sanction, UpdateLog
)
from app.core.config import settings

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UNParserFinal:
    """Parser final para ONU usando la estructura real confirmada"""
    
    def __init__(self, db_session: Session = None):
        self.db = db_session or SessionLocal()
        self.stats = {
            'entities_added': 0,
            'entities_updated': 0,
            'entities_deleted': 0,
            'aliases_added': 0,
            'addresses_added': 0,
            'documents_added': 0,
            'sanctions_added': 0,
            'errors': []
        }
    
    def download_un_data(self, url: str) -> Optional[str]:
        """Descargar archivo XML de ONU"""
        try:
            logger.info(f"Descargando datos de ONU: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=120)
            response.raise_for_status()
            
            logger.info(f"Descarga exitosa. Tamaño: {len(response.text)} caracteres")
            return response.text
            
        except requests.RequestException as e:
            logger.error(f"Error descargando datos de ONU: {e}")
            return None
    
    def calculate_hash(self, xml_content: str) -> str:
        """Calcular hash del contenido XML"""
        return hashlib.md5(xml_content.encode()).hexdigest()
    
    def parse_date(self, date_string: str) -> Optional[date]:
        """Parsear fecha desde string"""
        if not date_string:
            return None
        
        try:
            # Formato común: YYYY-MM-DD
            if len(date_string) == 10:
                return datetime.strptime(date_string, '%Y-%m-%d').date()
            # Formato alternativo: DD/MM/YYYY
            elif '/' in date_string:
                return datetime.strptime(date_string, '%d/%m/%Y').date()
        except ValueError:
            logger.warning(f"No se pudo parsear fecha: {date_string}")
        
        return None
    
    def extract_individual_data(self, individual) -> Dict:
        """Extraer datos de un individuo del XML de ONU"""
        entity_data = {
            'source_id': '',
            'name': '',
            'type': 'INDIVIDUAL',
            'title': '',
            'aliases': [],
            'addresses': [],
            'documents': [],
            'nationalities': [],
            'birth_info': {},
            'sanctions': [],
            'committee': '',
            'resolution': '',
            'remarks': '',
            'listed_on': None
        }
        
        try:
            # Extraer ID
            dataid = individual.find('DATAID')
            if dataid is not None and dataid.text:
                entity_data['source_id'] = dataid.text.strip()
            
            # Extraer nombres
            name_parts = []
            for name_field in ['FIRST_NAME', 'SECOND_NAME', 'THIRD_NAME', 'FOURTH_NAME']:
                name_elem = individual.find(name_field)
                if name_elem is not None and name_elem.text:
                    name_parts.append(name_elem.text.strip())
            
            if name_parts:
                entity_data['name'] = ' '.join(name_parts)
            
            # Extraer comentarios/remarks
            comments = individual.find('COMMENTS1')
            if comments is not None and comments.text:
                entity_data['remarks'] = comments.text.strip()
            
            # Extraer fecha de listado
            listed_on = individual.find('LISTED_ON')
            if listed_on is not None and listed_on.text:
                entity_data['listed_on'] = self.parse_date(listed_on.text.strip())
            
            # Extraer tipo de lista UN
            un_list_type = individual.find('UN_LIST_TYPE')
            if un_list_type is not None and un_list_type.text:
                entity_data['committee'] = un_list_type.text.strip()
            
            # Extraer número de referencia
            reference_number = individual.find('REFERENCE_NUMBER')
            if reference_number is not None and reference_number.text:
                entity_data['resolution'] = reference_number.text.strip()
            
            # Extraer nacionalidades
            nationality_elem = individual.find('NATIONALITY')
            if nationality_elem is not None:
                value_elem = nationality_elem.find('VALUE')
                if value_elem is not None and value_elem.text:
                    entity_data['nationalities'].append(value_elem.text.strip())
            
            # Extraer fecha de nacimiento
            dob_elem = individual.find('INDIVIDUAL_DATE_OF_BIRTH')
            if dob_elem is not None:
                year_elem = dob_elem.find('YEAR')
                if year_elem is not None and year_elem.text:
                    entity_data['birth_info']['dob'] = year_elem.text.strip()
            
            # Extraer lugar de nacimiento
            pob_elem = individual.find('INDIVIDUAL_PLACE_OF_BIRTH')
            if pob_elem is not None:
                country_elem = pob_elem.find('COUNTRY')
                if country_elem is not None and country_elem.text:
                    entity_data['birth_info']['pob'] = country_elem.text.strip()
            
            # Extraer aliases
            for alias in individual.findall('INDIVIDUAL_ALIAS'):
                alias_name = alias.find('ALIAS_NAME')
                quality = alias.find('QUALITY')
                
                if alias_name is not None and alias_name.text:
                    # Normalizar calidad de alias - mantener valor original si no está mapeado
                    quality_mapping = {
                        'strong': 'STRONG',
                        'weak': 'WEAK',
                        'good': 'GOOD',
                        'low': 'LOW',
                        'high': 'HIGH',
                        'medium': 'MEDIUM'
                    }
                    
                    if quality is not None and quality.text:
                        normalized_quality = quality_mapping.get(quality.text.strip().lower(), quality.text.strip().upper())
                    else:
                        normalized_quality = 'UNKNOWN'
                    
                    entity_data['aliases'].append({
                        'name': alias_name.text.strip(),
                        'quality': normalized_quality
                    })
            
            # Extraer direcciones
            for address in individual.findall('INDIVIDUAL_ADDRESS'):
                address_parts = []
                country = ''
                
                # Extraer partes de la dirección
                for field in ['STREET', 'CITY', 'STATE_PROVINCE']:
                    elem = address.find(field)
                    if elem is not None and elem.text:
                        address_parts.append(elem.text.strip())
                
                # Extraer país
                country_elem = address.find('COUNTRY')
                if country_elem is not None and country_elem.text:
                    country = country_elem.text.strip()
                    address_parts.append(country)
                
                # Extraer nota adicional
                note_elem = address.find('NOTE')
                if note_elem is not None and note_elem.text:
                    address_parts.append(f"Nota: {note_elem.text.strip()}")
                
                if address_parts:
                    entity_data['addresses'].append({
                        'full_address': ', '.join(address_parts),
                        'country': country
                    })
            
            # Extraer documentos
            for document in individual.findall('INDIVIDUAL_DOCUMENT'):
                doc_type_elem = document.find('TYPE_OF_DOCUMENT')
                doc_number_elem = document.find('NUMBER')
                doc_country_elem = document.find('ISSUING_COUNTRY')
                
                if doc_number_elem is not None and doc_number_elem.text:
                    entity_data['documents'].append({
                        'type': doc_type_elem.text.strip() if doc_type_elem is not None and doc_type_elem.text else 'UNKNOWN',
                        'number': doc_number_elem.text.strip(),
                        'issuer': doc_country_elem.text.strip() if doc_country_elem is not None and doc_country_elem.text else ''
                    })
        
        except Exception as e:
            logger.error(f"Error extrayendo datos de individuo: {e}")
            self.stats['errors'].append(str(e))
        
        return entity_data
    
    def extract_entity_data(self, entity) -> Dict:
        """Extraer datos de una entidad del XML de ONU"""
        entity_data = {
            'source_id': '',
            'name': '',
            'type': 'ENTITY',
            'aliases': [],
            'addresses': [],
            'documents': [],
            'nationalities': [],
            'birth_info': {},
            'sanctions': [],
            'committee': '',
            'resolution': '',
            'remarks': '',
            'listed_on': None
        }
        
        try:
            # Extraer ID
            dataid = entity.find('DATAID')
            if dataid is not None and dataid.text:
                entity_data['source_id'] = dataid.text.strip()
            
            # Extraer nombre (para entidades solo hay FIRST_NAME)
            first_name = entity.find('FIRST_NAME')
            if first_name is not None and first_name.text:
                entity_data['name'] = first_name.text.strip()
            
            # Extraer comentarios/remarks
            comments = entity.find('COMMENTS1')
            if comments is not None and comments.text:
                entity_data['remarks'] = comments.text.strip()
            
            # Extraer fecha de listado
            listed_on = entity.find('LISTED_ON')
            if listed_on is not None and listed_on.text:
                entity_data['listed_on'] = self.parse_date(listed_on.text.strip())
            
            # Extraer tipo de lista UN
            un_list_type = entity.find('UN_LIST_TYPE')
            if un_list_type is not None and un_list_type.text:
                entity_data['committee'] = un_list_type.text.strip()
            
            # Extraer número de referencia
            reference_number = entity.find('REFERENCE_NUMBER')
            if reference_number is not None and reference_number.text:
                entity_data['resolution'] = reference_number.text.strip()
            
            # Extraer aliases
            for alias in entity.findall('ENTITY_ALIAS'):
                alias_name = alias.find('ALIAS_NAME')
                quality = alias.find('QUALITY')
                
                if alias_name is not None and alias_name.text:
                    # Normalizar calidad de alias - mantener valor original si no está mapeado
                    quality_mapping = {
                        'strong': 'STRONG',
                        'weak': 'WEAK',
                        'good': 'GOOD',
                        'low': 'LOW',
                        'high': 'HIGH',
                        'medium': 'MEDIUM'
                    }
                    
                    if quality is not None and quality.text:
                        normalized_quality = quality_mapping.get(quality.text.strip().lower(), quality.text.strip().upper())
                    else:
                        normalized_quality = 'UNKNOWN'
                    
                    entity_data['aliases'].append({
                        'name': alias_name.text.strip(),
                        'quality': normalized_quality
                    })
            
            # Extraer direcciones
            for address in entity.findall('ENTITY_ADDRESS'):
                address_parts = []
                country = ''
                
                # Extraer partes de la dirección
                for field in ['STREET', 'CITY', 'STATE_PROVINCE']:
                    elem = address.find(field)
                    if elem is not None and elem.text:
                        address_parts.append(elem.text.strip())
                
                # Extraer país
                country_elem = address.find('COUNTRY')
                if country_elem is not None and country_elem.text:
                    country = country_elem.text.strip()
                    address_parts.append(country)
                
                if address_parts:
                    entity_data['addresses'].append({
                        'full_address': ', '.join(address_parts),
                        'country': country
                    })
        
        except Exception as e:
            logger.error(f"Error extrayendo datos de entidad: {e}")
            self.stats['errors'].append(str(e))
        
        return entity_data
    
    def parse_un_xml(self, xml_content: str) -> bool:
        """Parsear archivo XML de ONU usando estructura correcta"""
        try:
            logger.info("Iniciando procesamiento de XML de ONU")
            root = ET.fromstring(xml_content)
            
            processed_count = 0
            
            # Procesar individuos
            individuals = root.findall('.//INDIVIDUAL')
            logger.info(f"Encontrados {len(individuals)} individuos en el XML")
            
            for individual in individuals:
                try:
                    entity_data = self.extract_individual_data(individual)
                    
                    if entity_data['name']:  # Solo procesar si tiene nombre
                        self.process_entity(entity_data)
                        processed_count += 1
                        
                        if processed_count % 100 == 0:
                            logger.info(f"Procesadas {processed_count} entidades...")
                            self.db.commit()
                
                except Exception as e:
                    logger.error(f"Error procesando individuo: {e}")
                    self.stats['errors'].append(str(e))
                    continue
            
            # Procesar entidades
            entities = root.findall('.//ENTITY')
            logger.info(f"Encontradas {len(entities)} entidades en el XML")
            
            for entity in entities:
                try:
                    entity_data = self.extract_entity_data(entity)
                    
                    if entity_data['name']:  # Solo procesar si tiene nombre
                        self.process_entity(entity_data)
                        processed_count += 1
                        
                        if processed_count % 100 == 0:
                            logger.info(f"Procesadas {processed_count} entidades...")
                            self.db.commit()
                
                except Exception as e:
                    logger.error(f"Error procesando entidad: {e}")
                    self.stats['errors'].append(str(e))
                    continue
            
            # Commit final
            self.db.commit()
            logger.info(f"Procesamiento completado. Total entidades: {processed_count}")
            return True
            
        except Exception as e:
            logger.error(f"Error general en parseo de ONU: {e}")
            self.db.rollback()
            return False
    
    def process_entity(self, entity_data: Dict) -> Entity:
        """Procesar y guardar una entidad en la base de datos"""
        try:
            # Verificar si la entidad ya existe
            existing_entity = self.db.query(Entity).filter(
                Entity.source == 'UN',
                Entity.source_id == entity_data['source_id']
            ).first()
            
            if existing_entity:
                # Actualizar entidad existente
                existing_entity.name = entity_data['name']
                existing_entity.type = entity_data['type']
                existing_entity.remarks = entity_data.get('remarks', '')
                existing_entity.committee = entity_data.get('committee', '')
                existing_entity.resolution = entity_data.get('resolution', '')
                existing_entity.listed_on = entity_data.get('listed_on')
                existing_entity.last_updated = datetime.utcnow()
                existing_entity.status = 'UPDATED'
                
                # Limpiar relaciones existentes
                for alias in existing_entity.aliases:
                    self.db.delete(alias)
                for address in existing_entity.addresses:
                    self.db.delete(address)
                for document in existing_entity.documents:
                    self.db.delete(document)
                for nationality in existing_entity.nationalities:
                    self.db.delete(nationality)
                for sanction in existing_entity.sanctions:
                    self.db.delete(sanction)
                
                entity = existing_entity
                self.stats['entities_updated'] += 1
            else:
                # Crear nueva entidad
                entity = Entity(
                    source='UN',
                    source_id=entity_data['source_id'],
                    name=entity_data['name'],
                    type=entity_data['type'],
                    remarks=entity_data.get('remarks', ''),
                    committee=entity_data.get('committee', ''),
                    resolution=entity_data.get('resolution', ''),
                    listed_on=entity_data.get('listed_on'),
                    status='ACTIVE',
                    hash_signature=hashlib.md5(str(entity_data).encode()).hexdigest()
                )
                self.db.add(entity)
                self.stats['entities_added'] += 1
            
            # Flush para obtener el ID
            self.db.flush()
            
            # Agregar aliases
            for alias_data in entity_data['aliases']:
                alias = Alias(
                    entity_id=entity.id,
                    alias_name=alias_data['name'],
                    quality=alias_data['quality']
                )
                self.db.add(alias)
                self.stats['aliases_added'] += 1
            
            # Agregar direcciones
            for address_data in entity_data['addresses']:
                address = Address(
                    entity_id=entity.id,
                    full_address=address_data['full_address'],
                    country=address_data['country']
                )
                self.db.add(address)
                self.stats['addresses_added'] += 1
            
            # Agregar documentos
            for doc_data in entity_data['documents']:
                document = Document(
                    entity_id=entity.id,
                    doc_type=doc_data['type'],
                    doc_number=doc_data['number'],
                    issuer=doc_data['issuer']
                )
                self.db.add(document)
                self.stats['documents_added'] += 1
            
            # Agregar nacionalidades
            for country in entity_data['nationalities']:
                nationality = Nationality(
                    entity_id=entity.id,
                    country=country
                )
                self.db.add(nationality)
            
            # Agregar información de nacimiento
            if entity_data['birth_info']:
                birth = Birth(
                    entity_id=entity.id,
                    dob=entity_data['birth_info'].get('dob', ''),
                    pob=entity_data['birth_info'].get('pob', '')
                )
                self.db.add(birth)
            
            # Agregar sanción
            sanction = Sanction(
                entity_id=entity.id,
                program=entity_data.get('committee', 'UN Security Council'),
                authority='UN',
                listing_date=entity_data.get('listed_on'),
                comments=entity_data.get('remarks', '')
            )
            self.db.add(sanction)
            self.stats['sanctions_added'] += 1
            
            return entity
            
        except Exception as e:
            logger.error(f"Error procesando entidad {entity_data['name']}: {e}")
            self.stats['errors'].append(str(e))
            return None
    
    def run_un_update(self) -> Dict:
        """Ejecutar actualización de ONU"""
        start_time = datetime.utcnow()
        
        try:
            # Descargar datos
            xml_content = self.download_un_data(settings.un_consolidated_url)
            if not xml_content:
                raise Exception("No se pudo descargar el archivo XML")
            
            # Verificar cambios
            file_hash = self.calculate_hash(xml_content)
            last_update = self.db.query(UpdateLog).filter(
                UpdateLog.source == 'UN'
            ).order_by(UpdateLog.update_date.desc()).first()
            
            if last_update and last_update.file_hash == file_hash:
                logger.info("No hay cambios en los datos de ONU")
                return {'status': 'no_changes', 'hash': file_hash}
            
            # Procesar XML
            success = self.parse_un_xml(xml_content)
            
            # Registrar actualización
            update_log = UpdateLog(
                source='UN',
                update_date=start_time,
                records_added=self.stats['entities_added'],
                records_updated=self.stats['entities_updated'],
                records_deleted=self.stats['entities_deleted'],
                file_hash=file_hash,
                status='SUCCESS' if success else 'FAILED',
                error_message='; '.join(self.stats['errors']) if self.stats['errors'] else None
            )
            
            self.db.add(update_log)
            self.db.commit()
            
            return {
                'status': 'success' if success else 'failed',
                'stats': self.stats,
                'duration': (datetime.utcnow() - start_time).total_seconds()
            }
            
        except Exception as e:
            logger.error(f"Error en actualización ONU: {e}")
            
            update_log = UpdateLog(
                source='UN',
                update_date=start_time,
                status='FAILED',
                error_message=str(e)
            )
            self.db.add(update_log)
            self.db.commit()
            
            return {
                'status': 'failed',
                'error': str(e),
                'duration': (datetime.utcnow() - start_time).total_seconds()
            }
        finally:
            if self.db:
                self.db.close()

# Función de conveniencia
def run_un_update():
    """Ejecutar actualización de ONU con parser final"""
    parser = UNParserFinal()
    result = parser.run_un_update()
    return result

if __name__ == "__main__":
    result = run_un_update()
    print(f"Resultado: {result}")