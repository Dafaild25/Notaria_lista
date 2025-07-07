# app/etl/ofac_parser_final.py
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

class OFACParserFinal:
    """Parser final para OFAC usando la estructura real confirmada"""
    
    def __init__(self, db_session: Session = None):
        self.db = db_session or SessionLocal()
        self.namespace = {'ofac': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML'}
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
    
    def download_ofac_data(self, url: str) -> Optional[str]:
        """Descargar archivo XML de OFAC"""
        try:
            logger.info(f"Descargando datos de OFAC: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=120)
            response.raise_for_status()
            
            logger.info(f"Descarga exitosa. Tamaño: {len(response.text)} caracteres")
            return response.text
            
        except requests.RequestException as e:
            logger.error(f"Error descargando datos de OFAC: {e}")
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
            # Formato alternativo: DD MMM YYYY
            elif len(date_string) > 10:
                return datetime.strptime(date_string, '%d %b %Y').date()
        except ValueError:
            logger.warning(f"No se pudo parsear fecha: {date_string}")
        
        return None
    
    def extract_entity_data(self, sdn_entry) -> Dict:
        """Extraer datos de un sdnEntry del XML"""
        entity_data = {
            'source_id': '',
            'name': '',
            'type': 'INDIVIDUAL',
            'aliases': [],
            'addresses': [],
            'documents': [],
            'nationalities': [],
            'birth_info': {},
            'sanctions': [],
            'program': ''
        }
        
        try:
            # Extraer UID
            uid_elem = sdn_entry.find('ofac:uid', self.namespace)
            if uid_elem is not None and uid_elem.text:
                entity_data['source_id'] = uid_elem.text.strip()
            
            # Extraer nombre
            first_name_elem = sdn_entry.find('ofac:firstName', self.namespace)
            last_name_elem = sdn_entry.find('ofac:lastName', self.namespace)
            
            name_parts = []
            if first_name_elem is not None and first_name_elem.text:
                name_parts.append(first_name_elem.text.strip())
            if last_name_elem is not None and last_name_elem.text:
                name_parts.append(last_name_elem.text.strip())
            
            if name_parts:
                entity_data['name'] = ' '.join(name_parts)
            
            # Extraer tipo
            sdn_type_elem = sdn_entry.find('ofac:sdnType', self.namespace)
            if sdn_type_elem is not None and sdn_type_elem.text:
                sdn_type = sdn_type_elem.text.strip()
                if sdn_type == 'Individual':
                    entity_data['type'] = 'INDIVIDUAL'
                elif sdn_type in ['Entity', 'Organization']:
                    entity_data['type'] = 'ENTITY'
                elif sdn_type == 'Vessel':
                    entity_data['type'] = 'VESSEL'
                elif sdn_type == 'Aircraft':
                    entity_data['type'] = 'AIRCRAFT'
            
            # Extraer programa de sanciones
            program_list = sdn_entry.find('ofac:programList', self.namespace)
            if program_list is not None:
                program_elem = program_list.find('ofac:program', self.namespace)
                if program_elem is not None and program_elem.text:
                    entity_data['program'] = program_elem.text.strip()
            
            # Extraer aliases (AKA)
            aka_list = sdn_entry.find('ofac:akaList', self.namespace)
            if aka_list is not None:
                for aka in aka_list.findall('ofac:aka', self.namespace):
                    aka_first = aka.find('ofac:firstName', self.namespace)
                    aka_last = aka.find('ofac:lastName', self.namespace)
                    
                    aka_name_parts = []
                    if aka_first is not None and aka_first.text:
                        aka_name_parts.append(aka_first.text.strip())
                    if aka_last is not None and aka_last.text:
                        aka_name_parts.append(aka_last.text.strip())
                    
                    if aka_name_parts:
                        aka_type = aka.find('ofac:type', self.namespace)
                        quality = 'UNKNOWN'
                        if aka_type is not None and aka_type.text:
                            quality = aka_type.text.strip().upper()
                        
                        entity_data['aliases'].append({
                            'name': ' '.join(aka_name_parts),
                            'quality': quality
                        })
            
            # Extraer fecha de nacimiento
            dob_list = sdn_entry.find('ofac:dateOfBirthList', self.namespace)
            if dob_list is not None:
                dob_item = dob_list.find('ofac:dateOfBirthItem', self.namespace)
                if dob_item is not None:
                    dob_elem = dob_item.find('ofac:dateOfBirth', self.namespace)
                    if dob_elem is not None and dob_elem.text:
                        entity_data['birth_info']['dob'] = dob_elem.text.strip()
            
            # Extraer lugar de nacimiento
            pob_list = sdn_entry.find('ofac:placeOfBirthList', self.namespace)
            if pob_list is not None:
                pob_item = pob_list.find('ofac:placeOfBirthItem', self.namespace)
                if pob_item is not None:
                    pob_elem = pob_item.find('ofac:placeOfBirth', self.namespace)
                    if pob_elem is not None and pob_elem.text:
                        entity_data['birth_info']['pob'] = pob_elem.text.strip()
            
            # Extraer direcciones
            address_list = sdn_entry.find('ofac:addressList', self.namespace)
            if address_list is not None:
                for address in address_list.findall('ofac:address', self.namespace):
                    address_parts = []
                    country = ''
                    
                    # Extraer partes de la dirección
                    addr1 = address.find('ofac:address1', self.namespace)
                    if addr1 is not None and addr1.text:
                        address_parts.append(addr1.text.strip())
                    
                    addr2 = address.find('ofac:address2', self.namespace)
                    if addr2 is not None and addr2.text:
                        address_parts.append(addr2.text.strip())
                    
                    city = address.find('ofac:city', self.namespace)
                    if city is not None and city.text:
                        address_parts.append(city.text.strip())
                    
                    state = address.find('ofac:stateOrProvince', self.namespace)
                    if state is not None and state.text:
                        address_parts.append(state.text.strip())
                    
                    country_elem = address.find('ofac:country', self.namespace)
                    if country_elem is not None and country_elem.text:
                        country = country_elem.text.strip()
                        address_parts.append(country)
                    
                    if address_parts:
                        entity_data['addresses'].append({
                            'full_address': ', '.join(address_parts),
                            'country': country
                        })
            
            # Extraer documentos de identidad
            id_list = sdn_entry.find('ofac:idList', self.namespace)
            if id_list is not None:
                for id_item in id_list.findall('ofac:id', self.namespace):
                    id_type = id_item.find('ofac:idType', self.namespace)
                    id_number = id_item.find('ofac:idNumber', self.namespace)
                    id_country = id_item.find('ofac:idCountry', self.namespace)
                    
                    if id_number is not None and id_number.text:
                        entity_data['documents'].append({
                            'type': id_type.text.strip() if id_type is not None else 'UNKNOWN',
                            'number': id_number.text.strip(),
                            'issuer': id_country.text.strip() if id_country is not None else ''
                        })
            
            # Extraer nacionalidades
            nationality_list = sdn_entry.find('ofac:nationalityList', self.namespace)
            if nationality_list is not None:
                for nationality in nationality_list.findall('ofac:nationality', self.namespace):
                    country = nationality.find('ofac:country', self.namespace)
                    if country is not None and country.text:
                        entity_data['nationalities'].append(country.text.strip())
        
        except Exception as e:
            logger.error(f"Error extrayendo datos de entidad: {e}")
            self.stats['errors'].append(str(e))
        
        return entity_data
    
    def parse_ofac_xml(self, xml_content: str) -> bool:
        """Parsear archivo XML de OFAC usando estructura correcta"""
        try:
            logger.info("Iniciando procesamiento de XML de OFAC")
            root = ET.fromstring(xml_content)
            
            # Buscar sdnEntry usando namespace
            sdn_entries = root.findall('.//ofac:sdnEntry', self.namespace)
            
            if not sdn_entries:
                logger.warning("No se encontraron sdnEntry en el XML")
                return False
            
            logger.info(f"Encontradas {len(sdn_entries)} entidades en el XML")
            
            processed_count = 0
            
            for sdn_entry in sdn_entries:
                try:
                    entity_data = self.extract_entity_data(sdn_entry)
                    
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
            logger.error(f"Error general en parseo de OFAC: {e}")
            self.db.rollback()
            return False
    
    def process_entity(self, entity_data: Dict) -> Entity:
        """Procesar y guardar una entidad en la base de datos"""
        try:
            # Verificar si la entidad ya existe
            existing_entity = self.db.query(Entity).filter(
                Entity.source == 'OFAC',
                Entity.source_id == entity_data['source_id']
            ).first()
            
            if existing_entity:
                # Actualizar entidad existente
                existing_entity.name = entity_data['name']
                existing_entity.type = entity_data['type']
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
                    source='OFAC',
                    source_id=entity_data['source_id'],
                    name=entity_data['name'],
                    type=entity_data['type'],
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
                program=entity_data['program'] or 'OFAC',
                authority='OFAC',
                comments=f"Sancionado por OFAC - Programa: {entity_data['program']}"
            )
            self.db.add(sanction)
            self.stats['sanctions_added'] += 1
            
            return entity
            
        except Exception as e:
            logger.error(f"Error procesando entidad {entity_data['name']}: {e}")
            self.stats['errors'].append(str(e))
            return None
    
    def run_ofac_update(self) -> Dict:
        """Ejecutar actualización de OFAC"""
        start_time = datetime.utcnow()
        
        try:
            # Descargar datos
            xml_content = self.download_ofac_data(settings.ofac_consolidated_url)
            if not xml_content:
                raise Exception("No se pudo descargar el archivo XML")
            
            # Verificar cambios
            file_hash = self.calculate_hash(xml_content)
            last_update = self.db.query(UpdateLog).filter(
                UpdateLog.source == 'OFAC'
            ).order_by(UpdateLog.update_date.desc()).first()
            
            if last_update and last_update.file_hash == file_hash:
                logger.info("No hay cambios en los datos de OFAC")
                return {'status': 'no_changes', 'hash': file_hash}
            
            # Procesar XML
            success = self.parse_ofac_xml(xml_content)
            
            # Registrar actualización
            update_log = UpdateLog(
                source='OFAC',
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
            logger.error(f"Error en actualización OFAC: {e}")
            
            update_log = UpdateLog(
                source='OFAC',
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
def run_ofac_update():
    """Ejecutar actualización de OFAC con parser final"""
    parser = OFACParserFinal()
    result = parser.run_ofac_update()
    return result

if __name__ == "__main__":
    result = run_ofac_update()
    print(f"Resultado: {result}")