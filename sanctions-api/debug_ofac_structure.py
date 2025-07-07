# debug_ofac_structure.py
"""
Script para analizar la estructura real del XML de OFAC
"""
import requests
import xml.etree.ElementTree as ET
from app.core.config import settings

def analyze_ofac_structure():
    """Analizar la estructura del XML de OFAC"""
    
    print("🔍 Analizando estructura del XML de OFAC...")
    
    # Descargar XML
    response = requests.get(settings.ofac_consolidated_url, timeout=60)
    xml_content = response.text
    
    print(f"📄 Tamaño del archivo: {len(xml_content)} caracteres")
    
    # Parsear XML
    root = ET.fromstring(xml_content)
    
    print(f"🌳 Elemento raíz: <{root.tag}>")
    print(f"   Atributos: {root.attrib}")
    
    # Analizar estructura nivel por nivel
    print("\n📊 Estructura del XML:")
    
    def analyze_element(element, level=0, max_level=3):
        indent = "  " * level
        
        if level > max_level:
            return
        
        # Mostrar información del elemento
        print(f"{indent}<{element.tag}>")
        
        # Mostrar atributos si los tiene
        if element.attrib:
            print(f"{indent}  Atributos: {element.attrib}")
        
        # Contar hijos únicos
        child_tags = {}
        for child in element:
            tag = child.tag
            if tag not in child_tags:
                child_tags[tag] = 0
            child_tags[tag] += 1
        
        # Mostrar resumen de hijos
        for tag, count in child_tags.items():
            print(f"{indent}  └── <{tag}> ({count} elementos)")
            
            # Analizar primer hijo de cada tipo
            if level < max_level:
                first_child = element.find(tag)
                if first_child is not None:
                    analyze_element(first_child, level + 1, max_level)
            
            if count > 1:
                print(f"{indent}      ... y {count-1} más")
    
    analyze_element(root)
    
    # Buscar patrones comunes
    print("\n🔍 Buscando patrones comunes...")
    
    patterns_to_check = [
        'distinctParty',
        'sdnEntry', 
        'entry',
        'record',
        'entity',
        'individual',
        'person',
        'organization',
        'name',
        'firstName',
        'lastName'
    ]
    
    for pattern in patterns_to_check:
        elements = root.findall(f'.//{pattern}')
        if elements:
            print(f"✅ Encontrados {len(elements)} elementos <{pattern}>")
            
            # Mostrar estructura del primer elemento
            if elements:
                first_elem = elements[0]
                print(f"   Primer elemento <{pattern}>:")
                print(f"     Atributos: {first_elem.attrib}")
                
                # Mostrar hijos del primer elemento
                children = list(first_elem)
                if children:
                    print(f"     Hijos: {[child.tag for child in children[:5]]}")
                    if len(children) > 5:
                        print(f"     ... y {len(children)-5} más")
                
                # Mostrar texto si lo tiene
                if first_elem.text and first_elem.text.strip():
                    print(f"     Texto: {first_elem.text.strip()[:50]}...")
        else:
            print(f"❌ No encontrados elementos <{pattern}>")
    
    # Buscar elementos con nombres
    print("\n🔍 Buscando elementos con nombres...")
    
    # Buscar cualquier elemento que contenga texto parecido a nombres
    all_elements = root.findall('.//*')
    name_elements = []
    
    for elem in all_elements:
        if elem.text and elem.text.strip():
            text = elem.text.strip()
            # Criterio simple: texto con espacios, mayúsculas, longitud razonable
            if (' ' in text and 
                any(c.isupper() for c in text) and 
                3 < len(text) < 100 and
                not text.isdigit()):
                name_elements.append((elem.tag, text))
    
    if name_elements:
        print(f"✅ Encontrados {len(name_elements)} elementos con posibles nombres")
        
        # Mostrar algunos ejemplos
        print("   Ejemplos:")
        for tag, text in name_elements[:10]:
            print(f"     <{tag}>: {text}")
        
        # Mostrar estadísticas de tags
        tag_counts = {}
        for tag, _ in name_elements:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        print("   Tags más comunes:")
        for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"     <{tag}>: {count} elementos")
    else:
        print("❌ No se encontraron elementos con nombres aparentes")
    
    print("\n✅ Análisis completado")

if __name__ == "__main__":
    analyze_ofac_structure()