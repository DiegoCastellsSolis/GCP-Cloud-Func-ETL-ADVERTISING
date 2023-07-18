from google.cloud import storage
import psycopg2
import pandas as pd
import os
import xml.etree.ElementTree as ET
import xml.dom.minidom
from xml.dom import minidom
import pycountry
from unidecode import unidecode
import pycountry
from lxml import etree
import functions_framework
from google.cloud import secretmanager


# Lista de Paises en los cuales trabaja BIGBOX
Country = [ 'Argentina','Chile','Uruguay','Perú','España','México']

# Lista de columnas del la Tabla
#aux_columnes sera el paramtero de mensaje

##
meta_columns  =  ['id', 'title', 'description', 'link', 'image_link', 'availability', 'condition', 'price', 'gtin', 'brand', 'mpn', 'google_product_category']
google_columns = ['id', 'title', 'description', 'link', 'image_link', 'availability', 'condition', 'price', 'gtin', 'brand', 'mpn', 'google_product_category']

QUERY = """
select 
	   bo.product_ptr_id::varchar(255)  as id,
	   bo.name as title, 
	   trim(replace(replace(replace(replace(replace(replace(replace(regexp_replace(bo.description, '<[^>]*>', '', 'g'),'&ntilde;','ñ'),'&aacute;','á'),'&eacute;','é'),'&iacute;','í'),'&oacute;','ó'),'&uacute;','ú'),'&nbsp;',' ')) as description, -- limpio tags html
		case 			
			when cc.i18n = 'ar' then concat('https://www.bigbox.com.ar/regalos/',category.slug,'/', bo.slug,'/')
			when cc.i18n = 'cl' then concat('https://www.bigbox.cl/regalos/',category.slug,'/', bo.slug,'/')
			when cc.i18n = 'mx' then concat('https://www.bigbox.com.mx/regalos/',category.slug,'/', bo.slug,'/') 
			when cc.i18n = 'pe' then concat('https://www.bigbox.com.pe/regalos/',category.slug,'/', bo.slug,'/')
			when cc.i18n = 'uy' then concat('https://www.bigbox.com.uy/regalos/',category.slug,'/', bo.slug,'/')
			when cc.i18n = 'es' then concat('https://www.bigbox.es/regalos/',category.slug,'/', bo.slug,'/')
		end as link,
	  	concat('https://web-bigbox.storage.googleapis.com/',image_new_box_physical) as image_link,
	   'in stock' as availability,
	   'new' as "condition",
	    case 
			when bo.use_price_in_usd = true then concat(cast(bo.price_in_usd as text),' USD') 
			when cc.i18n = 'co' then concat(cast(bo.price_colombia as text),' COP') 
			when cc.i18n = 'ar' then concat(cast(bo.price_argentina as text),' ARS') 
			when cc.i18n = 'cl' then concat(cast(bo.price_chile as text),' CLP') 
			when cc.i18n = 'mx' then concat(cast(bo.price_mexico as text),' MXN') 
			when cc.i18n = 'pe' then concat(cast(bo.price_peru as text),' PEN') 
			when cc.i18n = 'uy' then concat(cast(bo.price_uruguay as text),' UYU') 
			when cc.i18n = 'es' then concat(cast(bo.price_spain as text), ' EUR') 
		end as price,
		'' as gtin,
		'Bigbox' as brand,		
		'' as mpn,
	   'Arts & Entertainment > Party & Celebration > Gift Giving > Gift Cards & Certificates' as google_product_category
	   --,	   'FALSE' as identifier_exists
from public.bigbox_box bo
JOIN public.core_country cc -- pais
		ON bo.country_id = cc.id
JOIN core_category as category  -- categorias
   ON category.id = bo.category_id
where bo.available = true
and bo.digital_version = true
and (bo.expiration_date is null or bo.expiration_date > current_date)
AND cc.name = %s
union all
-- actividades de una box para catalogo de Google
select  
	   concat(ca.product_ptr_id,'-',bo.name) as id,
	--concat(ca.name,' - ',bo.name)
	   ca.name  as title, 
	   trim(replace(replace(replace(replace(replace(replace(replace(regexp_replace(ca.short_description, '<[^>]*>', '', 'g'),'&ntilde;','ñ'),'&aacute;','á'),'&eacute;','é'),'&iacute;','í'),'&oacute;','ó'),'&uacute;','ú'),'&nbsp;',' ')) as description, -- limpio tags html
		case 			
			when cc.i18n = 'ar' then concat('https://www.bigbox.com.ar/regalos/',category.slug,'/', bo.slug,'/', ca.slug,'/')
			when cc.i18n = 'cl' then concat('https://www.bigbox.cl/regalos/',category.slug,'/', bo.slug,'/', ca.slug,'/')
			when cc.i18n = 'mx' then concat('https://www.bigbox.com.mx/regalos/',category.slug,'/', bo.slug,'/', ca.slug,'/')
			when cc.i18n = 'pe' then concat('https://www.bigbox.com.pe/regalos/',category.slug,'/', bo.slug,'/', ca.slug,'/')
			when cc.i18n = 'uy' then concat('https://www.bigbox.com.uy/regalos/',category.slug,'/', bo.slug,'/', ca.slug,'/')
			when cc.i18n = 'es' then concat('https://www.bigbox.es/regalos/',category.slug,'/', bo.slug,'/', ca.slug,'/')
		end as link,
		concat('https://web-bigbox.storage.googleapis.com/',imagen.image) as image_link,
	   'in stock' as availability,
	   'new' as "condition",
	    case 
			when bo.use_price_in_usd = true then concat(cast(bo.price_in_usd as text),' USD') 
			when cc.i18n = 'co' then concat(cast(bo.price_colombia as text),' COP') 
			when cc.i18n = 'ar' then concat(cast(bo.price_argentina as text),' ARS') 
			when cc.i18n = 'cl' then concat(cast(bo.price_chile as text),' CLP') 
			when cc.i18n = 'mx' then concat(cast(bo.price_mexico as text),' MXN') 
			when cc.i18n = 'pe' then concat(cast(bo.price_peru as text),' PEN') 
			when cc.i18n = 'uy' then concat(cast(bo.price_uruguay as text),' UYU') 
			when cc.i18n = 'es' then concat(cast(bo.price_spain as text), ' EUR') 
		end as price,
		'' as gtin,
		'Bigbox' as brand,		
		'' as mpn,					
	   'Arts & Entertainment > Party & Celebration > Gift Giving > Gift Cards & Certificates' as google_product_category
	   --,	   'FALSE' as identifier_exists
from public.bigbox_box bo
join public.bigbox_box_activities ba
	on bo.product_ptr_id = ba.box_id
join public.core_activity ca
	on ca.product_ptr_id = ba.activity_id
	and ca.available = true
	and ca.visible_catalog = true
	and (ca.dead_date is null or ca.dead_date<= current_date)
JOIN public.core_country cc -- pais
		ON bo.country_id = cc.id
JOIN core_category as category  -- categorias
   ON category.id = bo.category_id
left join lateral ( select img.image
				    from public.core_activityimage as img
					where img.activity_id = ca.product_ptr_id
				    order by img.order asc
				    limit 1
				  ) imagen -- puede traer mas de un registro por eso uso limit
	on true
where (ca.expiration_date is null or ca.expiration_date<= current_date)
and bo.available = true
and (bo.fisical_version = true or bo.digital_version = true)
and (ca.short_description is not null and trim(ca.short_description)!='')
and ca.score_satisfaction >= 4.5 -- puntaje actividad
AND cc.name = %s
order by 1, 3
"""



conn = psycopg2.connect(
    host="104.199.38.127",
    database="main",
    user="bi_only_read",
    password="f*BzeK]Jgre~8hx.Y9J[3##-/;"
)

def excec_query(country,consulta,aux_columns):
    #Crea un cursor
    cursor = conn.cursor()

    # Ejecutar la consulta
    cursor.execute(consulta, (country, country))
    
    # Obtener los resultados
    resultados = cursor.fetchall()
    # Convertir los resultados en un DataFrame
    df = pd.DataFrame(resultados, columns=aux_columns)
    # Cerrar el cursor y la conexión
    cursor.close()
    return df

def indent_xml(file_path):
    doc = xml.dom.minidom.parse(file_path)
    pretty_xml = doc.toprettyxml(indent="  ", encoding="utf-8")
    
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(pretty_xml.decode("utf-8"))  # Convertir bytes a cadena antes de escribir



def generate_xml_from_dataframe(df,file_to_create, country):
    # Crear el elemento raíz del XML
    if file_to_create == 'Meta':
        root = ET.Element('rss')
        root.set('xmlns:g', 'http://base.google.com/ns/1.0')
        root.set('version', '2.0')
        root.set('xmlns:atom', 'http://www.w3.org/2005/Atom')
    else:
        if file_to_create == 'Google':
            root = ET.Element('rss')
            root.set('xmlns:g', 'http://base.google.com/ns/1.0')
            root.set('version', '2.0')            

    # Crear el elemento 'channel'
    channel = ET.SubElement(root, 'channel')

    # Agregar el título, enlace y descripción
    title = ET.SubElement(channel, 'title')
    title.text = f'Bigbox - {country}'
     
    if country == 'España':
        country_obj = pycountry.countries.get(alpha_2='ES')
    else:
        country_obj = pycountry.countries.search_fuzzy(country)[0]
    
    link = ET.SubElement(channel, 'link')
    if country in ['Uruguay','Perú','Argentina','México']:
        link.text = f'www.bigbox.com.{country_obj.alpha_2.lower()}'
    else:
        link.text = f'www.bigbox.{country_obj.alpha_2.lower()}' 

    description = ET.SubElement(channel, 'description')
    description.text = "This is a sample feed containing the required and recommended attributes for a variety of different products"


    # Filtrar los elementos vacíos del DataFrame
    df_filtered = df.dropna(subset=df.columns, how='all')

    # Agregar los elementos 'item' del DataFrame filtrado
    for _, row in df_filtered.iterrows():
        item = ET.SubElement(channel, 'item')

        # Agregar las columnas como elementos dentro de 'item'
        for column in df.columns:
            element = ET.SubElement(item, f"g:{column}")
            element.text = str(row[column])

    # Crear el objeto ElementTree
    tree = ET.ElementTree(root)
     
    xml_string = ET.tostring(tree.getroot(), encoding='utf-8')

    # Crear el objeto minidom
    dom = xml.dom.minidom.parseString(xml_string)

    # Obtener el XML indentado
    pretty_xml_string = dom.toprettyxml(indent="  ")
    
    return pretty_xml_string

def create_XML(event, file_to_create, aux_columns, bucket_name):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    for aux_country in Country:
        print(aux_country)
        print('')
        df = excec_query(aux_country, event, aux_columns)
            
        # Guardar el XML en un archivo
        nombre_archivo = f"{file_to_create}_{unidecode(aux_country)}.xml"
        
        content_file = generate_xml_from_dataframe(df, file_to_create, aux_country) 

       
        
        # Crea un nuevo objeto de archivo en el bucket
        blob = bucket.blob(nombre_archivo)

        # Carga el contenido al objeto de archivo
        blob.upload_from_string(content_file, content_type='application/xml')

        print(f"Archivo {nombre_archivo} subido a {bucket_name}")


@functions_framework.http
def main(request):
    client = storage.Client()
    # Crea una instancia del cliente de Secret Manager
    secret_client = secretmanager.SecretManagerServiceClient()

    # Define el nombre completo del secreto
    secret_name = "projects/bi-main-335421/secrets/perm-gcp/versions/latest"  

    # Accede al valor del secreto
    response = secret_client.access_secret_version(request={"name": secret_name})
    secret_payload = response.payload.data.decode("UTF-8") 

    conn = psycopg2.connect(
        host="127.190.34.138",
        database="postgres",
        user="postgres",
        password=secret_payload
    )

    bucket = 'xml_meta_gogle'

    # Creamos los archivos de Google y Meta
    for msg in [ 'Google','Meta' ]:
        if msg == 'Google':
            # Creamos los archivos de Google
            create_XML(QUERY,msg,google_columns,bucket)
        else:
            if msg == 'Meta':
                # Creamos los archivos de Meta
                create_XML(QUERY,msg,meta_columns,bucket)
    conn.close()