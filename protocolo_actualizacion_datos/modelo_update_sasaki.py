import geopandas as gpd
import pandas as pd
import requests
import math
import numpy
from datetime import datetime
from shapely import wkt
from shapely.errors import WKTReadingError
from shapely.geometry import mapping, shape
from shapely import Polygon
import warnings
warnings.filterwarnings("ignore")
import re
import numpy as np
from shapely.validation import make_valid
import os


def safe_int(value, default=None):
    """
    Convierte un valor a int de manera segura.
    - Si es NaN, None o no convertible, devuelve `default`.
    - Si es float convertible a entero, castea.
    """
    try:
        if pd.isna(value):
            return default
        return int(value)
    except (ValueError, TypeError):
        return default
    
def subir_fotos(ruta_foto, url_base, folder_base):
    # Validar que exista foto
    if ruta_foto is None or pd.isna(ruta_foto):
        return None
    
    ruta_archivo = os.path.join(folder_base, ruta_foto)

    # Validar que el archivo exista realmente
    if not os.path.isfile(ruta_archivo):
        print(f"[WARN] Archivo de foto no encontrado: {ruta_archivo}")
        return None

    url_fotos = f"{url_base}api/v1/subir-foto/"

    with open(ruta_archivo, "rb") as f:
        files = {
            "archivo": (ruta_foto, f, "image/jpeg")
        }
        response = requests.post(url_fotos, files=files, verify=False)

    if response.status_code == 201:
        return response.json().get("url")

    print(f"[WARN] No se pudo subir la foto {ruta_foto}. Status: {response.status_code}")
    return None

def limpiar_nan(obj):
    if isinstance(obj, dict):
        return {k: limpiar_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [limpiar_nan(v) for v in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    else:
        return obj

def compiler_info_productor(nombre):
    objeto_productor = {
        "activo":False,
        "delegado": False,
        "nombre_completo": nombre,
        "fecha_afiliacion": str(datetime.now().strftime("%Y-%m-%d")),
    }
    return objeto_productor
    

def reportar_lotes(row):
    if bool(row['otro_asoc']):
        if not pd.isna(row["doc_prod"]):
            documento= safe_int(row["doc_prod"])
        else:
            documento=None
    else:
        if not pd.isna(row["documento"]):
            documento= safe_int(row["documento"])
        else:
            documento=None

    response = {
        "documento_productor":documento,
        "poligono":str(row["geometry"]),
        "mongo_atribute":{
            "numero_documento": str(documento),
            "nombre_productor": str(row['nom_prod']),
            "fecha_visita": pd.to_datetime(row['fecha_visi']).strftime("%Y-%m-%d") if pd.notnull(row['fecha_visi']) else None,
            "area": row['area'],
            "observaciones": "",
            "descripcion": "",
            "numero_lote": row['numero_lot'],
            "descripcion_lote": "",
            "variedad": row['variedad'],
            "distancia_surcos": row['distancia_'],
            "distancia_plantas": row['distanci_1'],
            "densidad": row['densidad'],
            "numero_plantas": row['numero_pla'],
            "gramos_plantas": row['gramos_pla'],
            "kg_produccion": row['kg_producc'],
            "fecha_actividad": pd.to_datetime(row['fecha_acti']).strftime("%Y-%m-%d") if pd.notnull(row['fecha_acti']) else None,
            "produccion": row['produccion'],
            "estado_cultivo":row['estado_cul'],
            "subtipo_operacion": row['subtipo_op']
        },
        "productor_data":{}
    }

    if bool(row['otro_asoc']):
        if not pd.isna(row["nom_prod"]):
            nombre = str(row["nom_prod"])
            info_productor = compiler_info_productor(nombre)
            response["productor_data"] = info_productor

    response = limpiar_nan(response)
    return response

def reportar_fincas(row):
    if bool(row['otro_asoc']):
        if not pd.isna(row["doc_prod"]):
            documento= safe_int(row["doc_prod"])
        else:
            documento=None
    else:
        if not pd.isna(row["documento"]):
            documento= safe_int(row["documento"])
        else:
            documento=None
    response = {
        "documento_productor":documento,
        "poligono":str(row["geometry"]),
        "mongo_atribute":{
            "documento":str(documento),
            "nombre_productor":str(row["nom_prod"]),
            "area": row['area'],
            "nombre_finca": row['nombre_fin'],
            "fecha_visita": pd.to_datetime(row['fecha_visi']).strftime("%Y-%m-%d") if pd.notnull(row['fecha_visi']) else None
        },
        "productor_data":{}
    }
    
    if bool(row['otro_asoc']):
        if not pd.isna(row["nom_prod"]):
            nombre = str(row["nom_prod"])
            info_productor = compiler_info_productor(nombre)
            response["productor_data"] = info_productor
    
    response = limpiar_nan(response)
    return response

def reportar_conservacion(row):
    if bool(row['Asociado']):
        if not pd.isna(row["doc_prod"]):
            documento= safe_int(row["doc_prod"])
        else:
            documento=None
    else:
        if not pd.isna(row["doc_aso"]):
            documento= safe_int(row["doc_aso"])
        else:
            documento=None

    response = {
        "documento_productor":documento,
        "poligono":str(row["geometry"]),
        "mongo_atribute":{
            "documento":str(documento),
            "nombre_productor":str(row["nom_prod"]),
            "area": row['area'],
            "tipo_arboles":row['tipo_arb'],
            "fecha_visita": pd.to_datetime(row['fecha_vis']).strftime("%Y-%m-%d") if pd.notnull(row['fecha_vis']) else None
        },
        "productor_data":{}
    }

    if bool(row['Asociado']):
        if not pd.isna(row["nom_prod"]):
            nombre = str(row["nom_prod"])
            info_productor = compiler_info_productor(nombre)
            response["productor_data"] = info_productor
    
    response = limpiar_nan(response)
    
    return response

def reportar_infraestructura(row):
    if bool(row['Asociado']):
        if not pd.isna(row["doc_prod"]):
            documento= safe_int(row["doc_prod"])
        else:
            documento=None
    else:
        if not pd.isna(row["doc_aso"]):
            documento= safe_int(row["doc_aso"])
        else:
            documento=None
    response = {
        "documento_productor":documento,
        "poligono":str(row["geometry"]),
        "mongo_atribute":{
            "documento":str(documento),
            "nombre_productor":str(row["nom_prod"]),
            "area": row['area'],
            "fecha_visita": pd.to_datetime(row['fecha_vis']).strftime("%Y-%m-%d") if pd.notnull(row['fecha_vis']) else None,
            "tipo_estructura": row['tipo_estr'],
            "estructura": row['estruc_sel']
        },
        "productor_data":{}
    }

    if bool(row['Asociado']):
        if not pd.isna(row["nom_prod"]):
            nombre = str(row["nom_prod"])
            info_productor = compiler_info_productor(nombre)
            response["productor_data"] = info_productor

    response = limpiar_nan(response)

    return response

def actualizar_fincas_existentes(row):
    if bool(row['otro_asoc']):
        if not pd.isna(row["doc_prod"]):
            documento= safe_int(row["doc_prod"])
        else:
            documento=None
    else:
        if not pd.isna(row["documento"]):
            documento= safe_int(row["documento"])
        else:
            documento=None
    respuesta={
        "poligono":str(row["geometry"]),
        "mongo_atribute":{
            "documento":str(documento),
            "nombre_finca":row["nombre_fin"],
            "fecha_visita":pd.to_datetime(row['fecha_visi']).strftime("%Y-%m-%d") if pd.notnull(row['fecha_visi']) else None,
            "area":row["area"]
        },
        "productor_data":{},
        "documento_productor": documento,
    }
    
    if bool(row['otro_asoc']):
        if not pd.isna(row["nom_prod"]):
            nombre = str(row["nom_prod"])
            info_productor = compiler_info_productor(nombre)
            respuesta["productor_data"] = info_productor

    response=limpiar_nan(respuesta)
    return response

def actualizar_lotes_existentes(row):
    if bool(row['otro_asoc']):
        if not pd.isna(row["doc_prod"]):
            documento= safe_int(row["doc_prod"])
        else:
            documento=None
    else:
        if not pd.isna(row["documento"]):
            documento= safe_int(row["documento"])
        else:
            documento=None
    respuesta = {
        "poligono":str(row["geometry"]),
        "mongo_atribute":{
            "nombre_productor": str(documento),
            "fecha_visita": pd.to_datetime(row['fecha_visi']).strftime("%Y-%m-%d") if pd.notnull(row['fecha_visi']) else None,
            "area": row['area'],
            "observaciones": "",
            "descripcion": "",
            "numero_lote": row['numero_lot'],
            "descripcion_lote": "",
            "variedad": row['variedad'],
            "distancia_surcos": row['distancia_'],
            "distancia_plantas": row['distanci_1'],
            "densidad": row['densidad'],
            "numero_plantas": row['numero_pla'],
            "gramos_plantas": row['gramos_pla'],
            "kg_produccion": row['kg_producc'],
            "fecha_actividad": pd.to_datetime(row['fecha_acti']).strftime("%Y-%m-%d") if pd.notnull(row['fecha_acti']) else None,
            "produccion": row['produccion'],
            "estado_cultivo":row['estado_cul'],
            "subtipo_operacion": row['subtipo_op']
        },
        "productor_data":{},
        "documento_productor": documento,
    }
    if bool(row['otro_asoc']):
        if not pd.isna(row["nom_prod"]):
            nombre = str(row["nom_prod"])
            info_productor = compiler_info_productor(nombre)
            respuesta["productor_data"] = info_productor
            
    respuesta = limpiar_nan(respuesta)
    
    return respuesta

def buscar_productor(cedula, df_productor):
    print(cedula)
    # Convertir a entero de forma segura
    cedula_int = int(cedula)

    # Convertir la columna 'documento' a entero (ignorando errores)
    df_productor["documento_int"] = pd.to_numeric(df_productor["documento"], errors="coerce").astype("Int64")

    # Filtrar comparando como enteros
    df_filtro = df_productor.loc[df_productor["documento_int"] == cedula_int]

    return len(df_filtro) > 0

def preparar_json_fotos(row, cols_foto, url_base, folder_base):
    attachments_diccionario = {}
    for columna in cols_foto:
        valor = row[columna]
        if pd.isna(valor):
            continue
        attachments_diccionario[columna] = subir_fotos(valor, url_base, folder_base)
    
    return attachments_diccionario




def reporte_coronel(gdf, tipo, metodo_reporte, session, capas_gdf_dict, url_base, folder_base):
    respuestas_exitosas_lote = []
    respuestas_no_exitosas_lote = []
    cols_foto = [c for c in gdf.columns if c.lower().startswith("foto")]
    for i, row in gdf.iterrows():    
        respuesta = metodo_reporte(row)
        attachments = {}
        if len(cols_foto) > 0:
            attachments = preparar_json_fotos(row, cols_foto, url_base, folder_base)
        respuesta["attachments"] = attachments
        response = session.post(capas_gdf_dict[tipo], json=respuesta, verify=False)
        if response.ok:
            try:
                data = response.json()
                if "id" in data or data.get("status") == "success":
                    respuestas_exitosas_lote.append(data)
            except Exception as e:
                print(f"Error al procesar JSON en fila {i} del tipo {tipo}: {e}")
        else:
            try:
                data = response.json()
            except ValueError:
                data = {"status_code": response.status_code, "text": response.text}
            data["fila"] = i
            data["tipo"] = tipo
            data["peticion"] = respuesta
            respuestas_no_exitosas_lote.append(data)
            print(f"Error en fila {i} tipo {tipo}: status {response.status_code}, contenido: {response.text[:200]}")

    df_exitoso = pd.DataFrame(respuestas_exitosas_lote)
    df_no_exitoso = pd.DataFrame(respuestas_no_exitosas_lote)

    with pd.ExcelWriter(os.path.join(folder_base,f'{tipo}.xlsx'), engine='openpyxl') as writer:
        df_exitoso.to_excel(writer, sheet_name='rep_suc', index=False)
        df_no_exitoso.to_excel(writer, sheet_name='rep_not_suc', index=False)


def actualizacion_coronel(gdf, tipo, metodo_reporte, session, capas_gdf_dict,url_base, folder_base):
    respuestas_exitosas_lote = []
    respuestas_no_exitosas_lote = []
    cols_foto = [c for c in gdf.columns if c.lower().startswith("foto")]
    gdf = gdf.loc[gdf["actualizar"] == 1]
    url = capas_gdf_dict[tipo]
    gdf = gdf.rename(columns={'id_1': 'id'})
    for i, row in gdf.iterrows():
        if pd.isna(row["id"]):
            respuestas_exitosas_lote.append({"objeto":row, "razon": "lote creado en capa erronea"})
        else:
            fecha_ahora = datetime.now()
            fecha_formateada = fecha_ahora.strftime("%d-%m-%Y_%H--%M--%S")
            url_actualizacion = f"{url}update/mongo_update/{row['id']}/{fecha_formateada}/"
            respuesta = metodo_reporte(row)
            atributos = respuesta
            documento = respuesta["documento_productor"]
            productor_data = respuesta["productor_data"]
            response = session.post(url_actualizacion, json=respuesta, verify=False)

            url_actualizacion = f"{url}{row['id']}/"
            respuesta = {"poligono": str(row["geometry"])}
            if documento is not None:
                respuesta["documento_productor"] = int(documento)
                respuesta["productor_data"] = productor_data

            attachments = {}
            if len(cols_foto) > 0:
                attachments = preparar_json_fotos(row, cols_foto, url_base, folder_base)
            respuesta["attachments"] = attachments
            response = session.patch(url_actualizacion, json=respuesta, verify=False)
            respuesta["id"] = row["id"]
            if response.ok:
                respuestas_exitosas_lote.append(respuesta)
            else:
                try:
                    data = response.json()
                except ValueError:
                    data = {"status_code": response.status_code, "text": response.text}
                data["fila"] = i
                data["tipo"] = tipo
                respuesta["mongo_atribute"] = atributos["mongo_atribute"]
                if data.get("detail") == "Not found.":
                    session.post(url, json=respuesta, verify=False)
                else:
                    data["peticion"] = respuesta
                    respuestas_no_exitosas_lote.append(data)
                    print(f"Error en fila {i} tipo {tipo}: status {response.status_code}, contenido: {response.text[:200]}")

    df_exitoso = pd.DataFrame(respuestas_exitosas_lote)
    df_no_exitoso = pd.DataFrame(respuestas_no_exitosas_lote)

    with pd.ExcelWriter(os.path.join(folder_base,f'{tipo}_act.xlsx'), engine='openpyxl') as writer:
        df_exitoso.to_excel(writer, sheet_name='rep_suc', index=False)
        df_no_exitoso.to_excel(writer, sheet_name='rep_not_suc', index=False)

def eliminacion_coronel(gdf, tipo, session, url_base, folder_base):
    respuestas_exitosas_lote = []
    respuestas_no_exitosas_lote = []
    for i, row in gdf.iterrows():
        if row["id"] != 1 or row["id"] != '1':
            response = session.delete(f"{url_base}api/v1/{tipo}/{row['id']}", verify=False)
            if response.ok:
                try:
                    data = response.json()
                    if "id" in data or data.get("status") == "success":
                        respuestas_exitosas_lote.append(data)
                except Exception as e:
                    print(f"Error al procesar JSON en fila {i} del tipo {tipo}: {e}")
            else:
                try:
                    respuestas_no_exitosas_lote.append(response.json())
                except ValueError:
                    # Si no es JSON, guardar el texto de la respuesta
                    respuestas_no_exitosas_lote.append({"status_code": response.status_code, "text": response.text, "peticion": {"tipo": tipo, "id": row["id"]}})

    df_exitoso = pd.DataFrame(respuestas_exitosas_lote)
    df_no_exitoso = pd.DataFrame(respuestas_no_exitosas_lote)

    with pd.ExcelWriter(os.path.join(folder_base,f'{tipo}_eliminados.xlsx'), engine='openpyxl') as writer:
        df_exitoso.to_excel(writer, sheet_name='rep_suc', index=False)
        df_no_exitoso.to_excel(writer, sheet_name='rep_not_suc', index=False)



def execute_model(folder_base, url_base):

    folder_individuales = os.path.join(folder_base, "shp")
    archivos_individuales = [os.path.join(folder_individuales, archivo) for archivo in os.listdir(folder_individuales) if archivo.endswith(".shp")]

    capas_gdf_dict = {
        "poligonos_fincas": f"{url_base}api/v1/poligonos_fincas/",
        "poligonos_lotes": f"{url_base}api/v1/poligonos_lotes/",
        "poligonos_infraestructura": f"{url_base}api/v1/poligonos_infraestructura/",
        "poligonos_conservacion": f"{url_base}api/v1/poligonos_conservacion/"
    }

    login = {
        "username":"Dorito",
        "password":"Portador123"
    }
    # Crear una sesión para mantener la cookie
    session = requests.Session()
    response = session.post(f"{url_base}api/user/login/", login, verify=False)
    capas = {}

    for archivo in archivos_individuales:
        nombre_elemento = os.path.basename(archivo)[:-4]
        nombre_elemento = nombre_elemento.lower().replace(" ", "_")
        capas[nombre_elemento] = gpd.read_file(archivo)
        print(f"creada_instancia: {nombre_elemento}")

    finca = capas.get("finca")
    lote = capas.get("lote")
    conservacion = capas.get("conservacion")
    construcciones = capas.get("construcciones")


    finca_del = finca.loc[finca["deshabilit"] == 1]
    lote_del = lote.loc[lote["deshabilit"] == 1]
    finca_act = finca.loc[finca["actualizar"] == 1]
    lote_act = lote.loc[lote["actualizar"] == 1]
    finca_add = finca.loc[finca["id"] == '1']
    lote_add = lote.loc[lote["id"] == '1']

    finca_add.to_file(os.path.join(folder_base,"fincas_a_adicionar.shp"))
    lote_add.to_file(os.path.join(folder_base,"lotes_a_adicionar.shp"))


    eliminacion_coronel(finca_del, "poligonos_fincas", session,url_base, folder_base)
    eliminacion_coronel(lote_del, "poligonos_lotes", session, url_base,folder_base)
    reporte_coronel(finca_add, "poligonos_fincas", reportar_fincas, session, capas_gdf_dict, url_base, folder_base)
    reporte_coronel(lote_add, "poligonos_lotes", reportar_lotes, session, capas_gdf_dict, url_base, folder_base)
    reporte_coronel(conservacion, "poligonos_conservacion", reportar_conservacion, session, capas_gdf_dict, url_base, folder_base)
    reporte_coronel(construcciones, "poligonos_infraestructura", reportar_infraestructura, session, capas_gdf_dict, url_base, folder_base)
    actualizacion_coronel(finca_act, "poligonos_fincas", actualizar_fincas_existentes, session, capas_gdf_dict, url_base, folder_base)
    actualizacion_coronel(lote_act, "poligonos_lotes", actualizar_lotes_existentes, session, capas_gdf_dict, url_base, folder_base)