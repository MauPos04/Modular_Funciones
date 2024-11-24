# Usuario_Y_Cafeteria.py

import boto3
import pandas as pd
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import webbrowser
from io import BytesIO
import json
import numpy as np
import os
import openpyxl
from datetime import datetime, timedelta
from decimal import Decimal
import warnings
import locale

# Importaciones de módulos personalizados
from config import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY
from Conversion import convertir_completo
from id_a_cafeteria import renombrar_hojas

# Importar las nuevas funciones para exportar órdenes completadas filtradas
from exceles_completados_filtrados import export_ordenes_completadas_por_mes, export_ordenes_completadas_rango_fecha
from excel_clasificacion_cafeterias import export_ordenes_completadas_por_cafeteria

# Ignorar FutureWarnings para aplicar cambios gradualmente
warnings.simplefilter(action='ignore', category=FutureWarning)

# Establecer el locale a español para obtener nombres de meses en español
try:
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
except locale.Error:
    # Si el locale no está disponible, usar el estándar
    locale.setlocale(locale.LC_TIME, '')

# ================================================
# Configuración y Escaneo de AWS DynamoDB
# ================================================

def configurar_aws():
    """
    Configura la conexión a AWS DynamoDB.

    Returns:
        dynamodb (boto3.resource): Recurso de DynamoDB.
        tablas (dict): Diccionario con los nombres de las tablas.
    """
    tablas = {
        # Tablas del primer script
        'ordenes': 'colosal-appu-ordenes-pdn',
        'usuarios_app': 'colosal-appu-usuarios-app-pdn',
        'usuarios': 'colosal-appu-usuarios-pdn',
        'cafeterias': 'colosal-appu-cafeterias-pdn',
        # Tablas del segundo script
        'ingredientes': 'colosal-appu-ingredientes-pdn',
        'instituciones': 'colosal-appu-instituciones-pdn',
        'productos': 'colosal-appu-productos-pdn'
    }

    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    return dynamodb, tablas

def escanear_tabla(dynamodb, table_name, limit=None):
    """
    Escanea una tabla de DynamoDB y recupera los ítems.

    Args:
        dynamodb (boto3.resource): Recurso de DynamoDB.
        table_name (str): Nombre de la tabla a escanear.
        limit (int, optional): Límite de ítems a recuperar. Defaults to None.

    Returns:
        list: Lista de ítems recuperados de la tabla.
    """
    table = dynamodb.Table(table_name)
    items = []
    total_scanned = 0

    try:
        # Parámetros iniciales
        scan_kwargs = {
            'ReturnConsumedCapacity': 'TOTAL'
        }

        if limit:
            scan_kwargs['Limit'] = limit

        done = False
        start_key = None

        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key

            try:
                response = table.scan(**scan_kwargs)
                items.extend(response.get('Items', []))
                total_scanned += len(response.get('Items', []))

                # Imprimir información de progreso
                print(f"Escaneados {len(response.get('Items', []))} items de {table_name}")
                print(f"Capacidad consumida: {response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)} unidades")

                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None

                if limit and total_scanned >= limit:
                    print(f"Alcanzado el límite de {limit} items")
                    break

            except Exception as e:
                print(f"Error durante el escaneo de la tabla {table_name}: {str(e)}")
                print(f"Último ExclusiveStartKey: {start_key}")
                # Intentar continuar desde el último punto conocido
                continue

        print(f"Scan completado. Total de items recuperados: {len(items)}")
        return items

    except Exception as e:
        print(f"Error fatal al escanear la tabla {table_name}: {str(e)}")
        print(f"Items recuperados antes del error: {len(items)}")
        raise e

# ================================================
# Funciones de Procesamiento de Datos
# ================================================

def filter_dataframe(df, search_term):
    """
    Filtra un DataFrame basado en un término de búsqueda global.

    Args:
        df (pd.DataFrame): DataFrame a filtrar.
        search_term (str): Término de búsqueda.

    Returns:
        pd.DataFrame: DataFrame filtrado.
    """
    if search_term:
        filtered_df = df[
            df.astype(str).apply(
                lambda x: x.str.contains(str(search_term), case=False, na=False)
            ).any(axis=1)
        ]
        return filtered_df
    return df

def convertir_decimal(x):
    """
    Convierte objetos Decimal a float.

    Args:
        x (any): Valor a convertir.

    Returns:
        any: Valor convertido.
    """
    if isinstance(x, Decimal):
        return float(x)
    elif isinstance(x, list):
        return [convertir_decimal(i) for i in x]
    elif isinstance(x, dict):
        return {k: convertir_decimal(v) for k, v in x.items()}
    else:
        return x

def process_ordenes_data(df_ordenes):
    """
    Procesa el DataFrame de órdenes.

    Args:
        df_ordenes (pd.DataFrame): DataFrame de órdenes sin procesar.

    Returns:
        pd.DataFrame: DataFrame de órdenes procesado.
    """
    if not df_ordenes.empty:
        # Convertir columnas numéricas
        df_ordenes['monto'] = pd.to_numeric(df_ordenes['monto'], errors='coerce')
        df_ordenes['tasa'] = pd.to_numeric(df_ordenes['tasa'], errors='coerce')

        # Convertir columnas de fecha/hora a datetime sin especificar formato para inferencia automática
        df_ordenes['fecha_creacion_dt'] = pd.to_datetime(df_ordenes['fecha_creacion'], errors='coerce')
        df_ordenes['fecha_terminacion_dt'] = pd.to_datetime(df_ordenes['fecha_terminacion'], errors='coerce')
        df_ordenes['hora_recogida_dt'] = pd.to_datetime(df_ordenes['hora_recogida'], errors='coerce').dt.time

        # Crear columna 'hora_creacion' extrayendo la hora
        df_ordenes['hora_creacion'] = df_ordenes['fecha_creacion_dt'].dt.strftime('%H:%M:%S')

        # Crear columnas de cadena formateadas para visualización
        df_ordenes['fecha_creacion_str'] = df_ordenes['fecha_creacion_dt'].dt.strftime('%d/%m/%Y')
        df_ordenes['fecha_terminacion_str'] = df_ordenes['fecha_terminacion_dt'].dt.strftime('%d/%m/%Y')
        df_ordenes['hora_recogida_str'] = df_ordenes['hora_recogida_dt'].astype(str)

        # Añadir nuevas columnas según instrucciones
        # Nuevas columnas basadas en 'monto'
        df_ordenes['VALOR COMISION Monto'] = (df_ordenes['monto'] * 0.02).round(3)
        df_ordenes['VALOR RETEFUENTE APPU Monto'] = (df_ordenes['monto'] * 0.015).round(3)
        df_ordenes['VALOR RTE ICA APPU Monto'] = (df_ordenes['monto'] * 0.005).round(3)
        df_ordenes['VALOR NETO Monto'] = (
            df_ordenes['monto'] - 
            df_ordenes['VALOR COMISION Monto'] - 
            df_ordenes['VALOR RETEFUENTE APPU Monto'] - 
            df_ordenes['VALOR RTE ICA APPU Monto']
        ).round(3)

        # APPU (basado en 'tasa')
        df_ordenes['VALOR COMISION APPU'] = (df_ordenes['tasa'] * 0.02).round(3)
        df_ordenes['VALOR RETEFUENTE APPU'] = (df_ordenes['tasa'] * 0.015).round(3)
        df_ordenes['VALOR RTE ICA APPU'] = (df_ordenes['tasa'] * 0.005).round(3)

        # CAFETERIA
        df_ordenes['VALOR PRODUCTO'] = (df_ordenes['monto'] - df_ordenes['tasa']).round(3)
        df_ordenes['VALOR COMISION CAFETERIA'] = (df_ordenes['VALOR PRODUCTO'] * 0.02).round(3)
        df_ordenes['COMISION APPU-CAFETERIA'] = (df_ordenes['VALOR PRODUCTO'] * 0.005).round(3)
        df_ordenes['COMISION-WOMPI'] = (df_ordenes['VALOR COMISION CAFETERIA'] + df_ordenes['COMISION APPU-CAFETERIA']).round(3)
        df_ordenes['VALOR RETEFUENTE CAFETERIA'] = (df_ordenes['VALOR PRODUCTO'] * 0.015).round(3)
        df_ordenes['VALOR RTE ICA CAFETERIA'] = (df_ordenes['VALOR PRODUCTO'] * 0.005).round(3)
        df_ordenes['VALOR NETO CAFETERIA'] = (
            df_ordenes['VALOR PRODUCTO'] - 
            df_ordenes['COMISION-WOMPI'] - 
            df_ordenes['VALOR RETEFUENTE CAFETERIA'] - 
            df_ordenes['VALOR RTE ICA CAFETERIA']
        ).round(3)

        df_ordenes['GANANCIA NETO APPU'] = (
            df_ordenes['tasa'] - 
            df_ordenes['VALOR COMISION APPU'] - 
            df_ordenes['VALOR RETEFUENTE APPU'] - 
            df_ordenes['VALOR RTE ICA APPU'] + 
            df_ordenes['COMISION APPU-CAFETERIA']
        ).round(3)

        # Ordenar el DataFrame por 'fecha_creacion_dt' y 'hora_recogida_dt'
        df_ordenes = df_ordenes.sort_values(by=['fecha_creacion_dt', 'hora_recogida_dt'], ascending=[False, False])

        # Definir el orden deseado de las columnas (incluyendo las nuevas columnas de visualización)
        desired_columns = [
            'id_orden',
            'documento_cliente',
            'nombre_cliente',
            'fecha_creacion_str',    # Usar la cadena formateada
            'hora_creacion',         # Ya existe
            'monto',
            'VALOR COMISION Monto',                # Nueva columna
            'VALOR RETEFUENTE APPU Monto',         # Nueva columna
            'VALOR RTE ICA APPU Monto',            # Nueva columna
            'VALOR NETO Monto',                    # Nueva columna
            'VALOR COMISION APPU',
            'VALOR RETEFUENTE APPU',
            'VALOR RTE ICA APPU',
            'GANANCIA NETO APPU',
            'VALOR PRODUCTO',
            'VALOR COMISION CAFETERIA',
            'COMISION APPU-CAFETERIA',
            'COMISION-WOMPI',
            'VALOR RETEFUENTE CAFETERIA',
            'VALOR RTE ICA CAFETERIA',
            'VALOR NETO CAFETERIA',
            'tasa',
            'cafeteria',
            'orden_completada',
            'hora_recogida_str',     # Usar la cadena formateada
            'productos_json',
            'para_llevar',
            'institucion',
            'fecha_terminacion_str', # Usar la cadena formateada
            'celular_cliente',
            'comprobante_pago',
            'observacion',
            'cafeteria_id'  # Campo adicional requerido
            # Añade aquí cualquier otro campo que desees mantener
        ]

        # Verificar que todas las columnas existan
        missing_columns = [col for col in desired_columns if col not in df_ordenes.columns]
        if missing_columns:
            print(f"Advertencia: Las siguientes columnas faltan en df_ordenes y se rellenarán con valores NaN: {missing_columns}")
            for col in missing_columns:
                df_ordenes[col] = np.nan

        # Reordenar las columnas del DataFrame según el orden deseado
        df_ordenes = df_ordenes[desired_columns]

        # Convertir 'productos_json' a string para Dash DataTable, manejando Decimals
        df_ordenes['productos_json'] = df_ordenes['productos_json'].apply(
            lambda x: json.dumps(convertir_decimal(x)) if isinstance(x, (dict, list)) else str(x)
        )

        # **Nueva Conversión para la Columna 'observacion'**
        # Asegurarse de que 'observacion' sea de tipo string, number o boolean
        df_ordenes['observacion'] = df_ordenes['observacion'].apply(
            lambda x: json.dumps(convertir_decimal(x)) if isinstance(x, (dict, list)) else (x if isinstance(x, (str, int, float, bool)) else str(x))
        )

    return df_ordenes

def process_products_data(df_ordenes):
    """
    Procesa el DataFrame de productos.

    Args:
        df_ordenes (pd.DataFrame): DataFrame de órdenes procesado.

    Returns:
        pd.DataFrame: DataFrame de productos.
    """
    if not df_ordenes.empty:
        # Procesar 'productos_json' para extraer las llaves requeridas
        df_ordenes_2 = df_ordenes[['id_orden', 'nombre_cliente', 'productos_json', 'fecha_creacion_str', 'hora_creacion', 'monto', 'VALOR PRODUCTO', 'VALOR NETO CAFETERIA']].copy()

        # Definir una función segura para cargar JSON
        def safe_json_loads(x):
            if isinstance(x, str):
                try:
                    return json.loads(x)
                except json.JSONDecodeError:
                    return []
            else:
                return []

        # Aplica la función segura a la columna 'productos_json'
        df_ordenes_2['productos_json'] = df_ordenes_2['productos_json'].apply(safe_json_loads)

        # Explode para tener una fila por cada producto
        df_exploded = df_ordenes_2.explode('productos_json')

        # Eliminar filas donde 'productos_json' es NaN
        df_exploded = df_exploded[df_exploded['productos_json'].notna()]

        # Expandir los diccionarios en columnas separadas
        df_products = pd.concat(
            [df_exploded.drop('productos_json', axis=1),
             df_exploded['productos_json'].apply(pd.Series)],
            axis=1
        )

        # Reemplazar NaN por valores vacíos o cero si es necesario
        df_products.fillna({'producto': '', 'cantidad': 0, 'precioUnitario': 0, 'precioTotal': 0}, inplace=True)

        # Convertir columnas numéricas - usando los nombres correctos del JSON
        df_products['cantidad'] = pd.to_numeric(df_products['cantidad'], errors='coerce').fillna(0).astype(int)
        df_products['precioUnitario'] = pd.to_numeric(df_products['precioUnitario'], errors='coerce').fillna(0).round(3)
        df_products['precioTotal'] = pd.to_numeric(df_products['precioTotal'], errors='coerce').fillna(0).round(3)

        # Reordenar columnas según lo especificado, incluyendo 'id_orden'
        desired_product_columns = ['id_orden', 'nombre_cliente', 'producto', 'cantidad', 'precioUnitario', 'precioTotal', 'fecha_creacion_str', 'hora_creacion', 'monto', 'VALOR PRODUCTO', 'VALOR NETO CAFETERIA']
        df_products = df_products[desired_product_columns]

    return df_products

def process_cafeterias_data(df_ordenes_completadas):
    """
    Procesa el DataFrame de órdenes para crear un resumen por cafeterías basándose en órdenes completadas.

    Args:
        df_ordenes_completadas (pd.DataFrame): DataFrame de órdenes completadas.

    Returns:
        pd.DataFrame: DataFrame con el resumen por cafeterías incluyendo nuevas columnas agregadas.
    """
    print("\nIniciando procesamiento de cafeterías...")

    # 1. Crear una copia del DataFrame original (ya filtrado)
    df_cafeterias = df_ordenes_completadas.copy()
    print(f"Registros totales en el DataFrame filtrado: {len(df_cafeterias)}")

    # 2. Obtener el mes actual
    current_date = datetime.now()
    first_day_current_month = current_date.replace(day=1)
    last_day_current_month = (first_day_current_month + pd.offsets.MonthEnd(0)).to_pydatetime()

    print(f"\nFecha de inicio: {first_day_current_month.strftime('%d/%m/%Y')}")
    print(f"Fecha de fin: {last_day_current_month.strftime('%d/%m/%Y')}")

    # 3. Aplicar filtros de rango de fechas para el mes actual
    df_cafeterias['fecha_creacion_dt'] = pd.to_datetime(df_cafeterias['fecha_creacion_str'], errors='coerce')

    df_cafeterias = df_cafeterias[
        (df_cafeterias['fecha_creacion_dt'] >= first_day_current_month) & 
        (df_cafeterias['fecha_creacion_dt'] <= last_day_current_month)
    ]

    # Mostrar información sobre el filtrado
    print(f"Registros que cumplen los criterios de fechas especificadas:")
    print(f"Total de registros: {len(df_cafeterias)}")

    # Mostrar el rango de fechas para verificación
    if not df_cafeterias.empty:
        print(f"\nRango de fechas en el DataFrame filtrado:")
        print(f"Fecha más antigua: {df_cafeterias['fecha_creacion_dt'].min().strftime('%d/%m/%Y')}")
        print(f"Fecha más reciente: {df_cafeterias['fecha_creacion_dt'].max().strftime('%d/%m/%Y')}")
    else:
        print("\nNo se encontraron registros que cumplan los criterios de filtrado")
        return pd.DataFrame()  # Retornar DataFrame vacío si no hay datos

    # 4. Convertir las columnas numéricas
    df_cafeterias['monto'] = pd.to_numeric(df_cafeterias['monto'], errors='coerce')
    df_cafeterias['tasa'] = pd.to_numeric(df_cafeterias['tasa'], errors='coerce')

    # 5. Agrupar por cafetería y calcular totales, incluyendo nuevas columnas
    aggregation_columns = {
        'monto': 'sum',
        'tasa': 'sum',
        'VALOR COMISION Monto': 'sum',
        'VALOR RETEFUENTE APPU Monto': 'sum',
        'VALOR RTE ICA APPU Monto': 'sum',
        'VALOR NETO Monto': 'sum',
        'VALOR COMISION APPU': 'sum',
        'VALOR RETEFUENTE APPU': 'sum',
        'VALOR RTE ICA APPU': 'sum',
        'GANANCIA NETO APPU': 'sum',
        'VALOR PRODUCTO': 'sum',
        'VALOR COMISION CAFETERIA': 'sum',
        'COMISION APPU-CAFETERIA': 'sum',
        'COMISION-WOMPI': 'sum',
        'VALOR RETEFUENTE CAFETERIA': 'sum',
        'VALOR RTE ICA CAFETERIA': 'sum',
        'VALOR NETO CAFETERIA': 'sum'
    }

    df_cafeterias_summary = df_cafeterias.groupby('cafeteria').agg(aggregation_columns).reset_index()

    # 6. Calcular monto sin tasa
    df_cafeterias_summary['monto_sin_tasa'] = df_cafeterias_summary['monto'] - df_cafeterias_summary['tasa']

    # 7. Ordenar por monto sin tasa de mayor a menor
    df_cafeterias_summary = df_cafeterias_summary.sort_values('monto_sin_tasa', ascending=False)

    # 8. Añadir fila de total
    total_row = pd.DataFrame({
        'cafeteria': ['Total (Órdenes Completadas)'],
        'monto': [df_cafeterias_summary['monto'].sum()],
        'tasa': [df_cafeterias_summary['tasa'].sum()],
        'VALOR COMISION Monto': [df_cafeterias_summary['VALOR COMISION Monto'].sum()],
        'VALOR RETEFUENTE APPU Monto': [df_cafeterias_summary['VALOR RETEFUENTE APPU Monto'].sum()],
        'VALOR RTE ICA APPU Monto': [df_cafeterias_summary['VALOR RTE ICA APPU Monto'].sum()],
        'VALOR NETO Monto': [df_cafeterias_summary['VALOR NETO Monto'].sum()],
        'VALOR COMISION APPU': [df_cafeterias_summary['VALOR COMISION APPU'].sum()],
        'VALOR RETEFUENTE APPU': [df_cafeterias_summary['VALOR RETEFUENTE APPU'].sum()],
        'VALOR RTE ICA APPU': [df_cafeterias_summary['VALOR RTE ICA APPU'].sum()],
        'GANANCIA NETO APPU': [df_cafeterias_summary['GANANCIA NETO APPU'].sum()],
        'VALOR PRODUCTO': [df_cafeterias_summary['VALOR PRODUCTO'].sum()],
        'VALOR COMISION CAFETERIA': [df_cafeterias_summary['VALOR COMISION CAFETERIA'].sum()],
        'COMISION APPU-CAFETERIA': [df_cafeterias_summary['COMISION APPU-CAFETERIA'].sum()],
        'COMISION-WOMPI': [df_cafeterias_summary['COMISION-WOMPI'].sum()],
        'VALOR RETEFUENTE CAFETERIA': [df_cafeterias_summary['VALOR RETEFUENTE CAFETERIA'].sum()],
        'VALOR RTE ICA CAFETERIA': [df_cafeterias_summary['VALOR RTE ICA CAFETERIA'].sum()],
        'VALOR NETO CAFETERIA': [df_cafeterias_summary['VALOR NETO CAFETERIA'].sum()],
        'monto_sin_tasa': [df_cafeterias_summary['monto_sin_tasa'].sum()]
    })

    df_cafeterias_summary = pd.concat([df_cafeterias_summary, total_row], ignore_index=True)

    # 9. Renombrar columnas para mejor presentación
    df_cafeterias_summary = df_cafeterias_summary.rename(columns={
        'cafeteria': 'Cafeterias',
        'monto': 'Monto con Tasa',
        'tasa': 'Tasa Total',
        'monto_sin_tasa': 'Monto sin Tasa',
        'VALOR COMISION Monto': 'Total VALOR COMISION Monto',
        'VALOR RETEFUENTE APPU Monto': 'Total VALOR RETEFUENTE APPU Monto',
        'VALOR RTE ICA APPU Monto': 'Total VALOR RTE ICA APPU Monto',
        'VALOR NETO Monto': 'Total VALOR NETO Monto',
        'VALOR COMISION APPU': 'Total VALOR COMISION APPU',
        'VALOR RETEFUENTE APPU': 'Total VALOR RETEFUENTE APPU',
        'VALOR RTE ICA APPU': 'Total VALOR RTE ICA APPU',
        'GANANCIA NETO APPU': 'Total GANANCIA NETO APPU',
        'VALOR PRODUCTO': 'Total VALOR PRODUCTO',
        'VALOR COMISION CAFETERIA': 'Total VALOR COMISION CAFETERIA',
        'COMISION APPU-CAFETERIA': 'Total COMISION APPU-CAFETERIA',
        'COMISION-WOMPI': 'Total COMISION-WOMPI',
        'VALOR RETEFUENTE CAFETERIA': 'Total VALOR RETEFUENTE CAFETERIA',
        'VALOR RTE ICA CAFETERIA': 'Total VALOR RTE ICA CAFETERIA',
        'VALOR NETO CAFETERIA': 'Total VALOR NETO CAFETERIA'
    })

    # 10. Seleccionar y reordenar columnas finales
    final_columns = [
        'Cafeterias', 'Monto con Tasa', 'Tasa Total', 'Monto sin Tasa',
        'Total VALOR COMISION Monto', 'Total VALOR RETEFUENTE APPU Monto',
        'Total VALOR RTE ICA APPU Monto', 'Total VALOR NETO Monto',
        'Total VALOR COMISION APPU', 'Total VALOR RETEFUENTE APPU',
        'Total VALOR RTE ICA APPU', 'Total GANANCIA NETO APPU',
        'Total VALOR PRODUCTO', 'Total VALOR COMISION CAFETERIA',
        'Total COMISION APPU-CAFETERIA', 'Total COMISION-WOMPI',
        'Total VALOR RETEFUENTE CAFETERIA', 'Total VALOR RTE ICA CAFETERIA',
        'Total VALOR NETO CAFETERIA'
    ]

    # Verificar que todas las columnas existan
    missing_final_columns = [col for col in final_columns if col not in df_cafeterias_summary.columns]
    if missing_final_columns:
        print(f"Advertencia: Las siguientes columnas faltan en df_cafeterias_summary y se rellenarán con valores NaN: {missing_final_columns}")
        for col in missing_final_columns:
            df_cafeterias_summary[col] = np.nan

    df_cafeterias_summary = df_cafeterias_summary[final_columns]

    # 11. Redondear los valores numéricos a 3 decimales
    numeric_columns = [
        'Monto con Tasa', 'Tasa Total', 'Monto sin Tasa',
        'Total VALOR COMISION Monto', 'Total VALOR RETEFUENTE APPU Monto',
        'Total VALOR RTE ICA APPU Monto', 'Total VALOR NETO Monto',
        'Total VALOR COMISION APPU', 'Total VALOR RETEFUENTE APPU',
        'Total VALOR RTE ICA APPU', 'Total GANANCIA NETO APPU',
        'Total VALOR PRODUCTO', 'Total VALOR COMISION CAFETERIA',
        'Total COMISION APPU-CAFETERIA', 'Total COMISION-WOMPI',
        'Total VALOR RETEFUENTE CAFETERIA', 'Total VALOR RTE ICA CAFETERIA',
        'Total VALOR NETO CAFETERIA'
    ]

    df_cafeterias_summary[numeric_columns] = df_cafeterias_summary[numeric_columns].round(3)

    print("\nResumen final de cafeterías (solo órdenes completadas):")
    print(df_cafeterias_summary)

    return df_cafeterias_summary

def convertir_columnas_numericas(df):
    """
    Convierte columnas específicas a tipos numéricos.

    Args:
        df (pd.DataFrame): DataFrame a procesar.

    Returns:
        pd.DataFrame: DataFrame con columnas numéricas convertidas.
    """
    if 'precio_unitario' in df.columns:
        df['precio_unitario'] = pd.to_numeric(df['precio_unitario'], errors='coerce')
    if 'cantidad_disponible' in df.columns:
        df['cantidad_disponible'] = pd.to_numeric(df['cantidad_disponible'], errors='coerce')
    return df

def procesar_ingredientes(df_ingredientes):
    """
    Procesa la columna 'opciones' del DataFrame de ingredientes.

    Args:
        df_ingredientes (pd.DataFrame): DataFrame de ingredientes.

    Returns:
        pd.DataFrame: DataFrame de ingredientes procesado.
    """
    if not df_ingredientes.empty and 'opciones' in df_ingredientes.columns:
        if isinstance(df_ingredientes.loc[0, 'opciones'], list):
            max_opciones = df_ingredientes['opciones'].apply(lambda x: len(x) if isinstance(x, list) else 0).max()
            for i in range(max_opciones):
                df_ingredientes[f'opcion_{i+1}_precio'] = df_ingredientes['opciones'].apply(
                    lambda x: round(x[i]['precio'], 3) if isinstance(x, list) and len(x) > i else None
                )
                df_ingredientes[f'opcion_{i+1}_ingrediente'] = df_ingredientes['opciones'].apply(
                    lambda x: x[i]['ingrediente'] if isinstance(x, list) and len(x) > i else None
                )
            df_ingredientes = df_ingredientes.drop(columns=['opciones'])
        else:
            df_ingredientes['opciones'] = df_ingredientes['opciones'].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else x
            )
    return df_ingredientes

def convertir_columnas_numericas_productos(df_productos):
    """
    Convierte columnas específicas a tipos numéricos en productos.

    Args:
        df_productos (pd.DataFrame): DataFrame de productos.

    Returns:
        pd.DataFrame: DataFrame de productos con columnas numéricas convertidas.
    """
    return convertir_columnas_numericas(df_productos)

def crear_grafico_instituciones(df_instituciones):
    """
    Crea un gráfico de barras apiladas con una línea para el porcentaje de instituciones activas.

    Args:
        df_instituciones (pd.DataFrame): DataFrame de instituciones.

    Returns:
        plotly.graph_objects.Figure: Figura del gráfico creado.
    """
    try:
        inst_stats = df_instituciones.groupby(['ciudad', 'is_active']).size().unstack(fill_value=0)
        inst_stats.columns = ['Inactivas', 'Activas']
        inst_stats['Total'] = inst_stats['Activas'] + inst_stats['Inactivas']
        inst_stats['% Activas'] = (inst_stats['Activas'] / inst_stats['Total'] * 100).round(1)

        fig = go.Figure()

        fig.add_trace(go.Bar(
            name='Instituciones Activas',
            x=inst_stats.index,
            y=inst_stats['Activas'],
            marker_color='#2ecc71'
        ))

        fig.add_trace(go.Bar(
            name='Instituciones Inactivas',
            x=inst_stats.index,
            y=inst_stats['Inactivas'],
            marker_color='#e74c3c'
        ))

        fig.add_trace(go.Scatter(
            name='% Activas',
            x=inst_stats.index,
            y=inst_stats['% Activas'],
            mode='lines+markers',
            line=dict(color='#3498db', width=2),
            yaxis='y2'
        ))

        fig.update_layout(
            title='Distribución de Instituciones por Ciudad',
            barmode='stack',
            xaxis_title='Ciudad',
            yaxis_title='Número de Instituciones',
            yaxis2=dict(
                title='% Instituciones Activas',
                overlaying='y',
                side='right',
                range=[0, 100]
            ),
            height=600,
            showlegend=True,
            hovermode='x unified'
        )

        return fig
    except Exception as e:
        print(f"Error en crear_grafico_instituciones: {e}")
        return go.Figure()

# ================================================
# Funciones de Exportación a Excel
# ================================================

def exportar_a_excel_integrado(dataframes, output_dir='excel_exports', timestamp=True):
    """
    Exporta todos los DataFrames a archivos Excel en el directorio especificado.

    Args:
        dataframes (dict): Diccionario con los DataFrames a exportar.
        output_dir (str, optional): Directorio donde se guardarán los archivos Excel. Defaults to 'excel_exports'.
        timestamp (bool, optional): Si True, añade fecha y hora al nombre del archivo. Defaults to True.

    Returns:
        dict: Diccionario con los paths de los archivos generados.
    """
    from datetime import datetime

    # Crear directorio si no existe
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Obtener timestamp si es necesario
    time_suffix = f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}" if timestamp else ""

    # Obtener el mes actual en español
    current_month = datetime.now().strftime('%B').capitalize()

    # Diccionario para almacenar los paths de los archivos generados
    generated_files = {}

    try:
        # 1. Exportar Órdenes con segmentaciones
        if 'ordenes_display' in dataframes and 'ordenes_completadas_display' in dataframes:
            filename = f'ordenes_segmentadas.xlsx'
            excel_path = os.path.join(output_dir, filename)
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                workbook = writer.book

                # Hoja 1: Órdenes
                sheet_name = 'Órdenes'
                dataframes['ordenes_display'].to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['ordenes_display']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaÓrdenes',
                    'style': 'Table Style Medium 9'
                })

                # Hoja 2: Órdenes Completadas
                sheet_name = 'Órdenes Completadas'
                dataframes['ordenes_completadas_display'].to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['ordenes_completadas_display']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaOrdenesCompletadas',
                    'style': 'Table Style Medium 9'
                })

                # Hoja 3: Volumen de pedidos por fecha
                if 'df_count' in dataframes:
                    sheet_name = 'Volumen de pedidos por fecha'
                    dataframes['df_count'].to_excel(writer, index=False, sheet_name=sheet_name)
                    worksheet = writer.sheets[sheet_name]
                    df = dataframes['df_count']
                    num_rows, num_cols = df.shape
                    worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                        'columns': [{'header': col} for col in df.columns],
                        'name': 'TablaVolumenPedidos',
                        'style': 'Table Style Medium 9'
                    })

                # Hoja 4: Popularidad del producto
                if 'df_product_popularity' in dataframes:
                    sheet_name = 'Popularidad del producto'
                    dataframes['df_product_popularity'].to_excel(writer, index=False, sheet_name=sheet_name)
                    worksheet = writer.sheets[sheet_name]
                    df = dataframes['df_product_popularity']
                    num_rows, num_cols = df.shape
                    worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                        'columns': [{'header': col} for col in df.columns],
                        'name': 'TablaPopularidadProducto',
                        'style': 'Table Style Medium 9'
                    })

            generated_files['ordenes'] = excel_path

        # 2. Exportar Detalle de Productos
        if 'products' in dataframes:
            filename = f'detalle_productos.xlsx'
            excel_path = os.path.join(output_dir, filename)
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                workbook = writer.book
                sheet_name = 'Detalle de Productos'
                dataframes['products'].to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['products']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaDetalleProductos',
                    'style': 'Table Style Medium 9'
                })
            generated_files['productos'] = excel_path

        # 3. Exportar Usuarios App
        if 'usuarios_app' in dataframes:
            filename = f'usuarios_app.xlsx'
            excel_path = os.path.join(output_dir, filename)
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                workbook = writer.book
                sheet_name = 'Usuarios_App'
                dataframes['usuarios_app'].to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['usuarios_app']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaUsuariosApp',
                    'style': 'Table Style Medium 9'
                })
            generated_files['usuarios_app'] = excel_path

        # 4. Exportar Usuarios
        if 'usuarios' in dataframes:
            filename = f'usuarios.xlsx'
            excel_path = os.path.join(output_dir, filename)
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                workbook = writer.book
                sheet_name = 'Usuarios'
                dataframes['usuarios'].to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['usuarios']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaUsuarios',
                    'style': 'Table Style Medium 9'
                })
            generated_files['usuarios'] = excel_path

        # 5. Exportar Resumen de Cafeterías
        if 'cafeterias' in dataframes:
            filename = f'{current_month}WompiCafeterias.xlsx'
            excel_path = os.path.join(output_dir, filename)

            df_cafeterias = dataframes.get('cafeterias', pd.DataFrame())

            if not df_cafeterias.empty:
                with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                    workbook = writer.book
                    sheet_name = 'Resumen Cafeterías'
                    df_cafeterias.to_excel(writer, index=False, sheet_name=sheet_name)
                    worksheet = writer.sheets[sheet_name]
                    num_rows, num_cols = df_cafeterias.shape
                    worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                        'columns': [{'header': col} for col in df_cafeterias.columns],
                        'name': 'TablaResumenCafeterias',
                        'style': 'Table Style Medium 9'
                    })

                    # Formatear las columnas de monto
                    money_format = workbook.add_format({'num_format': '$#,##0.000'})

                    # Ajusta las columnas según el orden final del DataFrame
                    for idx, col in enumerate(df_cafeterias.columns):
                        if col != 'Cafeterias':
                            worksheet.set_column(idx, idx, 20, money_format)

                generated_files['cafeterias'] = excel_path
            else:
                print("No se encontraron datos para las cafeterías. Archivo no generado.")

        # 6. Exportar Ingredientes
        if 'ingredientes' in dataframes:
            filename = f'ingredientes.xlsx'
            excel_path = os.path.join(output_dir, filename)
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                workbook = writer.book

                # Hoja 1: Ingredientes
                sheet_name = 'Ingredientes'
                dataframes['ingredientes'].to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['ingredientes']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaIngredientes',
                    'style': 'Table Style Medium 9'
                })

                # Hoja 2: Análisis Opciones
                columnas_opciones = [col for col in df.columns if 'opcion_' in col]
                if columnas_opciones:
                    sheet_name = 'Análisis Opciones'
                    analisis_opciones = dataframes['ingredientes'][columnas_opciones].notna().sum().reset_index()
                    analisis_opciones.columns = ['Opción', 'Cantidad de Opciones']
                    analisis_opciones.to_excel(writer, index=False, sheet_name=sheet_name)
                    worksheet = writer.sheets[sheet_name]
                    num_rows, num_cols = analisis_opciones.shape
                    worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                        'columns': [{'header': col} for col in analisis_opciones.columns],
                        'name': 'TablaAnalisisOpciones',
                        'style': 'Table Style Medium 9'
                    })

            generated_files['ingredientes'] = excel_path

        # 7. Exportar Instituciones
        if 'instituciones' in dataframes:
            filename = f'instituciones.xlsx'
            excel_path = os.path.join(output_dir, filename)
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                workbook = writer.book

                # Hoja 1: Instituciones
                sheet_name = 'Instituciones'
                dataframes['instituciones'].to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['instituciones']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaInstituciones',
                    'style': 'Table Style Medium 9'
                })

                # Hoja 2: Análisis por Ciudad
                sheet_name = 'Análisis por Ciudad'
                inst_stats = dataframes['instituciones'].groupby(['ciudad', 'is_active']).size().unstack(fill_value=0).reset_index()
                inst_stats.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets['Análisis por Ciudad']
                num_rows, num_cols = inst_stats.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in inst_stats.columns],
                    'name': 'TablaAnalisisCiudad',
                    'style': 'Table Style Medium 9'
                })

            generated_files['instituciones'] = excel_path

        # 8. Exportar Productos
        if 'productos' in dataframes:
            filename = f'productos.xlsx'
            excel_path = os.path.join(output_dir, filename)
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                workbook = writer.book

                # Hoja 1: Productos
                sheet_name = 'Productos'
                dataframes['productos'].to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['productos']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaProductos',
                    'style': 'Table Style Medium 9'
                })

                # Hoja 2: Estadísticas
                estadisticas_productos = pd.DataFrame({
                    'Total Productos': [len(dataframes['productos'])],
                    'Productos con Stock': [(dataframes['productos']['cantidad_disponible'] > 0).sum()],
                    'Precio Promedio': [dataframes['productos']['precio_unitario'].mean().round(3)],
                    'Precio Máximo': [dataframes['productos']['precio_unitario'].max().round(3)],
                    'Precio Mínimo': [dataframes['productos']['precio_unitario'].min().round(3)]
                })
                sheet_name = 'Estadísticas'
                estadisticas_productos.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                num_rows, num_cols = estadisticas_productos.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in estadisticas_productos.columns],
                    'name': 'TablaEstadisticasProductos',
                    'style': 'Table Style Medium 9'
                })

            generated_files['productos'] = excel_path

        # 9. Exportar Cafeterias Raw (Renombrado a cafeterias_db)
        if 'cafeterias_raw' in dataframes:
            filename = f'cafeterias_db.xlsx'
            excel_path = os.path.join(output_dir, filename)
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                workbook = writer.book
                sheet_name = 'Cafeterias DB'
                dataframes['cafeterias_raw'].to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['cafeterias_raw']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaCafeteriasDB',
                    'style': 'Table Style Medium 9'
                })
            generated_files['cafeterias_raw'] = excel_path

        print("\nArchivos Excel generados exitosamente:")
        for clave, ruta in generated_files.items():
            print(f"- {clave}: {ruta}")

        return generated_files
    except Exception as e:
        print(f"Error al exportar archivos Excel: {str(e)}")
        return generated_files

# ================================================
# Funciones de Creación de Figuras
# ================================================

def create_figures_integrado(df_ordenes_completadas, df_products, df_usuarios, df_usuarios_app, df_instituciones):
    """
    Crea las figuras de Plotly necesarias para el dashboard basándose en órdenes completadas.

    Args:
        df_ordenes_completadas (pd.DataFrame): DataFrame de órdenes completadas.
        df_products (pd.DataFrame): DataFrame de productos procesado.
        df_usuarios (pd.DataFrame): DataFrame de usuarios.
        df_usuarios_app (pd.DataFrame): DataFrame de usuarios de la app.
        df_instituciones (pd.DataFrame): DataFrame de instituciones.

    Returns:
        dict: Diccionario con las figuras y dataframes adicionales necesarias para los downloads.
    """
    figures_and_data = {}

    # Crear DataFrame para gráficas
    df_graph = df_ordenes_completadas.copy()

    # Convertir 'fecha_creacion_dt' a datetime si es necesario
    if 'fecha_creacion_dt' not in df_graph.columns:
        df_graph['fecha_creacion_dt'] = pd.to_datetime(df_graph['fecha_creacion_str'], errors='coerce')

    # df_count: Cantidad de Órdenes por Fecha
    df_count = df_graph.groupby(df_graph['fecha_creacion_dt'].dt.date).size().reset_index(name='count')
    df_count.rename(columns={'fecha_creacion_dt': 'fecha_creacion'}, inplace=True)

    figures_and_data['df_count'] = df_count

    # df_orden: Distribución de Estados de las Órdenes (solo completadas)
    df_orden = df_ordenes_completadas['orden_completada'].value_counts().reset_index()
    df_orden.columns = ['orden_completada', 'count']
    df_orden = df_orden.sort_values(by='count', ascending=True)
    figures_and_data['df_orden'] = df_orden

    # Gráfica principal de órdenes
    fig_main = px.bar(
        df_count,
        x='fecha_creacion',
        y='count',
        title='Cantidad de Órdenes Completadas por Fecha de Creación',
        color='count',
        color_continuous_scale=px.colors.sequential.Viridis,
        labels={'fecha_creacion': 'Fecha de Creación', 'count': 'Cantidad de Órdenes'}
    )
    fig_main.update_layout(
        xaxis=dict(
            tickformat='%d/%m/%Y',
            tickangle=45
        )
    )
    figures_and_data['fig_main'] = fig_main

    # Gráfica de órdenes completadas
    fig_orden = px.bar(
        df_orden,
        x='count',
        y='orden_completada',
        orientation='h',
        title='Cantidad de Órdenes Completadas',
        labels={'count': 'Cantidad', 'orden_completada': 'Orden Completada'},
        color='count',
        color_continuous_scale=px.colors.sequential.Viridis
    )
    fig_orden.update_layout(
        xaxis=dict(
            title='Cantidad'
        ),
        yaxis=dict(
            title='Orden Completada'
        ),
        height=600
    )
    figures_and_data['fig_orden'] = fig_orden

    # Gráficas de productos pedidos por día
    df_products['fecha_creacion_dt'] = pd.to_datetime(df_products['fecha_creacion_str'], errors='coerce')
    df_product_counts = df_products.groupby(['fecha_creacion_dt', 'producto'])['cantidad'].sum().reset_index()
    df_product_counts.sort_values(by='fecha_creacion_dt', inplace=True)
    figures_and_data['df_product_counts'] = df_product_counts

    fig_product_counts = px.line(
        df_product_counts,
        x='fecha_creacion_dt',
        y='cantidad',
        color='producto',
        title='Cantidad de Productos Pedidos por Día (Órdenes Completadas)',
        labels={'fecha_creacion_dt': 'Fecha', 'cantidad': 'Cantidad', 'producto': 'Producto'},
        markers=True,
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    fig_product_counts.update_layout(
        xaxis=dict(
            tickformat='%d/%m/%Y',
            tickangle=45
        ),
        yaxis=dict(
            title='Cantidad'
        ),
        height=600
    )
    figures_and_data['fig_product_counts'] = fig_product_counts

    # Cálculo de cantidad total por producto para el gráfico de barras (último mes)
    current_date = pd.Timestamp.today()
    first_day_last_month = (current_date.replace(day=1) - pd.Timedelta(days=1)).replace(day=1)
    last_day_last_month = (first_day_last_month + pd.offsets.MonthEnd(0)).to_pydatetime()

    df_products_last_month = df_products[
        (df_products['fecha_creacion_dt'] >= first_day_last_month) &
        (df_products['fecha_creacion_dt'] <= last_day_last_month)
    ]
    df_product_total_last_month = df_products_last_month.groupby('producto')['cantidad'].sum().reset_index()
    df_product_total_last_month = df_product_total_last_month.sort_values(by='cantidad', ascending=True)
    figures_and_data['df_product_total_last_month'] = df_product_total_last_month

    fig_bar_products = px.bar(
        df_product_total_last_month,
        x='cantidad',
        y='producto',
        orientation='h',
        title='Cantidad Total de Cada Producto (Último Mes - Órdenes Completadas)',
        labels={'producto': 'Producto', 'cantidad': 'Cantidad'},
        text='cantidad',
        color='cantidad',
        color_continuous_scale=px.colors.sequential.Viridis
    )
    fig_bar_products.update_layout(
        xaxis=dict(
            tickangle=0,
            automargin=True
        ),
        yaxis=dict(
            title='Producto'
        ),
        margin=dict(l=300, r=50, t=100, b=50),
        height=800
    )
    figures_and_data['fig_bar_products'] = fig_bar_products

    # Gráfica de volumen de pedidos por fecha
    df_order_volume = df_ordenes_completadas.groupby('fecha_creacion_str').size().reset_index(name='Cantidad de Órdenes')
    figures_and_data['df_order_volume'] = df_order_volume

    fig_order_volume = px.line(
        df_order_volume,
        x='fecha_creacion_str',
        y='Cantidad de Órdenes',
        title='Volumen de Órdenes Completadas por Fecha',
        labels={'fecha_creacion_str': 'Fecha de Creación', 'Cantidad de Órdenes': 'Cantidad de Órdenes'},
        markers=True,
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    fig_order_volume.update_layout(
        xaxis=dict(
            tickformat='%d/%m/%Y',
            tickangle=45
        ),
        yaxis=dict(
            title='Cantidad de Órdenes'
        ),
        height=600
    )
    figures_and_data['fig_order_volume'] = fig_order_volume

    # Agrupación basada en clientes
    df_client_grouping = df_ordenes_completadas.groupby('nombre_cliente').agg(
        Total_Ordenes=('id_orden', 'count'),
        Total_Monto=('monto', 'sum')
    ).reset_index().sort_values(by='Total_Ordenes', ascending=False)
    figures_and_data['df_client_grouping'] = df_client_grouping

    fig_client_grouping = px.bar(
        df_client_grouping.head(10),
        x='Total_Ordenes',
        y='nombre_cliente',
        orientation='h',
        title='Top 10 Clientes por Número de Órdenes Completadas',
        labels={'nombre_cliente': 'Cliente', 'Total_Ordenes': 'Total de Órdenes'},
        text='Total_Ordenes',
        color='Total_Ordenes',
        color_continuous_scale=px.colors.sequential.Viridis
    )
    fig_client_grouping.update_layout(
        yaxis=dict(
            autorange="reversed"
        ),
        margin=dict(l=200, r=50, t=100, b=50),
        height=600
    )
    figures_and_data['fig_client_grouping'] = fig_client_grouping

    # Popularidad del producto
    df_product_popularity = df_products.groupby('producto').agg(
        Total_Cantidad=('cantidad', 'sum'),
        Total_Ventas=('precioTotal', 'sum')  # Cambiado de 'precio_total' a 'precioTotal'
    ).reset_index().sort_values(by='Total_Cantidad', ascending=False)
    figures_and_data['df_product_popularity'] = df_product_popularity

    fig_product_popularity = px.bar(
        df_product_popularity.head(10),
        x='Total_Cantidad',
        y='producto',
        orientation='h',
        title='Top 10 Productos Más Populares (Órdenes Completadas)',
        labels={'producto': 'Producto', 'Total_Cantidad': 'Cantidad Total'},
        text='Total_Cantidad',
        color='Total_Cantidad',
        color_continuous_scale=px.colors.sequential.Viridis
    )
    fig_product_popularity.update_layout(
        xaxis=dict(
            tickangle=45
        ),
        yaxis=dict(
            title='Producto'
        ),
        height=600
    )
    figures_and_data['fig_product_popularity'] = fig_product_popularity

    # Estado del pedido y comprobante de pago
    df_order_status = df_ordenes_completadas.groupby(['orden_completada', 'comprobante_pago']).size().reset_index(name='Cantidad')
    figures_and_data['df_order_status'] = df_order_status

    fig_order_status = px.bar(
        df_order_status,
        x='orden_completada',
        y='Cantidad',
        color='comprobante_pago',
        title='Estado del Pedido y Comprobante de Pago (Órdenes Completadas)',
        labels={'orden_completada': 'Estado del Pedido', 'Cantidad': 'Cantidad', 'comprobante_pago': 'Comprobante de Pago'},
        barmode='stack',
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    fig_order_status.update_layout(
        xaxis=dict(
            title='Estado del Pedido'
        ),
        yaxis=dict(
            title='Cantidad'
        ),
        height=600
    )
    figures_and_data['fig_order_status'] = fig_order_status

    # Ingresos por institución
    df_revenue_institution = df_ordenes_completadas.groupby('institucion').agg(
        Total_Revenue=('monto', 'sum'),
        Total_Ordenes=('id_orden', 'count')
    ).reset_index().sort_values(by='Total_Revenue', ascending=False)
    figures_and_data['df_revenue_institution'] = df_revenue_institution

    fig_revenue_institution = px.bar(
        df_revenue_institution.head(10),
        x='Total_Revenue',
        y='institucion',
        orientation='h',
        title='Top 10 Instituciones por Ingresos Totales (Órdenes Completadas)',
        labels={'institucion': 'Institución', 'Total_Revenue': 'Ingresos Totales'},
        text='Total_Revenue',
        color='Total_Revenue',
        color_continuous_scale=px.colors.sequential.Viridis
    )
    fig_revenue_institution.update_layout(
        yaxis=dict(
            autorange="reversed"
        ),
        margin=dict(l=200, r=50, t=100, b=50),
        height=600
    )
    figures_and_data['fig_revenue_institution'] = fig_revenue_institution

    # Pedidos para llevar vs en sitio
    df_order_type = df_ordenes_completadas.groupby('para_llevar').size().reset_index(name='Cantidad de Órdenes')
    df_order_type['Tipo de Orden'] = df_order_type['para_llevar'].map({True: 'Para Llevar', False: 'En Sitio'})
    figures_and_data['df_order_type'] = df_order_type

    fig_order_type = px.pie(
        df_order_type,
        values='Cantidad de Órdenes',
        names='Tipo de Orden',
        title='Proporción de Pedidos Para Llevar vs. En Sitio (Órdenes Completadas)',
        hole=0.4,
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    fig_order_type.update_traces(textposition='inside', textinfo='percent+label')
    figures_and_data['fig_order_type'] = fig_order_type

    # Horas punta
    df_peak_hours = df_ordenes_completadas.copy()
    df_peak_hours['fecha_creacion_dt'] = pd.to_datetime(df_peak_hours['fecha_creacion_str'], errors='coerce')

    # Obtener el mes actual
    current_date = datetime.now()
    first_day_current_month = current_date.replace(day=1)
    last_day_current_month = (first_day_current_month + pd.offsets.MonthEnd(0)).to_pydatetime()

    # Filtrar solo el mes actual
    df_peak_hours = df_peak_hours[
        (df_peak_hours['fecha_creacion_dt'] >= first_day_current_month) &
        (df_peak_hours['fecha_creacion_dt'] <= last_day_current_month)
    ]

    if df_peak_hours.empty:
        print("Advertencia: No hay datos para el mes actual. No se puede crear el gráfico de horas punta.")
        figures_and_data['fig_peak_hours'] = go.Figure()
    else:
        # Procesar las horas
        df_peak_hours['hora'] = pd.to_datetime(df_peak_hours['hora_creacion'], errors='coerce').dt.hour

        df_peak_hours = df_peak_hours.groupby('hora').size().reset_index(name='Cantidad de Órdenes').sort_values(by='hora')
        figures_and_data['df_peak_hours'] = df_peak_hours

        mes_nombre = first_day_current_month.strftime('%B %Y')

        fig_peak_hours = px.bar(
            df_peak_hours,
            x='hora',
            y='Cantidad de Órdenes',
            title=f'Órdenes Completadas por Hora del Día - {mes_nombre}',
            labels={'hora': 'Hora del Día', 'Cantidad de Órdenes': 'Cantidad de Órdenes'},
            text='Cantidad de Órdenes',
            color='hora',
            color_continuous_scale=px.colors.sequential.Viridis
        )
        fig_peak_hours.update_layout(
            xaxis=dict(
                tickmode='linear',
                dtick=1
            ),
            yaxis=dict(
                title='Cantidad de Órdenes'
            ),
            height=600
        )
        figures_and_data['fig_peak_hours'] = fig_peak_hours

    # Gráfico de instituciones
    if not df_instituciones.empty:
        fig_instituciones = crear_grafico_instituciones(df_instituciones)
        figures_and_data['fig_instituciones'] = fig_instituciones

    return figures_and_data

# ================================================
# Configuración de la Aplicación Dash
# ================================================

def setup_dash_app_integrado(figures_and_data, dataframes):
    """
    Configura la aplicación Dash, definiendo el layout y registrando los callbacks.

    Args:
        figures_and_data (dict): Diccionario con figuras y dataframes adicionales.
        dataframes (dict): Diccionario con los DataFrames principales.

    Returns:
        dash.Dash: Instancia de la aplicación Dash.
    """
    app = dash.Dash(__name__, suppress_callback_exceptions=True)

    # Combinar dataframes con figures_and_data
    dataframes.update(figures_and_data)

    # Definir estilos para tablas (del segundo script)
    estilo_celda = {
        'textAlign': 'left',
        'minWidth': '100px',
        'width': '150px',
        'maxWidth': '200px',
        'whiteSpace': 'normal',
        'height': '30px',
        'padding': '4px',
        'fontSize': '12px',
    }

    estilo_header = {
        'backgroundColor': 'rgb(230, 230, 230)',
        'fontWeight': 'bold',
        'fontSize': '14px',
        'padding': '4px',
    }

    estilo_datos = {
        'fontSize': '12px',
        'padding': '4px',
    }

    # Definir el layout de la aplicación Dash con pestañas
    app.layout = html.Div(children=[
        html.H1(children='Dashboard de Datos de la Base de Datos', style={'textAlign': 'center'}),

        dcc.Tabs([
            # Pestaña 1: Órdenes y Gráficas (del primer script)
            dcc.Tab(label='Órdenes y Gráficas', children=[
                dcc.Tabs([
                    # Subpestaña 1.1: Tabla de Órdenes
                    dcc.Tab(label='Tabla de Órdenes', children=[
                        html.Div([
                            dcc.Input(
                                id='search-ordenes',
                                type='text',
                                placeholder='Búsqueda global en órdenes...',
                                style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                            ),
                            dash_table.DataTable(
                                id='ordenes-table_1',
                                columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes.get('ordenes_display', pd.DataFrame()).columns],
                                data=dataframes.get('ordenes_display', pd.DataFrame()).to_dict('records'),
                                page_size=10,
                                sort_action='native',
                                sort_mode='multi',
                                style_table={'overflowX': 'auto'},
                                style_cell={
                                    'textAlign': 'left',
                                    'minWidth': '150px',
                                    'width': '200px',
                                    'maxWidth': '250px',
                                },
                                # Formatear columnas numéricas para mostrar 3 decimales
                                style_data_conditional=[
                                    {
                                        'if': {'column_id': col},
                                        'textAlign': 'right'
                                    } for col in ['VALOR COMISION Monto', 'VALOR RETEFUENTE APPU Monto', 'VALOR RTE ICA APPU Monto', 'VALOR NETO Monto',
                                                    'VALOR COMISION APPU', 'VALOR RETEFUENTE APPU', 'VALOR RTE ICA APPU', 'GANANCIA NETO APPU',
                                                    'VALOR PRODUCTO', 'VALOR COMISION CAFETERIA', 'COMISION APPU-CAFETERIA',
                                                    'COMISION-WOMPI', 'VALOR RETEFUENTE CAFETERIA', 'VALOR RTE ICA CAFETERIA',
                                                    'VALOR NETO CAFETERIA']
                                        if col in dataframes.get('ordenes_display', pd.DataFrame()).columns
                                ],
                                style_header={
                                    'backgroundColor': 'rgb(230, 230, 230)',
                                    'fontWeight': 'bold'
                                }
                            ),
                            html.Button("Descargar Órdenes a Excel", id="btn-download-ordenes", n_clicks=0),
                            dcc.Download(id="download-ordenes"),
                        ])
                    ]),
                    # Subpestaña 1.2: Órdenes Completadas
                    dcc.Tab(label='Órdenes Completadas', children=[
                        html.Div([
                            dcc.Input(
                                id='search-ordenes-completadas',
                                type='text',
                                placeholder='Búsqueda global en órdenes completadas...',
                                style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                            ),
                            dash_table.DataTable(
                                id='ordenes-completadas-table',
                                columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes.get('ordenes_completadas_display', pd.DataFrame()).columns],
                                data=dataframes.get('ordenes_completadas_display', pd.DataFrame()).to_dict('records'),
                                page_size=10,
                                sort_action='native',
                                sort_mode='multi',
                                style_table={'overflowX': 'auto'},
                                style_cell={
                                    'textAlign': 'left',
                                    'minWidth': '150px',
                                    'width': '200px',
                                    'maxWidth': '250px',
                                },
                                # Formatear columnas numéricas para mostrar 3 decimales
                                style_data_conditional=[
                                    {
                                        'if': {'column_id': col},
                                        'textAlign': 'right'
                                    } for col in ['VALOR COMISION Monto', 'VALOR RETEFUENTE APPU Monto', 'VALOR RTE ICA APPU Monto', 'VALOR NETO Monto',
                                                    'VALOR COMISION APPU', 'VALOR RETEFUENTE APPU', 'VALOR RTE ICA APPU', 'GANANCIA NETO APPU',
                                                    'VALOR PRODUCTO', 'VALOR COMISION CAFETERIA', 'COMISION APPU-CAFETERIA',
                                                    'COMISION-WOMPI', 'VALOR RETEFUENTE CAFETERIA', 'VALOR RTE ICA CAFETERIA',
                                                    'VALOR NETO CAFETERIA']
                                        if col in dataframes.get('ordenes_completadas_display', pd.DataFrame()).columns
                                ],
                                style_header={
                                    'backgroundColor': 'rgb(230, 230, 230)',
                                    'fontWeight': 'bold'
                                }
                            ),
                            html.Button("Descargar Órdenes Completadas a Excel", id="btn-download-ordenes-completadas", n_clicks=0),
                            dcc.Download(id="download-ordenes-completadas"),
                        ])
                    ]),
                    # Subpestaña 1.3: Gráficas de Órdenes
                    dcc.Tab(label='Gráficas de Órdenes', children=[
                        html.Div([
                            dcc.Graph(
                                id='cantidad-ordenes_1',
                                figure=dataframes.get('fig_main', {})
                            ),
                            dcc.Graph(
                                id='orden-completadas_2',
                                figure=dataframes.get('fig_orden', {})
                            )
                        ])
                    ])
                ])
            ]),

            # Pestaña 2: Detalle de Productos con subpestañas (del primer script)
            dcc.Tab(label='Detalle de Productos', children=[
                dcc.Tabs([
                    # Subpestaña 2.1: Tabla de Productos
                    dcc.Tab(label='Tabla de Productos', children=[
                        html.Div([
                            dcc.Input(
                                id='search-productos',
                                type='text',
                                placeholder='Búsqueda global en productos...',
                                style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                            ),
                            dash_table.DataTable(
                                id='ordenes-table_2',
                                columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes.get('products', pd.DataFrame()).columns],
                                data=dataframes.get('products', pd.DataFrame()).to_dict('records'),
                                page_size=10,
                                sort_action='native',
                                sort_mode='multi',
                                style_table={'overflowX': 'auto'},
                                style_cell={
                                    'textAlign': 'left',
                                    'minWidth': '150px',
                                    'width': '200px',
                                    'maxWidth': '250px',
                                },
                                style_header={
                                    'backgroundColor': 'rgb(230, 230, 230)',
                                    'fontWeight': 'bold'
                                }
                            ),
                            html.Button("Descargar Detalle de Productos a Excel", id="btn-download-detalle-productos", n_clicks=0),
                            dcc.Download(id="download-detalle-productos"),
                        ])
                    ]),
                    # Subpestaña 2.2: Gráficas de Productos
                    dcc.Tab(label='Gráficas de Productos', children=[
                        html.Div([
                            dcc.Graph(
                                id='productos-bar-chart',
                                figure=dataframes.get('fig_bar_products', {})
                            ),
                            dcc.Graph(
                                id='product-popularity',
                                figure=dataframes.get('fig_product_popularity', {})
                            ),
                        ])
                    ]),
                    # Subpestaña 2.3: Análisis de Ventas
                    dcc.Tab(label='Análisis de Ventas', children=[
                        html.Div([
                            dcc.Graph(
                                id='revenue-institution',
                                figure=dataframes.get('fig_revenue_institution', {})
                            ),
                            dcc.Graph(
                                id='order-type',
                                figure=dataframes.get('fig_order_type', {})
                            ),
                            dcc.Graph(
                                id='peak-hours',
                                figure=dataframes.get('fig_peak_hours', {})
                            ),
                        ])
                    ])
                ])
            ]),

            # Pestaña 3: Usuarios App (del primer script)
            dcc.Tab(label='Usuarios App', children=[
                html.Div([
                    dcc.Input(
                        id='search-usuarios-app',
                        type='text',
                        placeholder='Búsqueda global en usuarios app...',
                        style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                    ),
                    dash_table.DataTable(
                        id='usuarios-app-table_2',
                        columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes.get('usuarios_app', pd.DataFrame()).columns],
                        data=dataframes.get('usuarios_app', pd.DataFrame()).to_dict('records'),
                        page_size=10,
                        sort_action='native',
                        sort_mode='multi',
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'textAlign': 'left',
                            'minWidth': '150px',
                            'width': '200px',
                            'maxWidth': '250px',
                        },
                        style_header={
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight': 'bold'
                        }
                    ),
                    html.Button("Descargar Usuarios App a Excel", id="btn-download-usuarios-app", n_clicks=0),
                    dcc.Download(id="download-usuarios-app"),
                ])
            ]),

            # Pestaña 4: Usuarios (del primer script)
            dcc.Tab(label='Usuarios', children=[
                html.Div([
                    dcc.Input(
                        id='search-usuarios',
                        type='text',
                        placeholder='Búsqueda global en usuarios...',
                        style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                    ),
                    dash_table.DataTable(
                        id='usuarios-table_3',
                        columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes.get('usuarios', pd.DataFrame()).columns],
                        data=dataframes.get('usuarios', pd.DataFrame()).to_dict('records'),
                        page_size=10,
                        sort_action='native',
                        sort_mode='multi',
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'textAlign': 'left',
                            'minWidth': '150px',
                            'width': '200px',
                            'maxWidth': '250px',
                        },
                        style_header={
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight': 'bold'
                        }
                    ),
                    html.Button("Descargar Usuarios a Excel", id="btn-download-usuarios", n_clicks=0),
                    dcc.Download(id="download-usuarios"),
                ])
            ]),

            # Pestaña 5: Resumen de Cafeterías (del primer script)
            dcc.Tab(label='Resumen de Cafeterías', children=[
                html.Div([
                    dcc.Input(
                        id='search-cafeterias',
                        type='text',
                        placeholder='Búsqueda global en cafeterías...',
                        style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                    ),
                    dash_table.DataTable(
                        id='cafeterias-table',
                        columns=[{'name': i, 'id': i} for i in dataframes.get('cafeterias', pd.DataFrame()).columns],
                        data=dataframes.get('cafeterias', pd.DataFrame()).to_dict('records'),
                        page_size=8,
                        sort_action='native',
                        sort_mode='multi',
                        style_table={'overflowX': 'auto'},
                        style_cell=estilo_celda,
                        style_header=estilo_header,
                        style_data=estilo_datos,
                        # Formatear columnas numéricas para mostrar 3 decimales
                        style_data_conditional=[
                            {
                                'if': {'column_id': col},
                                'textAlign': 'right'
                            } for col in ['Monto con Tasa', 'Tasa Total', 'Monto sin Tasa',
                                        'Total VALOR COMISION Monto', 'Total VALOR RETEFUENTE APPU Monto',
                                        'Total VALOR RTE ICA APPU Monto', 'Total VALOR NETO Monto',
                                        'Total VALOR COMISION APPU', 'Total VALOR RETEFUENTE APPU',
                                        'Total VALOR RTE ICA APPU', 'Total GANANCIA NETO APPU',
                                        'Total VALOR PRODUCTO', 'Total VALOR COMISION CAFETERIA',
                                        'Total COMISION APPU-CAFETERIA', 'Total COMISION-WOMPI',
                                        'Total VALOR RETEFUENTE CAFETERIA', 'Total VALOR RTE ICA CAFETERIA',
                                        'Total VALOR NETO CAFETERIA']
                                if col in dataframes.get('cafeterias', pd.DataFrame()).columns
                        ],
                    ),
                    html.Button("Descargar Resumen de Cafeterías a Excel", id="btn-download-cafeterias", n_clicks=0),
                    dcc.Download(id="download-cafeterias"),
                ])
            ]),

            # Pestaña 6: Cafeterías (Renombrada a cafeterias_db)
            dcc.Tab(label='Cafeterías', children=[
                html.Div([
                    dcc.Input(
                        id='search-cafeterias-db',
                        type='text',
                        placeholder='Búsqueda global en cafeterías (db)...',
                        style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                    ),
                    dash_table.DataTable(
                        id='cafeterias_db_table',
                        columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes.get('cafeterias_raw', pd.DataFrame()).columns],
                        data=dataframes.get('cafeterias_raw', pd.DataFrame()).to_dict('records'),
                        page_size=10,
                        sort_action='native',
                        sort_mode='multi',
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'textAlign': 'left',
                            'minWidth': '150px',
                            'width': '200px',
                            'maxWidth': '250px',
                        },
                        style_header={
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight': 'bold'
                        }
                    ),
                    html.Button("Descargar Cafeterías DB a Excel", id="btn-download-cafeterias-db", n_clicks=0),
                    dcc.Download(id="download-cafeterias-db"),
                ])
            ]),

            # Pestaña 7: Ingredientes (del segundo script)
            dcc.Tab(label='Ingredientes', children=[
                html.Div([
                    dcc.Input(
                        id='buscar-ingredientes',
                        type='text',
                        placeholder='Búsqueda global en ingredientes...',
                        style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                    ),
                    dash_table.DataTable(
                        id='tabla-ingredientes',
                        columns=[{'name': col, 'id': col} for col in dataframes.get('ingredientes', pd.DataFrame()).columns],
                        data=dataframes.get('ingredientes', pd.DataFrame()).to_dict('records'),
                        page_size=8,
                        sort_action='native',
                        sort_mode='multi',
                        style_table={'overflowX': 'auto'},
                        style_cell=estilo_celda,
                        style_header=estilo_header,
                        style_data=estilo_datos,
                    ),
                    html.Button("Descargar Ingredientes a Excel", id="btn-download-ingredientes", n_clicks=0),
                    dcc.Download(id="descargar-ingredientes"),
                ])
            ]),

            # Pestaña 8: Instituciones (del segundo script)
            dcc.Tab(label='Instituciones', children=[
                dcc.Tabs([
                    # Subpestaña Tabla de Datos
                    dcc.Tab(label='Tabla de Datos', children=[
                        html.Div([
                            dcc.Input(
                                id='buscar-instituciones',
                                type='text',
                                placeholder='Búsqueda global en instituciones...',
                                style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                            ),
                            dash_table.DataTable(
                                id='tabla-instituciones',
                                columns=[{'name': col, 'id': col} for col in dataframes.get('instituciones', pd.DataFrame()).columns],
                                data=dataframes.get('instituciones', pd.DataFrame()).to_dict('records'),
                                page_size=8,
                                sort_action='native',
                                sort_mode='multi',
                                style_table={'overflowX': 'auto'},
                                style_cell=estilo_celda,
                                style_header=estilo_header,
                                style_data=estilo_datos,
                            ),
                            html.Button("Descargar Instituciones a Excel", id="btn-download-instituciones", n_clicks=0),
                            dcc.Download(id="descargar-instituciones"),
                        ])
                    ]),
                    # Subpestaña Gráficas
                    dcc.Tab(label='Gráficas', children=[
                        html.Div([
                            dcc.Graph(
                                id='grafico-instituciones',
                                figure=dataframes.get('fig_instituciones', {})
                            )
                        ])
                    ])
                ])
            ]),

            # Pestaña 9: Productos (del segundo script y parte del primero)
            dcc.Tab(label='Productos', children=[
                html.Div([
                    dcc.Input(
                        id='buscar-productos-main',
                        type='text',
                        placeholder='Búsqueda global en productos...',
                        style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                    ),
                    dash_table.DataTable(
                        id='tabla-productos',
                        columns=[{'name': col, 'id': col} for col in dataframes.get('productos', pd.DataFrame()).columns],
                        data=dataframes.get('productos', pd.DataFrame()).to_dict('records'),
                        page_size=8,
                        sort_action='native',
                        sort_mode='multi',
                        style_table={'overflowX': 'auto'},
                        style_cell=estilo_celda,
                        style_header=estilo_header,
                        style_data=estilo_datos,
                    ),
                    html.Button("Descargar Productos a Excel", id="btn-download-productos-main", n_clicks=0),
                    dcc.Download(id="descargar-productos"),
                ])
            ]),
        ])
    ])

    # --------------------------------
    # Callbacks para el Filtrado
    # --------------------------------

    # Callback para filtrar Órdenes
    @app.callback(
        Output('ordenes-table_1', 'data'),
        [Input('search-ordenes', 'value')]
    )
    def update_ordenes_table(search_value):
        filtered_df = filter_dataframe(dataframes.get('ordenes_display', pd.DataFrame()), search_value)
        return filtered_df.to_dict('records')

    # Callback para filtrar Órdenes Completadas
    @app.callback(
        Output('ordenes-completadas-table', 'data'),
        [Input('search-ordenes-completadas', 'value')]
    )
    def update_ordenes_completadas_table(search_value):
        filtered_df = filter_dataframe(dataframes.get('ordenes_completadas_display', pd.DataFrame()), search_value)
        return filtered_df.to_dict('records')

    # Callback para filtrar Productos (Tabla de Productos)
    @app.callback(
        Output('ordenes-table_2', 'data'),
        [Input('search-productos', 'value')]
    )
    def update_productos_table(search_value):
        filtered_df = filter_dataframe(dataframes.get('products', pd.DataFrame()), search_value)
        return filtered_df.to_dict('records')

    # Callback para filtrar Usuarios App
    @app.callback(
        Output('usuarios-app-table_2', 'data'),
        [Input('search-usuarios-app', 'value')]
    )
    def update_usuarios_app_table(search_value):
        filtered_df = filter_dataframe(dataframes.get('usuarios_app', pd.DataFrame()), search_value)
        return filtered_df.to_dict('records')

    # Callback para filtrar Usuarios
    @app.callback(
        Output('usuarios-table_3', 'data'),
        [Input('search-usuarios', 'value')]
    )
    def update_usuarios_table(search_value):
        filtered_df = filter_dataframe(dataframes.get('usuarios', pd.DataFrame()), search_value)
        return filtered_df.to_dict('records')

    # Callback para filtrar Resumen de Cafeterías
    @app.callback(
        Output('cafeterias-table', 'data'),
        [Input('search-cafeterias', 'value')]
    )
    def update_cafeterias_table(search_value):
        filtered_df = filter_dataframe(dataframes.get('cafeterias', pd.DataFrame()), search_value)
        return filtered_df.to_dict('records')

    # Callback para filtrar Cafeterías DB (renombrado)
    @app.callback(
        Output('cafeterias_db_table', 'data'),
        [Input('search-cafeterias-db', 'value')]
    )
    def update_cafeterias_db_table(search_value):
        filtered_df = filter_dataframe(dataframes.get('cafeterias_raw', pd.DataFrame()), search_value)
        return filtered_df.to_dict('records')

    # Callback para filtrar Ingredientes
    @app.callback(
        Output('tabla-ingredientes', 'data'),
        [Input('buscar-ingredientes', 'value')]
    )
    def actualizar_tabla_ingredientes(valor_busqueda):
        if valor_busqueda:
            filtered_df = filter_dataframe(dataframes.get('ingredientes', pd.DataFrame()), valor_busqueda)
            return filtered_df.to_dict('records')
        return dataframes.get('ingredientes', pd.DataFrame()).to_dict('records')

    # Callback para filtrar Instituciones
    @app.callback(
        Output('tabla-instituciones', 'data'),
        [Input('buscar-instituciones', 'value')]
    )
    def actualizar_tabla_instituciones(valor_busqueda):
        if valor_busqueda:
            filtered_df = filter_dataframe(dataframes.get('instituciones', pd.DataFrame()), valor_busqueda)
            return filtered_df.to_dict('records')
        return dataframes.get('instituciones', pd.DataFrame()).to_dict('records')

    # Callback para filtrar Productos (Tabla de Productos en Pestaña 8)
    @app.callback(
        Output('tabla-productos', 'data'),
        [Input('buscar-productos-main', 'value')]
    )
    def actualizar_tabla_productos_main(valor_busqueda):
        if valor_busqueda:
            filtered_df = filter_dataframe(dataframes.get('productos', pd.DataFrame()), valor_busqueda)
            return filtered_df.to_dict('records')
        return dataframes.get('productos', pd.DataFrame()).to_dict('records')

    # --------------------------------
    # Callbacks para Descargas
    # --------------------------------

    # Callback para descargar la tabla de órdenes con segmentaciones
    @app.callback(
        Output("download-ordenes", "data"),
        Input("btn-download-ordenes", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_ordenes(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                workbook = writer.book

                # Hoja 1: Órdenes
                sheet_name = 'Órdenes'
                dataframes.get('ordenes_display', pd.DataFrame()).to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes.get('ordenes_display', pd.DataFrame())
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaÓrdenes',
                    'style': 'Table Style Medium 9'
                })

                # Hoja 2: Órdenes Completadas
                sheet_name = 'Órdenes Completadas'
                dataframes.get('ordenes_completadas_display', pd.DataFrame()).to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes.get('ordenes_completadas_display', pd.DataFrame())
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaOrdenesCompletadas',
                    'style': 'Table Style Medium 9'
                })

                # Hoja 3: Volumen de pedidos por fecha
                if 'df_count' in dataframes:
                    sheet_name = 'Volumen de pedidos por fecha'
                    dataframes['df_count'].to_excel(writer, index=False, sheet_name=sheet_name)
                    worksheet = writer.sheets[sheet_name]
                    df = dataframes['df_count']
                    num_rows, num_cols = df.shape
                    worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                        'columns': [{'header': col} for col in df.columns],
                        'name': 'TablaVolumenPedidos',
                        'style': 'Table Style Medium 9'
                    })

                # Hoja 4: Popularidad del producto
                if 'df_product_popularity' in dataframes:
                    sheet_name = 'Popularidad del producto'
                    dataframes['df_product_popularity'].to_excel(writer, index=False, sheet_name=sheet_name)
                    worksheet = writer.sheets[sheet_name]
                    df = dataframes['df_product_popularity']
                    num_rows, num_cols = df.shape
                    worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                        'columns': [{'header': col} for col in df.columns],
                        'name': 'TablaPopularidadProducto',
                        'style': 'Table Style Medium 9'
                    })

            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "ordenes_segmentadas.xlsx")

    # Callback para descargar el detalle de productos
    @app.callback(
        Output("download-detalle-productos", "data"),
        Input("btn-download-detalle-productos", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_detalle_productos(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                workbook = writer.book
                sheet_name = 'Detalle de Productos'
                dataframes.get('products', pd.DataFrame()).to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['products']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaDetalleProductos',
                    'style': 'Table Style Medium 9'
                })
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "detalle_productos.xlsx")

    # Callback para descargar Productos Principales
    @app.callback(
        Output("descargar-productos", "data"),
        Input("btn-download-productos-main", "n_clicks"),
        prevent_initial_call=True,
    )
    def descargar_productos(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                workbook = writer.book
                sheet_name = 'Productos'
                df_productos = dataframes.get('productos', pd.DataFrame())
                df_productos.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                num_rows, num_cols = df_productos.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df_productos.columns],
                    'name': 'TablaProductos',
                    'style': 'Table Style Medium 9'
                })

                # Estadísticas de Productos
                estadisticas_productos = pd.DataFrame({
                    'Total Productos': [len(df_productos)],
                    'Productos con Stock': [(df_productos['cantidad_disponible'] > 0).sum()],
                    'Precio Promedio': [df_productos['precio_unitario'].mean().round(3)],
                    'Precio Máximo': [df_productos['precio_unitario'].max().round(3)],
                    'Precio Mínimo': [df_productos['precio_unitario'].min().round(3)]
                })
                sheet_name = 'Estadísticas'
                estadisticas_productos.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                num_rows, num_cols = estadisticas_productos.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in estadisticas_productos.columns],
                    'name': 'TablaEstadisticasProductos',
                    'style': 'Table Style Medium 9'
                })

            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "productos.xlsx")

    # Callback para descargar Usuarios App
    @app.callback(
        Output("download-usuarios-app", "data"),
        Input("btn-download-usuarios-app", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_usuarios_app(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                workbook = writer.book
                sheet_name = 'Usuarios_App'
                dataframes.get('usuarios_app', pd.DataFrame()).to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['usuarios_app']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaUsuariosApp',
                    'style': 'Table Style Medium 9'
                })
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "usuarios_app.xlsx")

    # Callback para descargar Usuarios
    @app.callback(
        Output("download-usuarios", "data"),
        Input("btn-download-usuarios", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_usuarios(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                workbook = writer.book
                sheet_name = 'Usuarios'
                dataframes.get('usuarios', pd.DataFrame()).to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                df = dataframes['usuarios']
                num_rows, num_cols = df.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df.columns],
                    'name': 'TablaUsuarios',
                    'style': 'Table Style Medium 9'
                })
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "usuarios.xlsx")

    # Callback para descargar Resumen de Cafeterías
    @app.callback(
        Output("download-cafeterias", "data"),
        Input("btn-download-cafeterias", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_cafeterias(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                workbook = writer.book
                sheet_name = 'Resumen Cafeterías'
                df_cafeterias = dataframes.get('cafeterias', pd.DataFrame())
                df_cafeterias.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                num_rows, num_cols = df_cafeterias.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df_cafeterias.columns],
                    'name': 'TablaResumenCafeterias',
                    'style': 'Table Style Medium 9'
                })

                # Formatear las columnas de monto
                money_format = workbook.add_format({'num_format': '$#,##0.000'})

                # Ajusta las columnas según el orden final del DataFrame
                for idx, col in enumerate(df_cafeterias.columns):
                    if col != 'Cafeterias':
                        worksheet.set_column(idx, idx, 20, money_format)

            buffer.seek(0)
            # Obtener el nombre del primer mes para el nombre del archivo
            if not df_cafeterias.empty:
                first_cafeteria = df_cafeterias.iloc[0]['Cafeterias']
                first_month = first_cafeteria.split()[0]
                return dcc.send_bytes(buffer.read(), f"Resumen_Cafeterias_{first_month}.xlsx")
            else:
                return dcc.send_bytes(buffer.read(), "Resumen_Cafeterias.xlsx")

    # Callback para descargar Cafeterías DB
    @app.callback(
        Output("download-cafeterias-db", "data"),
        Input("btn-download-cafeterias-db", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_cafeterias_db(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                workbook = writer.book

                # Hoja 1: Instituciones
                sheet_name = 'Cafeterias DB'
                df_cafeterias_db = dataframes.get('cafeterias_raw', pd.DataFrame())
                df_cafeterias_db.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                num_rows, num_cols = df_cafeterias_db.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df_cafeterias_db.columns],
                    'name': 'TablaCafeteriasDB',
                    'style': 'Table Style Medium 9'
                })
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "cafeterias_db.xlsx")

    # Callback para descargar Ingredientes
    @app.callback(
        Output("descargar-ingredientes", "data"),
        Input("btn-download-ingredientes", "n_clicks"),
        prevent_initial_call=True,
    )
    def descargar_ingredientes(n_clicks):
        if n_clicks:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                workbook = writer.book
                sheet_name = 'Ingredientes'
                df_ingredientes = dataframes.get('ingredientes', pd.DataFrame())
                df_ingredientes.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                num_rows, num_cols = df_ingredientes.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df_ingredientes.columns],
                    'name': 'TablaIngredientes',
                    'style': 'Table Style Medium 9'
                })

                # Hoja 2: Análisis Opciones
                columnas_opciones = [col for col in df_ingredientes.columns if 'opcion_' in col]
                if columnas_opciones:
                    sheet_name = 'Análisis Opciones'
                    analisis_opciones = dataframes['ingredientes'][columnas_opciones].notna().sum().reset_index()
                    analisis_opciones.columns = ['Opción', 'Cantidad de Opciones']
                    analisis_opciones.to_excel(writer, index=False, sheet_name=sheet_name)
                    worksheet = writer.sheets[sheet_name]
                    num_rows, num_cols = analisis_opciones.shape
                    worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                        'columns': [{'header': col} for col in analisis_opciones.columns],
                        'name': 'TablaAnalisisOpciones',
                        'style': 'Table Style Medium 9'
                    })

            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "ingredientes.xlsx")

    # Callback para descargar Instituciones
    @app.callback(
        Output("descargar-instituciones", "data"),
        Input("btn-download-instituciones", "n_clicks"),
        prevent_initial_call=True,
    )
    def descargar_instituciones(n_clicks):
        if n_clicks:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                workbook = writer.book

                # Hoja 1: Instituciones
                sheet_name = 'Instituciones'
                df_instituciones = dataframes.get('instituciones', pd.DataFrame())
                df_instituciones.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets[sheet_name]
                num_rows, num_cols = df_instituciones.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in df_instituciones.columns],
                    'name': 'TablaInstituciones',
                    'style': 'Table Style Medium 9'
                })

                # Hoja 2: Análisis por Ciudad
                sheet_name = 'Análisis por Ciudad'
                inst_stats = dataframes['instituciones'].groupby(['ciudad', 'is_active']).size().unstack(fill_value=0).reset_index()
                inst_stats.to_excel(writer, index=False, sheet_name=sheet_name)
                worksheet = writer.sheets['Análisis por Ciudad']
                num_rows, num_cols = inst_stats.shape
                worksheet.add_table(0, 0, num_rows, num_cols - 1, {
                    'columns': [{'header': col} for col in inst_stats.columns],
                    'name': 'TablaAnalisisCiudad',
                    'style': 'Table Style Medium 9'
                })

            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "instituciones.xlsx")

    return app

# ================================================
# Función Principal
# ================================================

def main():
    """
    Función principal que orquesta la configuración, procesamiento de datos, creación de gráficos,
    configuración de la aplicación Dash y exportación de archivos Excel.
    """
    # Configurar AWS
    dynamodb, tablas = configurar_aws()

    # Escanear todas las tablas necesarias
    datos_ordenes = escanear_tabla(dynamodb, tablas['ordenes'])
    datos_usuarios_app = escanear_tabla(dynamodb, tablas['usuarios_app'])
    datos_usuarios = escanear_tabla(dynamodb, tablas['usuarios'])
    datos_cafeterias = escanear_tabla(dynamodb, tablas['cafeterias'])
    datos_ingredientes = escanear_tabla(dynamodb, tablas['ingredientes'])
    datos_instituciones = escanear_tabla(dynamodb, tablas['instituciones'])
    datos_productos = escanear_tabla(dynamodb, tablas['productos'])

    # Convertir datos a DataFrames de pandas
    df_ordenes = pd.DataFrame(datos_ordenes)
    df_usuarios_app = pd.DataFrame(datos_usuarios_app)
    df_usuarios = pd.DataFrame(datos_usuarios)
    df_cafeterias = pd.DataFrame(datos_cafeterias)
    df_ingredientes = pd.DataFrame(datos_ingredientes)
    df_instituciones = pd.DataFrame(datos_instituciones)
    df_productos = pd.DataFrame(datos_productos)

    # Convertir Decimal a float en ordenes antes de procesar
    if not df_ordenes.empty:
        df_ordenes = df_ordenes.apply(lambda col: col.map(convertir_decimal) if col.dtype == object else col)

    # Procesamiento de df_ordenes (primer script)
    df_ordenes = process_ordenes_data(df_ordenes)

    # Crear DataFrame de Órdenes Completadas (primer script)
    df_ordenes_completadas = df_ordenes[df_ordenes['orden_completada'].str.lower() == 'completada']

    # Procesamiento de productos basado en órdenes completadas (primer script)
    df_products = process_products_data(df_ordenes_completadas)

    # Procesamiento de cafeterías basado en órdenes completadas (primer script)
    df_cafeterias_resumen = process_cafeterias_data(df_ordenes_completadas)

    # Procesamiento de ingredientes (segundo script)
    df_ingredientes = procesar_ingredientes(df_ingredientes)

    # Procesamiento de productos numéricos (segundo script)
    df_productos = convertir_columnas_numericas_productos(df_productos)

    # Convertir Decimal a float en ingredientes y productos (segundo script)
    # Reemplazar applymap con apply y map para evitar FutureWarnings
    if not df_ingredientes.empty:
        df_ingredientes = df_ingredientes.apply(lambda col: col.map(convertir_decimal) if col.dtype == object else col)

    if not df_productos.empty:
        df_productos = df_productos.apply(lambda col: col.map(convertir_decimal) if col.dtype == object else col)

    # Crear figuras basadas en órdenes completadas y otras tablas
    figures_and_data = create_figures_integrado(df_ordenes_completadas, df_products, df_usuarios, df_usuarios_app, df_instituciones)

    # Crear un diccionario de dataframes
    dataframes_dict = {
        'ordenes': df_ordenes,  # Todas las órdenes
        'ordenes_completadas': df_ordenes_completadas,  # Solo órdenes completadas
        'products': df_products,
        'usuarios_app': df_usuarios_app,
        'usuarios': df_usuarios,
        'cafeterias': df_cafeterias_resumen,  # Resumen por cafeterías
        'ingredientes': df_ingredientes,
        'instituciones': df_instituciones,
        'productos': df_productos,
        'cafeterias_raw': df_cafeterias  # Datos raw de cafeterías
    }

    # Crear DataFrames de visualización sin excluir columnas
    df_ordenes_display = df_ordenes.copy()
    df_ordenes_completadas_display = df_ordenes_completadas.copy()

    # Convertir 'productos_json' a string en los DataFrames de visualización
    df_ordenes_display['productos_json'] = df_ordenes_display['productos_json'].apply(
        lambda x: json.dumps(convertir_decimal(x)) if isinstance(x, (dict, list)) else str(x)
    )
    df_ordenes_completadas_display['productos_json'] = df_ordenes_completadas_display['productos_json'].apply(
        lambda x: json.dumps(convertir_decimal(x)) if isinstance(x, (dict, list)) else str(x)
    )

    # Añadir dataframes de visualización al diccionario
    dataframes_dict['ordenes_display'] = df_ordenes_display
    dataframes_dict['ordenes_completadas_display'] = df_ordenes_completadas_display

    # Añadir dataframes adicionales de figures_and_data
    for key, value in figures_and_data.items():
        if key.startswith('df_') or key.startswith('fig_'):
            dataframes_dict[key] = value

    # Exportar todos los Excel antes de iniciar la aplicación
    exportar_a_excel_integrado(dataframes_dict)

    # Llamar a las nuevas funciones para exportar órdenes completadas filtradas
    export_ordenes_completadas_por_mes(df_ordenes_completadas)
    export_ordenes_completadas_rango_fecha(df_ordenes_completadas)
    
    # Nueva llamada para exportar órdenes clasificadas por cafeterías
    export_ordenes_completadas_por_cafeteria(df_ordenes_completadas)

    # Ejecutar la conversión completa si es necesario (función del primer script)
    convertir_completo()

    # Configurar la aplicación Dash
    app = setup_dash_app_integrado(figures_and_data, dataframes_dict)

    # Abrir el navegador automáticamente
    webbrowser.open('http://127.0.0.1:8050/')
    app.run_server(debug=True, use_reloader=False)
    renombrar_hojas()

if __name__ == '__main__':
    main()
