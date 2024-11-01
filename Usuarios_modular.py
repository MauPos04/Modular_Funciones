import boto3
import pandas as pd
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import webbrowser
from io import BytesIO
import json
import numpy as np
import os
import openpyxl

from config import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY

def setup_aws():
    """
    Configura la conexión a AWS DynamoDB.

    Returns:
        dynamodb (boto3.resource): Recurso de DynamoDB.
        TABLE_ORDENES (str): Nombre de la tabla de órdenes.
        TABLE_USUARIOS_APP (str): Nombre de la tabla de usuarios de la app.
        TABLE_USUARIOS (str): Nombre de la tabla de usuarios.
    """
    # Definir nombres de las tablas
    TABLE_ORDENES = 'colosal-appu-ordenes-pdn'
    TABLE_USUARIOS_APP = 'colosal-appu-usuarios-app-pdn'
    TABLE_USUARIOS = 'colosal-appu-usuarios-pdn'

    # Conectar a DynamoDB
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    return dynamodb, TABLE_ORDENES, TABLE_USUARIOS_APP, TABLE_USUARIOS

def scan_table(dynamodb, table_name, limit=None):
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
                print(f"Escaneados {total_scanned} items de {table_name}")
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

        # Convertir columnas de fecha/hora a datetime
        df_ordenes['fecha_creacion_dt'] = pd.to_datetime(df_ordenes['fecha_creacion'], errors='coerce', dayfirst=True)
        df_ordenes['fecha_terminacion_dt'] = pd.to_datetime(df_ordenes['fecha_terminacion'], errors='coerce', dayfirst=True)
        df_ordenes['hora_recogida_dt'] = pd.to_datetime(df_ordenes['hora_recogida'], errors='coerce').dt.time

        # Crear columna 'hora_creacion' extrayendo la hora
        df_ordenes['hora_creacion'] = df_ordenes['fecha_creacion_dt'].dt.strftime('%H:%M:%S')

        # Crear columnas de cadena formateadas para visualización
        df_ordenes['fecha_creacion_str'] = df_ordenes['fecha_creacion_dt'].dt.strftime('%d/%m/%Y')
        df_ordenes['fecha_terminacion_str'] = df_ordenes['fecha_terminacion_dt'].dt.strftime('%d/%m/%Y')
        df_ordenes['hora_recogida_str'] = df_ordenes['hora_recogida_dt'].astype(str)

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
            'observacion'
        ]

        # Reordenar las columnas del DataFrame según el orden deseado
        df_ordenes = df_ordenes[desired_columns]

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
        df_ordenes_2 = df_ordenes[['id_orden', 'nombre_cliente', 'productos_json', 'fecha_creacion_str', 'hora_creacion', 'monto']].copy()

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
        df_products['precioUnitario'] = pd.to_numeric(df_products['precioUnitario'], errors='coerce').fillna(0)
        df_products['precioTotal'] = pd.to_numeric(df_products['precioTotal'], errors='coerce').fillna(0)

        # Reordenar columnas según lo especificado, incluyendo 'id_orden'
        desired_product_columns = ['id_orden', 'nombre_cliente', 'producto', 'cantidad', 'precioUnitario', 'precioTotal', 'fecha_creacion_str', 'hora_creacion', 'monto']
        df_products = df_products[desired_product_columns]

    return df_products

def create_figures(df_ordenes_completadas, df_products, df_usuarios, df_usuarios_app):
    """
    Crea las figuras de Plotly necesarias para el dashboard basándose en órdenes completadas.

    Args:
        df_ordenes_completadas (pd.DataFrame): DataFrame de órdenes completadas.
        df_products (pd.DataFrame): DataFrame de productos procesado.
        df_usuarios (pd.DataFrame): DataFrame de usuarios.
        df_usuarios_app (pd.DataFrame): DataFrame de usuarios de la app.

    Returns:
        dict: Diccionario con las figuras y dataframes adicionales necesarias para los downloads.
    """
    figures_and_data = {}

    # Crear DataFrame para gráficas
    df_graph = df_ordenes_completadas.copy()

    # Convertir 'fecha_creacion_dt' a datetime si es necesario
    if 'fecha_creacion_dt' not in df_graph.columns:
        df_graph['fecha_creacion_dt'] = pd.to_datetime(df_graph['fecha_creacion_str'], format='%d/%m/%Y', errors='coerce')

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
    df_products['fecha_creacion_dt'] = pd.to_datetime(df_products['fecha_creacion_str'], format='%d/%m/%Y', errors='coerce')
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
    one_month_ago = current_date - pd.DateOffset(months=1)
    df_products_last_month = df_products[df_products['fecha_creacion_dt'] >= one_month_ago]
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
        color_discrete_sequence=px.colors.sequential.Viridis
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
        color_discrete_sequence=px.colors.sequential.Viridis
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
        x='producto',
        y='Total_Cantidad',
        title='Top 10 Productos Más Populares (Órdenes Completadas)',
        labels={'producto': 'Producto', 'Total_Cantidad': 'Cantidad Total'},
        text='Total_Cantidad',
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    fig_product_popularity.update_layout(
        xaxis=dict(
            tickangle=45
        ),
        yaxis=dict(
            title='Cantidad Total'
        ),
        height=600
    )
    figures_and_data['fig_product_popularity'] = fig_product_popularity

    # Estado del pedido y comprobante de pago
    # Dado que todas las órdenes en df_ordenes_completadas ya están completadas, este gráfico puede no ser necesario
    # Pero si 'comprobante_pago' varía, mantenerlo

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
        color_discrete_sequence=px.colors.sequential.Viridis
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
    df_peak_hours['fecha_creacion_dt'] = pd.to_datetime(df_peak_hours['fecha_creacion_str'], format='%d/%m/%Y', errors='coerce')

    # Obtener el mes más reciente
    fecha_mas_reciente = df_peak_hours['fecha_creacion_dt'].max()
    mes_mas_reciente = fecha_mas_reciente.replace(day=1)

    # Filtrar solo el mes más reciente
    df_peak_hours = df_peak_hours[df_peak_hours['fecha_creacion_dt'] >= mes_mas_reciente]

    # Procesar las horas
    df_peak_hours['hora'] = pd.to_datetime(df_peak_hours['hora_creacion'], format='%H:%M:%S', errors='coerce').dt.hour

    df_peak_hours = df_peak_hours.groupby('hora').size().reset_index(name='Cantidad de Órdenes').sort_values(by='hora')
    figures_and_data['df_peak_hours'] = df_peak_hours

    mes_nombre = fecha_mas_reciente.strftime('%B %Y')

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

    return figures_and_data

def process_cafeterias_data(df_ordenes_completadas):
    """
    Procesa el DataFrame de órdenes para crear un resumen por cafeterías basándose en órdenes completadas.

    Args:
        df_ordenes_completadas (pd.DataFrame): DataFrame de órdenes completadas.

    Returns:
        pd.DataFrame: DataFrame con el resumen por cafeterías
    """
    print("\nIniciando procesamiento de cafeterías...")

    # 1. Crear una copia del DataFrame original (ya filtrado)
    df_octubre = df_ordenes_completadas.copy()
    print(f"Registros totales en el DataFrame filtrado: {len(df_octubre)}")

    # 2. Convertir las fechas correctamente (formato europeo)
    df_octubre['fecha_creacion_dt'] = pd.to_datetime(df_octubre['fecha_creacion_str'], format='%d/%m/%Y', errors='coerce')

    print("\nEjemplo de fechas convertidas:")
    print(df_octubre['fecha_creacion_dt'].head())

    # 3. Definir fechas de inicio y fin del rango
    fecha_inicio = pd.Timestamp('2024-10-26')
    fecha_fin = pd.Timestamp('2024-10-30')

    print(f"\nFecha de inicio: {fecha_inicio.strftime('%d/%m/%Y')}")
    print(f"Fecha de fin: {fecha_fin.strftime('%d/%m/%Y')}")

    # 4. Aplicar filtros de rango de fechas
    df_octubre = df_octubre[
        (df_octubre['fecha_creacion_dt'] >= fecha_inicio) & 
        (df_octubre['fecha_creacion_dt'] <= fecha_fin)
    ]

    # Mostrar información sobre el filtrado
    print(f"Registros que cumplen los criterios de fechas especificadas:")
    print(f"Total de registros: {len(df_octubre)}")

    # Mostrar el rango de fechas para verificación
    if not df_octubre.empty:
        print(f"\nRango de fechas en el DataFrame filtrado:")
        print(f"Fecha más antigua: {df_octubre['fecha_creacion_dt'].min().strftime('%d/%m/%Y')}")
        print(f"Fecha más reciente: {df_octubre['fecha_creacion_dt'].max().strftime('%d/%m/%Y')}")
    else:
        print("\nNo se encontraron registros que cumplan los criterios de filtrado")
        return pd.DataFrame()  # Retornar DataFrame vacío si no hay datos

    # 5. Convertir las columnas numéricas
    df_octubre['monto'] = pd.to_numeric(df_octubre['monto'], errors='coerce')
    df_octubre['tasa'] = pd.to_numeric(df_octubre['tasa'], errors='coerce')

    # 6. Agrupar por cafetería y calcular totales
    df_cafeterias = df_octubre.groupby('cafeteria').agg({
        'monto': 'sum',
        'tasa': 'sum'
    }).reset_index()

    # 7. Calcular monto sin tasa
    df_cafeterias['monto_sin_tasa'] = df_cafeterias['monto'] - df_cafeterias['tasa']

    # 8. Ordenar por monto sin tasa de mayor a menor
    df_cafeterias = df_cafeterias.sort_values('monto_sin_tasa', ascending=False)

    # 9. Añadir fila de total
    total_row = pd.DataFrame({
        'cafeteria': ['Total 26/10/2024 - 30/10/2024 (Órdenes Completadas)'],
        'monto': [df_cafeterias['monto'].sum()],
        'tasa': [df_cafeterias['tasa'].sum()],
        'monto_sin_tasa': [df_cafeterias['monto_sin_tasa'].sum()]
    })

    df_cafeterias = pd.concat([df_cafeterias, total_row], ignore_index=True)

    # 10. Renombrar columnas para mejor presentación
    df_cafeterias = df_cafeterias.rename(columns={
        'cafeteria': 'Cafeterias',
        'monto': 'Monto con Tasa',
        'monto_sin_tasa': 'Monto sin Tasa'
    })

    # 11. Seleccionar y reordenar columnas finales
    df_cafeterias = df_cafeterias[['Cafeterias', 'Monto con Tasa', 'Monto sin Tasa']]

    # 12. Redondear los valores numéricos
    df_cafeterias['Monto con Tasa'] = df_cafeterias['Monto con Tasa'].round(0)
    df_cafeterias['Monto sin Tasa'] = df_cafeterias['Monto sin Tasa'].round(0)

    print("\nResumen final de cafeterías (solo órdenes completadas):")
    print(df_cafeterias)

    return df_cafeterias

def export_all_excel_files(dataframes, output_dir='excel_exports', timestamp=True):
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

    # Obtener el mes actual
    current_month = datetime.now().strftime('%B')

    # Diccionario para almacenar los paths de los archivos generados
    generated_files = {}

    try:
        # 1. Exportar Órdenes con segmentaciones
        filename = f'ordenes_segmentadas.xlsx'
        excel_path = os.path.join(output_dir, filename)
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            # Hoja 1: Órdenes
            dataframes['ordenes'].to_excel(writer, index=False, sheet_name='Órdenes')

            # Hoja 2: Órdenes Completadas
            dataframes['ordenes_completadas'].to_excel(writer, index=False, sheet_name='Órdenes Completadas')

            # Hoja 3: Volumen de pedidos por fecha
            dataframes['df_count'].to_excel(writer, index=False, sheet_name='Volumen de pedidos por fecha')

            # Hoja 4: Popularidad del producto
            dataframes['df_product_popularity'].to_excel(writer, index=False, sheet_name='Popularidad del producto')

            # Hoja 5: Ingresos por institución
            dataframes['df_revenue_institution'].to_excel(writer, index=False, sheet_name='Ingresos por institución')

            # Hoja 6: Para llevar vs en sitio
            dataframes['df_order_type'].to_excel(writer, index=False, sheet_name='Para llevar o en el lugar')

            # Hoja 7: Horas punta
            dataframes['df_peak_hours'].to_excel(writer, index=False, sheet_name='Horas punta')

        generated_files['ordenes'] = excel_path

        # 2. Exportar Detalle de Productos
        filename = f'detalle_productos.xlsx'
        excel_path = os.path.join(output_dir, filename)
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            dataframes['products'].to_excel(writer, index=False, sheet_name='Detalle de Productos')

        generated_files['productos'] = excel_path

        # 3. Exportar Usuarios App
        filename = f'usuarios_app.xlsx'
        excel_path = os.path.join(output_dir, filename)
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            dataframes['usuarios_app'].to_excel(writer, index=False, sheet_name='Usuarios_App')

        generated_files['usuarios_app'] = excel_path

        # 4. Exportar Usuarios
        filename = f'usuarios.xlsx'
        excel_path = os.path.join(output_dir, filename)
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            dataframes['usuarios'].to_excel(writer, index=False, sheet_name='Usuarios')

        generated_files['usuarios'] = excel_path

        # 5. Exportar Resumen de Cafeterías
        filename = f'{current_month}WompiCafeterias.xlsx'
        excel_path = os.path.join(output_dir, filename)

        # Procesar datos de cafeterías
        df_cafeterias = dataframes.get('cafeterias', pd.DataFrame())

        if not df_cafeterias.empty:
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                df_cafeterias.to_excel(writer, index=False, sheet_name='Resumen Cafeterías')

                # Obtener el objeto workbook y worksheet
                workbook = writer.book
                worksheet = writer.sheets['Resumen Cafeterías']

                # Dar formato a las columnas
                money_format = workbook.add_format({'num_format': '$#,##0'})
                worksheet.set_column('B:C', 20, money_format)  # Formato para las columnas de monto

            generated_files['cafeterias'] = excel_path
        else:
            print("No se encontraron datos para las cafeterías. Archivo no generado.")

        print("Archivos Excel generados exitosamente:")
        for key, path in generated_files.items():
            print(f"- {key}: {path}")

        return generated_files

    except Exception as e:
        print(f"Error al generar los archivos Excel: {str(e)}")
        return None

def setup_dash_app(figures_and_data, dataframes):
    """
    Configura la aplicación Dash, definiendo el layout y registrando los callbacks.

    Args:
        figures_and_data (dict): Diccionario con figuras y dataframes adicionales.
        dataframes (dict): Diccionario con los DataFrames principales.

    Returns:
        dash.Dash: Instancia de la aplicación Dash.
    """
    app = dash.Dash(__name__, suppress_callback_exceptions=True)  # Añadir suppress_callback_exceptions=True

    # Combinar dataframes con figures_and_data
    dataframes.update(figures_and_data)

    # Definir columnas deseadas para usuarios
    desired_user_columns = [
        'id',
        'username',
        'first_name',
        'last_name',
        'email',
        'password',
        'phone_number',
        'date_joined',
        'last_login',
        'is_active',
        'is_staff',
        'is_admin',
        'is_superadmin'
    ]

    # Reordenar y rellenar df_usuarios
    df_usuarios = dataframes['usuarios']
    missing_columns = [col for col in desired_user_columns if col not in df_usuarios.columns]
    if missing_columns:
        print(f"Las siguientes columnas faltan en df_usuarios y se rellenarán con valores vacíos: {missing_columns}")
        for col in missing_columns:
            df_usuarios[col] = np.nan
    df_usuarios = df_usuarios[desired_user_columns]
    dataframes['usuarios'] = df_usuarios

    # Definir el layout de la aplicación Dash con pestañas
    app.layout = html.Div(children=[
        html.H1(children='Dashboard de Datos de la Base de Datos', style={'textAlign': 'center'}),

        dcc.Tabs([
            # Pestaña 1: Órdenes y Gráficas
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
                                columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes['ordenes'].columns],
                                data=dataframes['ordenes'].to_dict('records'),
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
                                columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes['ordenes_completadas'].columns],
                                data=dataframes['ordenes_completadas'].to_dict('records'),
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
                                figure=dataframes['fig_main']
                            ),
                            dcc.Graph(
                                id='orden-completadas_2',
                                figure=dataframes['fig_orden']
                            )
                        ])
                    ])
                ])
            ]),

            # Pestaña 2: Detalle de Productos con subpestañas
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
                                columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes['products'].columns],
                                data=dataframes['products'].to_dict('records'),
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
                            ),
                            html.Button("Descargar Detalle de Productos a Excel", id="btn-download-productos", n_clicks=0),
                            dcc.Download(id="download-productos"),
                        ])
                    ]),
                    # Subpestaña 2.2: Gráficas de Productos
                    dcc.Tab(label='Gráficas de Productos', children=[
                        html.Div([
                            dcc.Graph(
                                id='productos-bar-chart',
                                figure=dataframes['fig_bar_products']
                            ),
                            dcc.Graph(
                                id='product-popularity',
                                figure=dataframes['fig_product_popularity']
                            ),
                        ])
                    ]),
                    # Subpestaña 2.3: Análisis de Ventas
                    dcc.Tab(label='Análisis de Ventas', children=[
                        html.Div([
                            dcc.Graph(
                                id='revenue-institution',
                                figure=dataframes['fig_revenue_institution']
                            ),
                            dcc.Graph(
                                id='order-type',
                                figure=dataframes['fig_order_type']
                            ),
                            dcc.Graph(
                                id='peak-hours',
                                figure=dataframes['fig_peak_hours']
                            ),
                        ])
                    ])
                ])
            ]),

            # Pestaña 3: Usuarios App (sin subpestañas)
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
                        columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes['usuarios_app'].columns],
                        data=dataframes['usuarios_app'].to_dict('records'),
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
                    ),
                    html.Button("Descargar Usuarios App a Excel", id="btn-download-usuarios-app", n_clicks=0),
                    dcc.Download(id="download-usuarios-app"),
                ])
            ]),

            # Pestaña 4: Usuarios (sin subpestañas)
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
                        columns=[{'name': i.replace('_', ' ').capitalize(), 'id': i} for i in dataframes['usuarios'].columns],
                        data=dataframes['usuarios'].to_dict('records'),
                        page_size=10,
                        sort_action='native',
                        sort_mode='multi',
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'textAlign': 'left',
                            'minWidth': '100px',
                            'width': '150px',
                            'maxWidth': '180px',
                        },
                    ),
                    html.Button("Descargar Usuarios a Excel", id="btn-download-usuarios", n_clicks=0),
                    dcc.Download(id="download-usuarios"),
                ])
            ]),

            # Pestaña 5: Resumen de Cafeterías
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
                        columns=[{'name': i, 'id': i} for i in dataframes['cafeterias'].columns],
                        data=dataframes['cafeterias'].to_dict('records'),
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
                    ),
                    html.Button("Descargar Resumen de Cafeterías a Excel", id="btn-download-cafeterias", n_clicks=0),
                    dcc.Download(id="download-cafeterias"),
                ])
            ]),
        ])
    ])

    # Callbacks para el filtrado
    @app.callback(
        Output('ordenes-table_1', 'data'),
        [Input('search-ordenes', 'value')]
    )
    def update_ordenes_table(search_value):
        filtered_df = filter_dataframe(dataframes['ordenes'], search_value)
        return filtered_df.to_dict('records')

    @app.callback(
        Output('ordenes-table_2', 'data'),
        [Input('search-productos', 'value')]
    )
    def update_productos_table(search_value):
        filtered_df = filter_dataframe(dataframes['products'], search_value)
        return filtered_df.to_dict('records')

    @app.callback(
        Output('ordenes-completadas-table', 'data'),
        [Input('search-ordenes-completadas', 'value')]
    )
    def update_ordenes_completadas_table(search_value):
        filtered_df = filter_dataframe(dataframes['ordenes_completadas'], search_value)
        return filtered_df.to_dict('records')

    @app.callback(
        Output('usuarios-app-table_2', 'data'),
        [Input('search-usuarios-app', 'value')]
    )
    def update_usuarios_app_table(search_value):
        filtered_df = filter_dataframe(dataframes['usuarios_app'], search_value)
        return filtered_df.to_dict('records')

    @app.callback(
        Output('usuarios-table_3', 'data'),
        [Input('search-usuarios', 'value')]
    )
    def update_usuarios_table(search_value):
        filtered_df = filter_dataframe(dataframes['usuarios'], search_value)
        return filtered_df.to_dict('records')

    @app.callback(
        Output('cafeterias-table', 'data'),
        [Input('search-cafeterias', 'value')]
    )
    def update_cafeterias_table(search_value):
        filtered_df = filter_dataframe(dataframes['cafeterias'], search_value)
        return filtered_df.to_dict('records')

    # Callbacks para descargar las tablas a Excel

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
                # Hoja 1: Órdenes
                dataframes['ordenes'].to_excel(writer, index=False, sheet_name='Órdenes')
                
                # Hoja 2: Órdenes Completadas
                dataframes['ordenes_completadas'].to_excel(writer, index=False, sheet_name='Órdenes Completadas')

                # Hoja 3: Volumen de pedidos por fecha
                dataframes['df_count'].to_excel(writer, index=False, sheet_name='Volumen de pedidos por fecha')

                # Hoja 4: Popularidad del producto
                dataframes['df_product_popularity'].to_excel(writer, index=False, sheet_name='Popularidad del producto')

                # Hoja 5: Ingresos por institución
                dataframes['df_revenue_institution'].to_excel(writer, index=False, sheet_name='Ingresos por institución')

                # Hoja 6: Para llevar vs en sitio
                dataframes['df_order_type'].to_excel(writer, index=False, sheet_name='Para llevar o en el lugar')

                # Hoja 7: Horas punta
                dataframes['df_peak_hours'].to_excel(writer, index=False, sheet_name='Horas punta')

            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "ordenes_segmentadas.xlsx")

    # Callback para descargar el detalle de productos
    @app.callback(
        Output("download-productos", "data"),
        Input("btn-download-productos", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_productos(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                dataframes['products'].to_excel(writer, index=False, sheet_name='Detalle de Productos')
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "detalle_productos.xlsx")

    # Callback para descargar la tabla de órdenes completadas
    @app.callback(
        Output("download-ordenes-completadas", "data"),
        Input("btn-download-ordenes-completadas", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_ordenes_completadas(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                dataframes['ordenes_completadas'].to_excel(writer, index=False, sheet_name='Órdenes Completadas')
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "ordenes_completadas.xlsx")

    # Callback para descargar la tabla de usuarios de la app
    @app.callback(
        Output("download-usuarios-app", "data"),
        Input("btn-download-usuarios-app", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_usuarios_app(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                dataframes['usuarios_app'].to_excel(writer, index=False, sheet_name='Usuarios_App')
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "usuarios_app.xlsx")

    # Callback para descargar la tabla de usuarios
    @app.callback(
        Output("download-usuarios", "data"),
        Input("btn-download-usuarios", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_usuarios(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                dataframes['usuarios'].to_excel(writer, index=False, sheet_name='Usuarios')
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "usuarios.xlsx")

    # Callback para descargar el resumen de cafeterías
    @app.callback(
        Output("download-cafeterias", "data"),
        Input("btn-download-cafeterias", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_cafeterias(n_clicks):
        if n_clicks > 0:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                dataframes['cafeterias'].to_excel(writer, index=False, sheet_name='Resumen Cafeterias')
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "Resumen_Cafeterias.xlsx")

    return app

def main():
    """
    Función principal que orquesta la configuración, procesamiento de datos, creación de gráficos,
    configuración de la aplicación Dash y exportación de archivos Excel.
    """
    # Configurar AWS
    dynamodb, TABLE_ORDENES, TABLE_USUARIOS_APP, TABLE_USUARIOS = setup_aws()

    # Recuperar datos de DynamoDB
    data_ordenes = scan_table(dynamodb, TABLE_ORDENES)
    data_usuarios_app = scan_table(dynamodb, TABLE_USUARIOS_APP)
    data_usuarios = scan_table(dynamodb, TABLE_USUARIOS)

    # Convertir datos a DataFrames de pandas
    df_ordenes = pd.DataFrame(data_ordenes)
    df_usuarios_app = pd.DataFrame(data_usuarios_app)
    df_usuarios = pd.DataFrame(data_usuarios)

    # Procesamiento de df_ordenes
    df_ordenes = process_ordenes_data(df_ordenes)

    # Crear DataFrame de Órdenes Completadas
    df_ordenes_completadas = df_ordenes[df_ordenes['orden_completada'].str.lower() == 'completada']

    # Procesamiento de productos basado en órdenes completadas
    df_products = process_products_data(df_ordenes_completadas)

    # Procesamiento de cafeterías basado en órdenes completadas
    df_cafeterias = process_cafeterias_data(df_ordenes_completadas)

    # Crear figuras basadas en órdenes completadas
    figures_and_data = create_figures(df_ordenes_completadas, df_products, df_usuarios, df_usuarios_app)

    # Crear un diccionario de dataframes
    dataframes = {
        'ordenes': df_ordenes,  # Todas las órdenes
        'ordenes_completadas': df_ordenes_completadas,  # Solo órdenes completadas
        'products': df_products,
        'usuarios_app': df_usuarios_app,
        'usuarios': df_usuarios,
        'cafeterias': df_cafeterias  # Resumen por cafeterías
    }

    # Añadir dataframes adicionales de figures_and_data
    for key, value in figures_and_data.items():
        if key.startswith('df_'):
            dataframes[key] = value

    # Crear figuras
    figures = {k: v for k, v in figures_and_data.items() if k.startswith('fig_')}

    # Configurar la aplicación Dash
    app = setup_dash_app(figures_and_data, dataframes)

    # Exportar todos los Excel antes de iniciar la aplicación
    export_all_excel_files(dataframes)

    # Iniciar la aplicación web
    webbrowser.open('http://127.0.0.1:8050/')
    app.run_server(debug=True, use_reloader=False)

if __name__ == '__main__':
    main()
