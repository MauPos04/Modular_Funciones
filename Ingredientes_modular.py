import boto3
import pandas as pd
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import json
from io import BytesIO
import webbrowser
from decimal import Decimal
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
import openpyxl

from config import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY

def configurar_aws():
    """
    Configura la conexión a AWS DynamoDB.

    Returns:
        dynamodb (boto3.resource): Recurso de DynamoDB.
        tablas (dict): Diccionario con los nombres de las tablas.
    """
    tablas = {
        'cafeterias': 'colosal-appu-cafeterias-pdn',
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

def escanear_tabla(dynamodb, table_name):
    """
    Escanea una tabla de DynamoDB y recupera los ítems.

    Args:
        dynamodb (boto3.resource): Recurso de DynamoDB.
        table_name (str): Nombre de la tabla a escanear.

    Returns:
        list: Lista de ítems recuperados de la tabla.
    """
    table = dynamodb.Table(table_name)
    datos = []
    try:
        response = table.scan()
        datos.extend(response.get('Items', []))
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            datos.extend(response.get('Items', []))
        print(f"Escaneo completado para la tabla {table_name}. Total de ítems: {len(datos)}")
        return datos
    except Exception as e:
        print(f"Error al escanear la tabla {table_name}: {e}")
        return []

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
                    lambda x: x[i]['precio'] if isinstance(x, list) and len(x) > i else None
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

def exportar_a_excel(df_cafeterias, df_ingredientes, df_instituciones, df_productos, output_dir='excel_exports', timestamp=True):
    """
    Exporta los DataFrames a archivos Excel en el directorio especificado.

    Args:
        df_cafeterias (pd.DataFrame): DataFrame de cafeterías.
        df_ingredientes (pd.DataFrame): DataFrame de ingredientes.
        df_instituciones (pd.DataFrame): DataFrame de instituciones.
        df_productos (pd.DataFrame): DataFrame de productos.
        output_dir (str, optional): Directorio de salida. Defaults to 'excel_exports'.
        timestamp (bool, optional): Añade timestamp al nombre del archivo. Defaults to True.

    Returns:
        dict: Diccionario con los paths de los archivos generados.
    """
    generated_files = {}
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    time_suffix = f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}" if timestamp else ""
    
    try:
        # 1. Exportar Cafeterías
        archivo_cafeterias = f'cafeterias.xlsx'
        ruta_cafeterias = os.path.join(output_dir, archivo_cafeterias)
        with pd.ExcelWriter(ruta_cafeterias, engine='xlsxwriter') as writer:
            df_cafeterias.to_excel(writer, index=False, sheet_name='Cafeterías')
            worksheet = writer.sheets['Cafeterías']
            for idx, col in enumerate(df_cafeterias.columns):
                max_length = max(df_cafeterias[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(idx, idx, min(max_length, 50))
        generated_files['cafeterias'] = ruta_cafeterias

        # 2. Exportar Ingredientes
        archivo_ingredientes = f'ingredientes.xlsx'
        ruta_ingredientes = os.path.join(output_dir, archivo_ingredientes)
        with pd.ExcelWriter(ruta_ingredientes, engine='xlsxwriter') as writer:
            workbook = writer.book
            number_format = workbook.add_format({'num_format': '#,##0.00'})
            df_ingredientes.to_excel(writer, index=False, sheet_name='Ingredientes')
            worksheet = writer.sheets['Ingredientes']
            # Formatear columnas de precios
            columnas_precios = [col for col in df_ingredientes.columns if 'precio' in col.lower()]
            for col in columnas_precios:
                idx = df_ingredientes.columns.get_loc(col)
                worksheet.set_column(idx, idx, 12, number_format)
            # Análisis de opciones
            if 'opciones' in df_ingredientes.columns or any('opcion_' in col for col in df_ingredientes.columns):
                columnas_opciones = [col for col in df_ingredientes.columns if 'opcion_' in col]
                analisis_opciones = df_ingredientes[columnas_opciones].notna().sum()
                analisis_opciones.to_frame('Cantidad de Opciones').to_excel(writer, sheet_name='Análisis Opciones')
        generated_files['ingredientes'] = ruta_ingredientes

        # 3. Exportar Instituciones
        archivo_instituciones = f'instituciones.xlsx'
        ruta_instituciones = os.path.join(output_dir, archivo_instituciones)
        with pd.ExcelWriter(ruta_instituciones, engine='xlsxwriter') as writer:
            df_instituciones.to_excel(writer, index=False, sheet_name='Instituciones')
            # Análisis por ciudad
            analisis_ciudad = df_instituciones.groupby(['ciudad', 'is_active']).size().unstack(fill_value=0)
            analisis_ciudad.to_excel(writer, sheet_name='Análisis por Ciudad')
            worksheet = writer.sheets['Instituciones']
            for idx, col in enumerate(df_instituciones.columns):
                worksheet.set_column(idx, idx, 15)
        generated_files['instituciones'] = ruta_instituciones

        # 4. Exportar Productos
        archivo_productos = f'productos.xlsx'
        ruta_productos = os.path.join(output_dir, archivo_productos)
        with pd.ExcelWriter(ruta_productos, engine='xlsxwriter') as writer:
            workbook = writer.book
            number_format = workbook.add_format({'num_format': '#,##0.00'})
            df_productos.to_excel(writer, index=False, sheet_name='Productos')
            worksheet = writer.sheets['Productos']
            # Formatear columnas numéricas
            if 'precio_unitario' in df_productos.columns:
                idx = df_productos.columns.get_loc('precio_unitario')
                worksheet.set_column(idx, idx, 12, number_format)
            if 'cantidad_disponible' in df_productos.columns:
                idx = df_productos.columns.get_loc('cantidad_disponible')
                worksheet.set_column(idx, idx, 12, number_format)
            # Análisis de productos
            estadisticas_productos = pd.DataFrame({
                'Total Productos': [len(df_productos)],
                'Productos con Stock': [(df_productos['cantidad_disponible'] > 0).sum()],
                'Precio Promedio': [df_productos['precio_unitario'].mean()],
                'Precio Máximo': [df_productos['precio_unitario'].max()],
                'Precio Mínimo': [df_productos['precio_unitario'].min()]
            })
            estadisticas_productos.to_excel(writer, sheet_name='Estadísticas', index=False)
        generated_files['productos'] = ruta_productos

        print("\nArchivos Excel generados exitosamente:")
        for clave, ruta in generated_files.items():
            print(f"- {clave}: {ruta}")

        return generated_files

    except Exception as e:
        print(f"\nError al generar los archivos Excel: {e}")
        return None

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

def configurar_app_dash(df_cafeterias, df_ingredientes, df_instituciones, df_productos):
    """
    Configura la aplicación Dash, definiendo el layout y registrando los callbacks.

    Args:
        df_cafeterias (pd.DataFrame): DataFrame de cafeterías.
        df_ingredientes (pd.DataFrame): DataFrame de ingredientes.
        df_instituciones (pd.DataFrame): DataFrame de instituciones.
        df_productos (pd.DataFrame): DataFrame de productos.

    Returns:
        dash.Dash: Instancia de la aplicación Dash.
    """
    app = dash.Dash(__name__)

    # Definir estilos
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

    # Layout de la aplicación
    app.layout = html.Div(children=[
        html.H1(children='Dashboard de Datos de la Base de Datos', style={'textAlign': 'center'}),

        dcc.Tabs([
            # Pestaña Cafeterías
            dcc.Tab(label='Cafeterías', children=[
                html.Div([
                    html.Div([
                        dcc.Input(
                            id='buscar-cafeterias',
                            type='text',
                            placeholder='Búsqueda global en cafeterías...',
                            style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                        ),
                    ]),
                    dash_table.DataTable(
                        id='tabla-cafeterias',
                        columns=[{'name': col, 'id': col, 'type': 'text'} for col in df_cafeterias.columns],
                        data=df_cafeterias.to_dict('records'),
                        page_size = 5,
                        sort_action='native',
                        sort_mode='multi',
                        style_table={'overflowX': 'auto'},
                        style_cell=estilo_celda,
                        style_header=estilo_header,
                        style_data=estilo_datos,
                    ),
                    html.Button("Descargar Cafeterías", id="btn-descargar-cafeterias", n_clicks=0),
                    dcc.Download(id="descargar-cafeterias")
                ])
            ]),

            # Pestaña Ingredientes
            dcc.Tab(label='Ingredientes', children=[
                html.Div([
                    html.Div([
                        dcc.Input(
                            id='buscar-ingredientes',
                            type='text',
                            placeholder='Búsqueda global en ingredientes...',
                            style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                        ),
                    ]),
                    dash_table.DataTable(
                        id='tabla-ingredientes',
                        columns=[{'name': col, 'id': col} for col in df_ingredientes.columns],
                        data=df_ingredientes.to_dict('records'),
                        page_size=5,
                        sort_action='native',
                        sort_mode='multi',
                        style_table={'overflowX': 'auto'},
                        style_cell=estilo_celda,
                        style_header=estilo_header,
                        style_data=estilo_datos,
                    ),
                    html.Button("Descargar Ingredientes", id="btn-descargar-ingredientes", n_clicks=0),
                    dcc.Download(id="descargar-ingredientes")
                ])
            ]),

            # Pestaña Instituciones
            dcc.Tab(label='Instituciones', children=[
                dcc.Tabs([
                    # Subpestaña Tabla de Datos
                    dcc.Tab(label='Tabla de Datos', children=[
                        html.Div([
                            html.Div([
                                dcc.Input(
                                    id='buscar-instituciones',
                                    type='text',
                                    placeholder='Búsqueda global en instituciones...',
                                    style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                                ),
                            ]),
                            dash_table.DataTable(
                                id='tabla-instituciones',
                                columns=[{'name': col, 'id': col} for col in df_instituciones.columns],
                                data=df_instituciones.to_dict('records'),
                                page_size=5,
                                sort_action='native',
                                sort_mode='multi',
                                style_table={'overflowX': 'auto'},
                                style_cell=estilo_celda,
                                style_header=estilo_header,
                                style_data=estilo_datos,
                            ),
                            html.Button("Descargar Instituciones", id="btn-descargar-instituciones", n_clicks=0),
                            dcc.Download(id="descargar-instituciones")
                        ])
                    ]),
                    # Subpestaña Gráficas
                    dcc.Tab(label='Gráficas', children=[
                        html.Div([
                            dcc.Graph(
                                id='grafico-instituciones',
                                figure=crear_grafico_instituciones(df_instituciones)
                            )
                        ])
                    ])
                ])
            ]),

            # Pestaña Productos
            dcc.Tab(label='Productos', children=[
                html.Div([
                    html.Div([
                        dcc.Input(
                            id='buscar-productos',
                            type='text',
                            placeholder='Búsqueda global en productos...',
                            style={'width': '100%', 'marginBottom': '10px', 'marginTop': '10px', 'padding': '8px'}
                        ),
                    ]),
                    dash_table.DataTable(
                        id='tabla-productos',
                        columns=[{'name': col, 'id': col} for col in df_productos.columns],
                        data=df_productos.to_dict('records'),
                        page_size=5,
                        sort_action='native',
                        sort_mode='multi',
                        style_table={'overflowX': 'auto'},
                        style_cell=estilo_celda,
                        style_header=estilo_header,
                        style_data=estilo_datos,
                    ),
                    html.Button("Descargar Productos", id="btn-descargar-productos", n_clicks=0),
                    dcc.Download(id="descargar-productos")
                ])
            ])
        ])
    ])

    # Callbacks para el filtrado
    @app.callback(
        Output('tabla-cafeterias', 'data'),
        [Input('buscar-cafeterias', 'value')]
    )
    def actualizar_tabla_cafeterias(valor_busqueda):
        if valor_busqueda:
            df_filtrado = df_cafeterias[df_cafeterias.astype(str).apply(
                lambda fila: fila.str.contains(valor_busqueda, case=False, na=False).any(),
                axis=1
            )]
            return df_filtrado.to_dict('records')
        return df_cafeterias.to_dict('records')

    @app.callback(
        Output('tabla-ingredientes', 'data'),
        [Input('buscar-ingredientes', 'value')]
    )
    def actualizar_tabla_ingredientes(valor_busqueda):
        if valor_busqueda:
            df_filtrado = df_ingredientes[df_ingredientes.astype(str).apply(
                lambda fila: fila.str.contains(valor_busqueda, case=False, na=False).any(),
                axis=1
            )]
            return df_filtrado.to_dict('records')
        return df_ingredientes.to_dict('records')

    @app.callback(
        Output('tabla-instituciones', 'data'),
        [Input('buscar-instituciones', 'value')]
    )
    def actualizar_tabla_instituciones(valor_busqueda):
        if valor_busqueda:
            df_filtrado = df_instituciones[df_instituciones.astype(str).apply(
                lambda fila: fila.str.contains(valor_busqueda, case=False, na=False).any(),
                axis=1
            )]
            return df_filtrado.to_dict('records')
        return df_instituciones.to_dict('records')

    @app.callback(
        Output('tabla-productos', 'data'),
        [Input('buscar-productos', 'value')]
    )
    def actualizar_tabla_productos(valor_busqueda):
        if valor_busqueda:
            df_filtrado = df_productos[df_productos.astype(str).apply(
                lambda fila: fila.str.contains(valor_busqueda, case=False, na=False).any(),
                axis=1
            )]
            return df_filtrado.to_dict('records')
        return df_productos.to_dict('records')

    # Callbacks para descargas
    @app.callback(
        Output("descargar-cafeterias", "data"),
        Input("btn-descargar-cafeterias", "n_clicks"),
        prevent_initial_call=True,
    )
    def descargar_cafeterias(n_clicks):
        if n_clicks:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_cafeterias.to_excel(writer, index=False, sheet_name='Cafeterías')
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "cafeterias.xlsx")

    @app.callback(
        Output("descargar-ingredientes", "data"),
        Input("btn-descargar-ingredientes", "n_clicks"),
        prevent_initial_call=True,
    )
    def descargar_ingredientes(n_clicks):
        if n_clicks:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_ingredientes.to_excel(writer, index=False, sheet_name='Ingredientes')
                # Análisis Opciones
                columnas_opciones = [col for col in df_ingredientes.columns if 'opcion_' in col]
                if columnas_opciones:
                    analisis_opciones = df_ingredientes[columnas_opciones].notna().sum()
                    analisis_opciones.to_frame('Cantidad de Opciones').to_excel(writer, sheet_name='Análisis Opciones')
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "ingredientes.xlsx")

    @app.callback(
        Output("descargar-instituciones", "data"),
        Input("btn-descargar-instituciones", "n_clicks"),
        prevent_initial_call=True,
    )
    def descargar_instituciones(n_clicks):
        if n_clicks:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_instituciones.to_excel(writer, index=False, sheet_name='Instituciones')
                # Análisis por Ciudad
                analisis_ciudad = df_instituciones.groupby(['ciudad', 'is_active']).size().unstack(fill_value=0)
                analisis_ciudad.to_excel(writer, sheet_name='Análisis por Ciudad')
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "instituciones.xlsx")

    @app.callback(
        Output("descargar-productos", "data"),
        Input("btn-descargar-productos", "n_clicks"),
        prevent_initial_call=True,
    )
    def descargar_productos(n_clicks):
        if n_clicks:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_productos.to_excel(writer, index=False, sheet_name='Productos')
                # Estadísticas de Productos
                estadisticas_productos = pd.DataFrame({
                    'Total Productos': [len(df_productos)],
                    'Productos con Stock': [(df_productos['cantidad_disponible'] > 0).sum()],
                    'Precio Promedio': [df_productos['precio_unitario'].mean()],
                    'Precio Máximo': [df_productos['precio_unitario'].max()],
                    'Precio Mínimo': [df_productos['precio_unitario'].min()]
                })
                estadisticas_productos.to_excel(writer, sheet_name='Estadísticas', index=False)
            buffer.seek(0)
            return dcc.send_bytes(buffer.read(), "productos.xlsx")

    return app

def principal():
    """
    Función principal que orquesta la configuración, procesamiento de datos,
    creación de gráficos, configuración de la aplicación Dash y exportación de archivos Excel.
    """
    # Configurar AWS
    dynamodb, tablas = configurar_aws()

    # Escanear tablas
    datos_cafeterias = escanear_tabla(dynamodb, tablas['cafeterias'])
    datos_ingredientes = escanear_tabla(dynamodb, tablas['ingredientes'])
    datos_instituciones = escanear_tabla(dynamodb, tablas['instituciones'])
    datos_productos = escanear_tabla(dynamodb, tablas['productos'])

    # Convertir a DataFrames
    df_cafeterias = pd.DataFrame(datos_cafeterias)
    df_ingredientes = pd.DataFrame(datos_ingredientes)
    df_instituciones = pd.DataFrame(datos_instituciones)
    df_productos = pd.DataFrame(datos_productos)

    # Convertir Decimal a float en ingredientes y productos
    df_ingredientes = df_ingredientes.applymap(convertir_decimal)
    df_productos = convertir_columnas_numericas(df_productos)

    # Procesar opciones en ingredientes
    df_ingredientes = procesar_ingredientes(df_ingredientes)

    # Exportar a Excel
    exportar_a_excel(df_cafeterias, df_ingredientes, df_instituciones, df_productos)

    # Configurar y ejecutar la aplicación Dash
    app = configurar_app_dash(df_cafeterias, df_ingredientes, df_instituciones, df_productos)

    # Abrir el navegador automáticamente
    webbrowser.open('http://127.0.0.1:8050/')
    app.run_server(debug=True, use_reloader=False)

if __name__ == '__main__':
    principal()
