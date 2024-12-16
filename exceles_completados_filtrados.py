# exceles_completados_filtrados.py

import pandas as pd
import os
from datetime import datetime
from io import BytesIO

def export_ordenes_completadas_por_mes(df, output_dir='excel_exports', filename='Ordenes_Completadas_Por_Mes.xlsx'):
    """
    Exporta las Órdenes Completadas desde el 18 de octubre de 2023 en adelante,
    separadas por mes en hojas distintas de un archivo Excel con formato de tabla.
    """
    # Filtrar las órdenes completadas desde el 18 de octubre de 2023
    start_date = pd.to_datetime('2024-10-18')
    df['fecha_creacion_dt'] = pd.to_datetime(df['fecha_creacion_str'], format='%d/%m/%Y', errors='coerce')
    df_filtered = df[df['fecha_creacion_dt'] >= start_date]

    # Verificar si hay datos después del filtrado
    if df_filtered.empty:
        print("No hay Órdenes Completadas desde el 18 de octubre de 2023.")
        return

    # Agrupar por mes
    df_filtered['Mes'] = df_filtered['fecha_creacion_dt'].dt.strftime('%B %Y')  # e.g., 'Noviembre 2023'

    # Crear directorio si no existe
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Crear el archivo Excel con una hoja por mes
    excel_path = os.path.join(output_dir, filename)
    with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
        workbook = writer.book

        for mes, grupo in df_filtered.groupby('Mes'):
            # Formatear el nombre de la hoja al nombre del mes en español
            sheet_name = ''.join([c for c in mes if c.isalnum() or c in (' ', '_')])[:31]

            # Escribir el DataFrame en la hoja
            grupo.to_excel(writer, index=False, sheet_name=sheet_name)

            # Formatear como tabla
            worksheet = writer.sheets[sheet_name]
            (max_row, max_col) = grupo.shape
            worksheet.add_table(0, 0, max_row, max_col - 1, {
                'columns': [{'header': col} for col in grupo.columns],
                'name': f'Tabla_{mes[:15]}',
                'style': 'Table Style Medium 9'
            })

            # Ajustar el ancho de las columnas
            for i, col in enumerate(grupo.columns):
                max_length = max(grupo[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, max_length)

    print(f"Archivo Excel '{filename}' creado exitosamente en '{output_dir}' con hojas por mes.")

def export_ordenes_completadas_rango_fecha(df, start_date='2023-10-18', end_date=None, output_dir='excel_exports', filename='Ordenes_Completadas_Rango_Fecha.xlsx'):
    """
    Exporta las Órdenes Completadas desde una fecha de inicio hasta la fecha actual,
    en un archivo Excel con formato de tabla.
    """
    # Convertir strings a datetime
    start_datetime = pd.to_datetime(start_date)
    end_datetime = pd.to_datetime(end_date) if end_date else pd.to_datetime(datetime.now())

    # Filtrar las órdenes completadas en el rango de fechas
    df['fecha_creacion_dt'] = pd.to_datetime(df['fecha_creacion_str'], format='%d/%m/%Y', errors='coerce')
    df_filtered = df[(df['fecha_creacion_dt'] >= start_datetime) & (df['fecha_creacion_dt'] <= end_datetime)]

    # Verificar si hay datos después del filtrado
    if df_filtered.empty:
        print(f"No hay Órdenes Completadas desde el {start_date} hasta el {end_datetime.strftime('%Y-%m-%d')}.")
        return

    # Crear directorio si no existe
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Exportar a Excel con formato de tabla
    excel_path = os.path.join(output_dir, filename)
    with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
        workbook = writer.book
        sheet_name = 'Órdenes Completadas Rango Fecha'
        df_filtered.to_excel(writer, index=False, sheet_name=sheet_name)
        worksheet = writer.sheets[sheet_name]
        (max_row, max_col) = df_filtered.shape
        worksheet.add_table(0, 0, max_row, max_col - 1, {
            'columns': [{'header': col} for col in df_filtered.columns],
            'name': 'TablaOrdenesRangoFecha',
            'style': 'Table Style Medium 9'
        })

        # Ajustar el ancho de las columnas
        for i, col in enumerate(df_filtered.columns):
            max_length = max(df_filtered[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, max_length)

    print(f"Archivo Excel '{filename}' creado exitosamente en '{output_dir}' con Órdenes Completadas desde el {start_date} hasta el {end_datetime.strftime('%Y-%m-%d')}.")
