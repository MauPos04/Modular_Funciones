# excel_clasificacion_cafeterias.py

import pandas as pd
import os
from io import BytesIO
from datetime import datetime

def export_ordenes_completadas_por_cafeteria(df, output_dir='excel_exports', filename='Ordenes_Completadas_Por_Cafeteria.xlsx'):
    """
    Exporta las Órdenes Completadas clasificadas por cafeterías a un archivo Excel,
    con cada cafetería en una hoja separada y formato de tabla.
    Filtra las órdenes entre el 18 de octubre de 2024 y la fecha actual.

    Args:
        df (pd.DataFrame): DataFrame con las órdenes completadas.
        output_dir (str, optional): Directorio para guardar el archivo Excel. Defaults to 'excel_exports'.
        filename (str, optional): Nombre del archivo Excel. Defaults to 'Ordenes_Completadas_Por_Cafeteria.xlsx'.
    """
    # Definir el rango de fechas
    start_date = pd.to_datetime('2024-10-18')
    end_date = pd.to_datetime(datetime.now())

    # Filtrar el DataFrame por el rango de fechas
    df_filtered = df[(df['fecha_creacion_dt'] >= start_date) & (df['fecha_creacion_dt'] <= end_date)]

    # Verificar que el DataFrame filtrado no esté vacío
    if df_filtered.empty:
        print("No hay Órdenes Completadas para exportar en el rango de fechas especificado.")
        return

    # Agrupar por cafetería
    grouped = df_filtered.groupby('cafeteria')

    # Crear directorio si no existe
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Crear el archivo Excel con una hoja por cafetería
    excel_path = os.path.join(output_dir, filename)
    with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
        workbook = writer.book

        for cafeteria, group in grouped:
            # Reemplazar caracteres no permitidos en nombres de hojas
            sheet_name = ''.join([c for c in cafeteria if c.isalnum() or c in (' ', '_')])[:31]  # Excel limita los nombres a 31 caracteres

            # Escribir el DataFrame en la hoja
            group.to_excel(writer, index=False, sheet_name=sheet_name)

            # Formatear como tabla
            worksheet = writer.sheets[sheet_name]
            (max_row, max_col) = group.shape
            worksheet.add_table(0, 0, max_row, max_col - 1, {
                'columns': [{'header': col} for col in group.columns],
                'name': f'Tabla_{cafeteria[:15]}',  # Nombre de la tabla limitado
                'style': 'Table Style Medium 9'
            })

            # Ajustar el ancho de las columnas
            for i, col in enumerate(group.columns):
                max_length = max(group[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, max_length)

    print(f"Archivo Excel '{filename}' creado exitosamente en '{output_dir}' con hojas por cafetería dentro del rango de fechas especificado.")
