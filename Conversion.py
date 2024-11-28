import pandas as pd
import os

def convertir_completo():
    # Crear la carpeta 'excel_exports' si no existe
    if not os.path.exists('excel_exports'):
        os.makedirs('excel_exports')

    def replace_ids_with_names(cafeterias_file, sales_file):
        # Crear las rutas completas para ambos archivos
        cafeterias_path = os.path.join('excel_exports', cafeterias_file)
        sales_path = os.path.join('excel_exports', sales_file)
        
        try:
            # Leer el archivo de cafeterías en formato Excel
            df_cafeterias = pd.read_excel(cafeterias_path)
        except FileNotFoundError:
            print(f"Error: No se encontró el archivo {cafeterias_path}")
            return None
        
        try:
            # Leer el archivo de ventas
            df_sales = pd.read_excel(sales_path)
        except FileNotFoundError:
            print(f"Error: No se encontró el archivo {sales_path}")
            return None
        
        # Crear un diccionario de mapeo id -> nombre
        id_to_name = dict(zip(df_cafeterias['id'].astype(str), df_cafeterias['nombre']))
        
        # Verificar la existencia de la columna 'Cafeterias' antes de reemplazar IDs
        if 'Cafeterias' in df_sales.columns:
            # Reemplazar los IDs por nombres
            df_sales['Cafeterias'] = df_sales['Cafeterias'].map(id_to_name)
        else:
            print("Error: La columna 'Cafeterias' no se encuentra en el archivo de ventas.")
            return None
        
        # Crear la ruta completa para el archivo de salida
        output_file = os.path.join('excel_exports', 'ventas_por_cafeteria_monto.xlsx')
        
        # Guardar el resultado en un nuevo archivo
        df_sales.to_excel(output_file, index=False)
        
        print(f"Archivo guardado como: {output_file}")
        return df_sales

    # Uso del script
    cafeterias_file = 'cafeterias_db.xlsx'
    sales_file = 'NoviembreWompiCafeterias.xlsx'

    result = replace_ids_with_names(cafeterias_file, sales_file)
    if result is not None:
        print("\nResultado:")
        print(result)