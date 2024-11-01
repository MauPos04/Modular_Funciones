import pandas as pd

def replace_ids_with_names(cafeterias_file, sales_file):
    # Leer el archivo de cafeterías (ahora en formato CSV)
    df_cafeterias = pd.read_csv(cafeterias_file)
    
    # Crear un diccionario de mapeo id -> nombre
    id_to_name = dict(zip(df_cafeterias['id'].astype(str), df_cafeterias['nombre']))
    
    # Leer el archivo de ventas
    df_sales = pd.read_excel(sales_file)
    
    # Verificar la existencia de la columna 'Cafeterias' antes de reemplazar IDs
    if 'Cafeterias' in df_sales.columns:
        # Reemplazar los IDs por nombres
        df_sales['Cafeterias'] = df_sales['Cafeterias'].map(id_to_name)
    else:
        print("Error: La columna 'Cafeterias' no se encuentra en el archivo de ventas.")
        return None
    
    # Guardar el resultado en un nuevo archivo
    output_file = 'ventas_por_cafeteria_monto_antes_y_despues_26-30.xlsx'
    df_sales.to_excel(output_file, index=False)
    
    print(f"Archivo guardado como: {output_file}")
    return df_sales

# Uso del script
cafeterias_file = 'cafeterias_20241029_185638.csv'
sales_file = 'Resumen_Cafeterias.xlsx'

result = replace_ids_with_names(cafeterias_file, sales_file)
if result is not None:
    print("\nResultado:")
    print(result)
