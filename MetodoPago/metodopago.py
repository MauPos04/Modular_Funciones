import pandas as pd
import boto3
from datetime import datetime
import os
from config import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY

def scan_table(dynamodb, table_name, limit=None):
    table = dynamodb.Table(table_name)
    items = []
    total_scanned = 0
    
    try:
        scan_kwargs = {'ReturnConsumedCapacity': 'TOTAL'}
        if limit:
            scan_kwargs['Limit'] = limit
            
        done = False
        start_key = None
        
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
                
            response = table.scan(**scan_kwargs)
            items.extend(response.get('Items', []))
            total_scanned += len(response.get('Items', []))
            
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
            
            if limit and total_scanned >= limit:
                break
                
        return items
        
    except Exception as e:
        print(f"Error al escanear la tabla: {str(e)}")
        raise e

def map_cafeteria_names(accounts_df, cafeterias_csv_path):
    cafeterias_df = pd.read_csv(cafeterias_csv_path)
    cafeteria_names = dict(zip(cafeterias_df['id'].astype(str), cafeterias_df['nombre']))
    accounts_df['nombre_cafeteria'] = accounts_df['cafeteria'].map(cafeteria_names)
    
    cafeteria_idx = accounts_df.columns.get_loc('cafeteria')
    columns = list(accounts_df.columns)
    columns.insert(cafeteria_idx + 1, columns.pop(-1))
    accounts_df = accounts_df[columns]
    
    return accounts_df

def main():
    # Configurar DynamoDB
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    TABLE_NAME = 'colosal-appu-metodos-pago-pdn'
    
    # Leer datos de DynamoDB
    print("Leyendo datos de DynamoDB...")
    data = scan_table(dynamodb, TABLE_NAME)
    accounts_df = pd.DataFrame(data)
    
    # Ruta al archivo CSV de cafeterías
    cafeterias_csv = 'cafeterias_20241029_185638.csv'
    
    # Mapear nombres de cafeterías
    print("Mapeando nombres de cafeterías...")
    result_df = map_cafeteria_names(accounts_df, cafeterias_csv)
    
    # Crear directorio para exportación si no existe
    output_dir = 'excel_exports'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Generar nombre del archivo
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_dir, f'cuentas_bancarias_con_nombres_.xlsx')
    
    # Exportar a Excel
    print(f"Exportando a Excel: {output_file}")
    result_df.to_excel(output_file, index=False)
    print("Proceso completado exitosamente.")

if __name__ == '__main__':
    main()