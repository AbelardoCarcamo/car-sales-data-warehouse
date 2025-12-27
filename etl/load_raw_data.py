import boto3 #Se importa la librería boto3 para interactuar con AWS S3
import pandas as pd #Se importa la librería pandas para manipulación de datos
from io import StringIO #se importa StringIO para manejar cadenas como archivos


BUCKET_NAME = "datawarehouse-raw-sales"
FILE_KEY = "car_prices.csv" 

session = boto3.Session( 
    profile_name="default",
    region_name="us-east-1"
)

s3 = session.client("s3") 

print("Descargando archivo desde S3...") 

response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY) 

csv_content = response["Body"].read().decode("utf-8") 

df = pd.read_csv(StringIO(csv_content))

print("Archivo cargado correctamente")
print("Filas:", len(df))
print("Columnas:", len(df.columns))

print("\nColumnas disponibles:")
print(df.columns)


df = df.dropna(axis=1, how='all')
df = df.dropna(axis=0, how='all')

df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]


if 'price' in df.columns:
    df['price'] = df['price'].astype(float)


if 'price' in df.columns:
    df['price_usd'] = df['price'] / 500


csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)


s3.put_object(
    Bucket=BUCKET_NAME,
    Key=FILE_KEY,
    Body=csv_buffer.getvalue()
)

print(f"Archivo limpio subido a S3: {FILE_KEY}")
print(f"Columnas finales: {df.columns.tolist()}")
print(f"Primeras filas:\n{df.head()}")