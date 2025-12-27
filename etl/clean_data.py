import boto3
import pandas as pd
from io import StringIO

BUCKET_NAME = "datawarehouse-raw-sales"
RAW_FILE_KEY = "car_prices.csv"
CLEAN_FILE_KEY = "car_prices_clean.csv"


s3 = boto3.client("s3")

print("Descargando archivo desde S3...")
obj = s3.get_object(Bucket=BUCKET_NAME, Key=RAW_FILE_KEY)
df = pd.read_csv(obj["Body"])

print(f"Filas iniciales: {df.shape[0]}")


df = df.dropna(subset=["sellingprice", "saledate"])


df["sellingprice"] = pd.to_numeric(df["sellingprice"], errors="coerce")
df["odometer"] = pd.to_numeric(df["odometer"], errors="coerce")
df["year"] = pd.to_numeric(df["year"], errors="coerce")


df = df[df["sellingprice"] > 0]
df = df[df["odometer"] >= 0]
df = df[df["year"] >= 1900]


text_columns = ["make", "model", "trim", "body", "color", "interior", "state"]

for col in text_columns:
    df[col] = df[col].astype(str).str.strip().str.upper()


df["saledate"] = pd.to_datetime(df["saledate"], errors="coerce")
df = df.dropna(subset=["saledate"])

print(f"Filas finales: {df.shape[0]}")



csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

print("Subiendo archivo limpio a S3...")
s3.put_object(
    Bucket=BUCKET_NAME,
    Key=CLEAN_FILE_KEY,
    Body=csv_buffer.getvalue()
)

print("Dataset limpio cargado correctamente")
