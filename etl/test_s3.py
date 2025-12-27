import boto3
from botocore.exceptions import ClientError

# Crear sesión usando el perfil por defecto y región explícita
session = boto3.Session(
    profile_name="default",
    region_name="us-east-1"
)

# Cliente S3
s3 = session.client("s3")

BUCKET_NAME = "datawarehouse-raw-sales"

try:
    # Listar objetos del bucket
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)

    if "Contents" not in response:
        print("El bucket está vacío o no tienes permiso para listar objetos.")
    else:
        print("Archivos encontrados en el bucket:")
        for obj in response["Contents"]:
            print(f"- {obj['Key']}")

except ClientError as e:
    print("Error al acceder a S3")
    print(e)
