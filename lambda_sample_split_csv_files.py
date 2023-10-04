import os
import csv
import boto3
import io

# Cria um cliente S3
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Obtém informações sobre o arquivo de origem a partir do evento
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    # Define o nome do bucket de destino (altere para o nome desejado)
    dest_bucket = 'bucket-destino-blogpost-cpe'

    # Lê o conteúdo do arquivo CSV do bucket de origem
    csv_data = s3_client.get_object(Bucket=source_bucket, Key=object_key)['Body'].read().decode('utf-8')

    # Divide o arquivo CSV em partes de até 50MB
    chunk_size = 50 * 1024 * 1024
    csv_reader = csv.reader(io.StringIO(csv_data))

    part_number = 1
    current_chunk = []  # Lista para armazenar as linhas da parte atual
    current_chunk_size = 0

    for row in csv_reader:
        row_str = ','.join(row) + '\n'  # Converte a linha do CSV em uma string
        row_size = len(row_str.encode('utf-8'))  # Calcula o tamanho da linha em bytes

        # Se a próxima linha não couber na parte atual, inicia uma nova parte
        if current_chunk_size + row_size > chunk_size:
            part_name = f"{object_key}.Part{part_number}.csv"  # Nome da parte atual

            # Upload da parte atual para o bucket de destino
            s3_client.put_object(Bucket=dest_bucket, Key=part_name, Body=''.join(current_chunk))

            # Prepara a próxima parte
            part_number += 1
            current_chunk = []
            current_chunk_size = 0

        current_chunk.append(row_str)
        current_chunk_size += row_size

    # Se houver alguma parte restante, faça o upload dela
    if current_chunk:
        part_name = f"{object_key}.Part{part_number}.csv"
        s3_client.put_object(Bucket=dest_bucket, Key=part_name, Body=''.join(current_chunk))

    # Retorna uma resposta de sucesso
    return {
        'statusCode': 200,
        'body': 'Arquivo dividido com sucesso!'
    }
