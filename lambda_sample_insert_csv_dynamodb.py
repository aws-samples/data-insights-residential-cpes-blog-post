import boto3
import csv
from datetime import datetime

def lambda_handler(event, context):
    
    try:
        s3_client = boto3.client('s3')  # Cria um cliente para interagir com o Amazon S3
        
        # Itera sobre os registros do evento (geralmente desencadeado por eventos S3)
        for s3_record in event['Records']:
            bucket_name = s3_record['s3']['bucket']['name']  # Obtém o nome do bucket S3
            object_name = s3_record['s3']['object']['key']   # Obtém o nome do objeto (arquivo) no S3

            filepath = '/tmp/' + object_name  # Caminho temporário para baixar o arquivo
            s3_client.download_file(bucket_name, object_name, filepath)  # Baixa o arquivo do S3
            _bulk_write_records(filepath)  # Chama a função para processar o arquivo
        
        # Retorna uma resposta de sucesso
        return {
            'statusCode': 200,
            'body': event
        }
    except Exception as err:
        print(err)  # Tratamento de exceção básico

# Função para processar registros do arquivo CSV
def _bulk_write_records(filepath):
    with open(filepath, 'r') as csv_file:  # Abre o arquivo CSV para leitura
        csv_reader = csv.reader(csv_file, delimiter=';')  # Lê o arquivo CSV usando ';' como delimitador

        records = []  # Lista para armazenar os registros processados

        for row in csv_reader:
            
            # Verifica se a linha tem pelo menos 3 campos
            if len(row) < 3:
                # Pula as linhas com número insuficiente de campos
                continue

            record_time = datetime.fromtimestamp(int(row[1]))  # Converte um timestamp em formato legível
            time_format = record_time.strftime('%m/%d/%Y %H:%M:%S')  # Formata a data/hora

            # Cria um registro com os dados processados
            record = {
                'modem_ip': row[0],
                'time': time_format,
                'cmts': row[2],
            }

            records.append(record)  # Adiciona o registro à lista

            if len(records) == 100:
                _submit_batch(records)  # Envia registros em lote para o DynamoDB
                records = []  # Limpa a lista após o envio

        if len(records) != 0:
            _submit_batch(records)  # Envia registros finais para o DynamoDB
        
        print(len(records))  # Imprime o número de registros processados

# Função para enviar registros em lote para o DynamoDB
def _submit_batch(records):
    try:
        dyndb = boto3.resource('dynamodb', region_name='us-west-2')  # Conexão com o DynamoDB

        table_name = 'csv-to-dynamoDB-Blogpost-1'  # Nome da tabela
        table = dyndb.Table(table_name)  # Obtenção da tabela

        # Verifica se a tabela existe, caso contrário, cria-a
        existing_tables = dyndb.tables.all()
        table_exists = any(table.name == table_name for table in existing_tables)

        if not table_exists:
            # Cria a tabela com a configuração especificada
            table = dyndb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'modem_ip',
                        'KeyType': 'HASH'  # Chave de partição
                    },
                    {
                        'AttributeName': 'time',
                        'KeyType': 'RANGE'  # Chave de classificação
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'modem_ip',
                        'AttributeType': 'S'  # String
                    },
                    {
                        'AttributeName': 'time',
                        'AttributeType': 'S'  # String
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            # Aguarda até que a tabela esteja pronta
            table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

        # Escreve os registros na tabela
        with table.batch_writer() as batch:
            for record in records:
                batch.put_item(
                    Item=record
                )
        print("Processed [%d] records." % (len(records)))  # Imprime o número de registros processados
    except Exception as err:
        print(err)  # Tratamento de exceção básico
