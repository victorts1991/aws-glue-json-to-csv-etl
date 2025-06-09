import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.session import SparkSession
from awsglue.job import Job
from pyspark.sql.functions import explode, col, lit, to_timestamp, date_format # Importar funcoes de data
import traceback

# Inicializa o GlueContext e SparkContext
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Configurações de Caminho ---
MAIN_BUCKET = "etl-json-project-raw-json"
INPUT_JSON_DIR_PATH = f"s3://{MAIN_BUCKET}/" 
OUTPUT_PROCESSED_CSV_DIR_PATH = f"s3://{MAIN_BUCKET}/processed_csv/"

# Lista dos Nomes dos arquivos JSON que SERÃO PROCESSADOS POR ESTE SCRIPT
json_file_names = [
    "resultado2022.json",
    "resultado2023.json",
    "resultado2024.json",
    "resultado2025.json",
    "resultado2025V2.json"
]

# Itera sobre cada arquivo JSON para processá-lo individualmente
for json_file_name in json_file_names:
    full_input_path = f"{INPUT_JSON_DIR_PATH}{json_file_name}"
    
    output_csv_file_name = json_file_name.rsplit('.json', 1)[0] + ".csv"
    full_output_path = f"{OUTPUT_PROCESSED_CSV_DIR_PATH}{output_csv_file_name}"

    print(f"--- Processando arquivo: {full_input_path} ---")

    try:
        df_raw = spark.read.option("multiline", "true").json(full_input_path)

        print(f"Tentando contar linhas do arquivo {json_file_name} após leitura...")
        num_rows = df_raw.count()
        print(f"Número de linhas lidas para {json_file_name}: {num_rows}")

        if num_rows == 0:
            print(f"AVISO: O arquivo {json_file_name} foi lido, mas está vazio ou malformado. Nenhuma linha para processar.")
            continue

        print(f"Schema do JSON bruto para {json_file_name}:")
        df_raw.printSchema()
        df_raw.show(5, truncate=False)

        # Colunas de nível superior relevantes para o explode do history
        base_columns_to_select = []
        select_expr_list = [
            col(c) if c in df_raw.columns else lit(None).alias(c)
            for c in base_columns_to_select
        ]
        
        if "history" in df_raw.columns:
            select_expr_list.append(col("history"))
            df_base_and_history = df_raw.select(*select_expr_list)

            df_history_exploded = df_base_and_history.select(explode(col("history")).alias("history_item"))

            # Agora, selecione APENAS os campos desejados e formate as datas
            processed_df = df_history_exploded.select(
                col("history_item.departmentName").alias("department_name"),
                # Converte string para timestamp e depois formata para DD/MM/YYYY HH:MM:SS
                to_timestamp(col("history_item.createdOn"), "yyyy-MM-dd HH:mm:ss").alias("created_on_ts"),
                date_format(col("created_on_ts"), "dd/MM/yyyy HH:mm:ss").alias("created_on"),

                to_timestamp(col("history_item.endDate"), "yyyy-MM-dd HH:mm:ss").alias("end_date_history_item_ts"),
                date_format(col("end_date_history_item_ts"), "dd/MM/yyyy HH:mm:ss").alias("end_date_history_item"),

                col("history_item.milestoneName").alias("milestone_name"),
                col("history_item.unitLocal").alias("unit_local"),
                col("history_item.credential").alias("credential"),
                col("history_item.userName").alias("user_name"),
                col("history_item.location").alias("location")
            ).drop("created_on_ts", "end_date_history_item_ts") # Remove as colunas temporárias de timestamp

            print(f"Schema do DataFrame processado para {json_file_name}:")
            processed_df.printSchema()
            processed_df.show(5, truncate=False)

            print(f"Escrevendo CSV para: {full_output_path}")
            processed_df.repartition(1).write.mode("overwrite").option("header", "true").csv(full_output_path)
            print(f"Processamento de {json_file_name} concluído com sucesso!")
        else:
            print(f"AVISO: O arquivo {json_file_name} não contém a coluna 'history'. Este script foca em dados com histórico.")
            # Se um arquivo não tem 'history', ele será ignorado neste caso, pois o foco é o histórico.
            continue # Pula para o próximo arquivo se não houver 'history' para processar

    except Exception as e:
        print(f"ERRO CRÍTICO ao processar {json_file_name}: {e}")
        print(f"Detalhes do erro para {json_file_name}:\n{traceback.format_exc()}")

job.commit()