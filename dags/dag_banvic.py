from __future__ import annotations

import pendulum
import os
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine

def extrair_csv(**kwargs):

    print("Extraindo dados do CSV")
    
    # 'ds' = formato YYYY-MM-DD.
    data_execucao = kwargs['ds']
    
    caminho_origem = '/opt/airflow/data/transacoes.csv'
    df = pd.read_csv(caminho_origem, sep=';')

    # Caminho no data lake local.
    caminho_destino = f'/opt/airflow/data/output/{data_execucao}/csv'
    os.makedirs(caminho_destino, exist_ok=True) # cria o diretório se não existir.
    # Salvando o DataFrame como um novo arquivo CSV no destino.
    df.to_csv(f'{caminho_destino}/transacoes.csv', index=False, sep=';')
    
    print(f"Arquivo salvo na pasta {caminho_destino}")

def extrair_sql(**kwargs):
    
    print("Extraindo dados do SQL")
    data_execucao = kwargs['ds']
    
    # Usando o Hook do Airflow para pegar a conexão 'postgres_source'.
    hook = PostgresHook(postgres_conn_id="postgres_source")
    conn_url = hook.get_uri()
    
    # Criando a 'engine' do SQLAlchemy para o Pandas se conectar ao banco.
    engine = create_engine(conn_url)
    
    # Lista de tabelas do arquivo banvic.sql.
    tabelas = ["agencias", "clientes", "colaboradores", "contas", "propostas_credito", "colaborador_agencia"]
    
    caminho_destino = f'/opt/airflow/data/output/{data_execucao}/sql'
    os.makedirs(caminho_destino, exist_ok=True)
    
    # Loop para extrair cada tabela.
    for tabela in tabelas:
        print(f"Extraindo tabela: {tabela}")
        df = pd.read_sql(f'SELECT * FROM public."{tabela}"', engine)
        df.to_csv(f'{caminho_destino}/{tabela}.csv', index=False, sep=';')
        print(f"Tabela {tabela} salva em {caminho_destino}/{tabela}.csv")

    print("Concluido.")

def carregar_dw(**kwargs):

    print("Carregando dados para o Data Warehouse")
    data_execucao = kwargs['ds']
    
    # Conectando ao nosso Data Warehouse usando a outra conexão.
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn_url = hook.get_uri()
    engine = create_engine(conn_url)
    
    # Diretório onde os arquivos extraídos estão.
    diretorio_base = f'/opt/airflow/data/output/{data_execucao}'
    
    # Itera sobre as fontes (csv, sql) e carrega os arquivos.
    for fonte in ['csv', 'sql']:
        caminho_completo = os.path.join(diretorio_base, fonte)
        
        # Pega todos os arquivos .csv do diretório.
        arquivos_csv = [f for f in os.listdir(caminho_completo) if f.endswith('.csv')]
        
        for arquivo in arquivos_csv:
            # O nome da tabela no DW será o nome do arquivo sem a extensão '.csv'.
            nome_tabela = arquivo.replace('.csv', '')
            
            caminho_arquivo = os.path.join(caminho_completo, arquivo)
            df = pd.read_csv(caminho_arquivo, sep=';')
            
            # Usando o método .to_sql do Pandas para carregar os dados.
            # if_exists='replace' significa que se a tabela já existir, ela será apagada e recriada.
            # Isso garante a idempotência exigida no desafio.
            df.to_sql(nome_tabela, engine, if_exists='replace', index=False, schema='public')

    print("Concluido.")

# --- Definição da DAG ---
with DAG(
    dag_id="pipeline_final",
    start_date=pendulum.datetime(2025, 9, 7, tz="UTC"),
    schedule="35 4 * * *",
    catchup=False,
    tags=["banvic", "pipeline_final"],
) as dag:
    
    # --- Definição das Tarefas ---
    tarefa_extracao_csv = PythonOperator(
        task_id="extrair_dados_csv",
        python_callable=extrair_csv,
    )

    tarefa_extracao_sql = PythonOperator(
        task_id="extrair_dados_sql",
        python_callable=extrair_sql,
    )

    tarefa_carregamento_dw = PythonOperator(
        task_id="carregar_dados_dw",
        python_callable=carregar_dw,
    )

    # Executando em paralelo
    [tarefa_extracao_csv, tarefa_extracao_sql] >> tarefa_carregamento_dw