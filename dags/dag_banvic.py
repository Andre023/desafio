from __future__ import annotations

import pendulum
import os
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine

# --- Funções da Lógica ETL ---

def extrair_dados_csv(**kwargs):
    """
    Extrai dados do arquivo CSV de transações e salva em uma pasta com a data da execução.
    """
    print("Iniciando extração de dados do CSV...")
    
    # O Airflow passa o contexto da execução para a função, incluindo a data.
    # 'ds' é a data de execução no formato YYYY-MM-DD.
    data_execucao = kwargs['ds']
    
    # Caminho onde o arquivo CSV original está (lembre-se que estamos dentro do contêiner do Airflow).
    # Vamos assumir que você montou uma pasta 'data' no seu projeto.
    caminho_origem = 'transacoes.csv'

    # Caminho de destino no nosso "Data Lake" local.
    # A estrutura de pastas é /ano-mes-dia/fonte/arquivo.csv
    caminho_destino_dir = f'/opt/airflow/data/output/{data_execucao}/csv'
    os.makedirs(caminho_destino_dir, exist_ok=True) # Garante que o diretório exista

    # Usando Pandas para ler o CSV (conceito dos vídeos).
    df = pd.read_csv(caminho_origem, sep=';')
    
    # Salvando o DataFrame como um novo arquivo CSV no destino.
    df.to_csv(f'{caminho_destino_dir}/transacoes.csv', index=False, sep=';')
    
    print(f"Arquivo transacoes.csv salvo em {caminho_destino_dir}")

def extrair_dados_sql(**kwargs):
    """
    Extrai todas as tabelas do banco de dados de origem (Postgres) e salva cada uma como um CSV.
    """
    print("Iniciando extração de dados do SQL...")
    data_execucao = kwargs['ds']
    
    # Usando o Hook do Airflow para pegar a conexão 'postgres_source' que criamos na UI.
    # Esta é a forma correta e segura de obter credenciais.
    hook = PostgresHook(postgres_conn_id="postgres_source")
    conn_url = hook.get_uri() # Pega a URL de conexão completa
    
    # Criando a 'engine' do SQLAlchemy para o Pandas se conectar ao banco.
    engine = create_engine(conn_url)
    
    # Lista de tabelas que vimos no arquivo banvic.sql.
    tabelas = ["agencias", "clientes", "colaboradores", "contas", "propostas_credito", "colaborador_agencia"]
    
    caminho_destino_dir = f'/opt/airflow/data/output/{data_execucao}/sql'
    os.makedirs(caminho_destino_dir, exist_ok=True)
    
    # Loop para extrair cada tabela (conceito dos vídeos).
    for tabela in tabelas:
        print(f"Extraindo tabela: {tabela}")
        # Usando pandas.read_sql para executar um 'SELECT *' na tabela.
        df = pd.read_sql(f'SELECT * FROM public."{tabela}"', engine)
        # Salvando a tabela como CSV.
        df.to_csv(f'{caminho_destino_dir}/{tabela}.csv', index=False, sep=';')
        print(f"Tabela {tabela} salva em {caminho_destino_dir}/{tabela}.csv")

    print("Extração de dados do SQL concluída.")

def carregar_dados_dw(**kwargs):
    """
    Lê os arquivos CSV do "Data Lake" local e os carrega no Data Warehouse.
    """
    print("Iniciando carregamento de dados para o Data Warehouse...")
    data_execucao = kwargs['ds']
    
    # Conectando ao nosso Data Warehouse usando a outra conexão.
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn_url = hook.get_uri()
    engine = create_engine(conn_url)
    
    # Diretório onde os arquivos extraídos estão.
    diretorio_base = f'/opt/airflow/data/output/{data_execucao}'
    
    # Itera sobre as fontes (csv, sql) e carrega os arquivos.
    for fonte in ['csv', 'sql']:
        caminho_completo_dir = os.path.join(diretorio_base, fonte)
        
        # Pega todos os arquivos .csv do diretório.
        arquivos_csv = [f for f in os.listdir(caminho_completo_dir) if f.endswith('.csv')]
        
        for arquivo in arquivos_csv:
            # O nome da tabela no DW será o nome do arquivo sem a extensão '.csv'.
            nome_tabela = arquivo.replace('.csv', '')
            print(f"Carregando arquivo {arquivo} para a tabela {nome_tabela}...")
            
            caminho_arquivo = os.path.join(caminho_completo_dir, arquivo)
            df = pd.read_csv(caminho_arquivo, sep=';')
            
            # Usando o método .to_sql do Pandas para carregar os dados.
            # if_exists='replace' significa que se a tabela já existir, ela será apagada e recriada.
            # Isso garante a idempotência exigida no desafio.
            df.to_sql(nome_tabela, engine, if_exists='replace', index=False, schema='public')
            print(f"Carga da tabela {nome_tabela} concluída.")

    print("Carregamento de dados para o Data Warehouse concluído.")


# --- Definição da DAG ---
with DAG(
    dag_id="pipeline_etl_banvic_final",
    start_date=pendulum.datetime(2025, 9, 7, tz="UTC"),
    schedule_interval="35 4 * * *",
    catchup=False,
    tags=["banvic", "etl"],
) as dag:
    
    # --- Definição das Tarefas ---
    tarefa_extracao_csv = PythonOperator(
        task_id="extrair_dados_csv",
        python_callable=extrair_dados_csv,
    )

    tarefa_extracao_sql = PythonOperator(
        task_id="extrair_dados_sql",
        python_callable=extrair_dados_sql,
    )

    tarefa_carregamento_dw = PythonOperator(
        task_id="carregar_dados_dw",
        python_callable=carregar_dados_dw,
    )

    # --- Ordem de Execução (Dependências) ---
    [tarefa_extracao_csv, tarefa_extracao_sql] >> tarefa_carregamento_dw