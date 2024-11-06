from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import os

def obter_conexao_db():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    return hook.get_conn()

def carregar_stage(chunk_dados):
    conn = obter_conexao_db()
    cursor = conn.cursor()

    consulta_insercao = """
    INSERT INTO stage_cotahist (
        tipo_registro,
        data_pregao,
        cod_bdi,
        cod_negociacao,
        tipo_mercado,
        nome_empresa,
        moeda,
        preco_abertura,
        preco_maximo,
        preco_minimo,
        preco_medio,
        preco_ultimo_negocio,
        preco_melhor_oferta_compra,
        preco_melhor_oferta_venda,
        numero_negocios,
        quantidade_papeis_negociados,
        volume_total_negociado,
        codigo_isin,
        num_distribuicao_papel
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    for registro in chunk_dados:
        valores = (
            str(registro['tipo_registro']),
            str(registro['data_pregao']),
            str(registro['cod_bdi']),
            str(registro['cod_negociacao']),
            str(registro['tipo_mercado']),
            str(registro['nome_empresa']),
            str(registro['moeda']),
            float(registro['preco_abertura']),
            float(registro['preco_maximo']),
            float(registro['preco_minimo']),
            float(registro['preco_medio']),
            float(registro['preco_ultimo_negocio']),
            float(registro['preco_melhor_oferta_compra']),
            float(registro['preco_melhor_oferta_venda']),
            str(registro['numero_negocios']),
            str(registro['quantidade_papeis_negociados']),
            str(registro['volume_total_negociado']),
            str(registro.get('codigo_isin', '')),
            str(registro.get('num_distribuicao_papel', ''))
        )
        cursor.execute(consulta_insercao, valores)
    
    conn.commit()
    cursor.close()
    conn.close()

def extrair():
    anos = ['2022', '2023', '2024']
    arquivos = [f'/opt/airflow/files/COTAHIST_A{ano}.txt' for ano in anos]
    
    separar_campos = [2, 8, 2, 12, 3, 12, 10, 3, 4, 13, 13, 13, 13, 13, 13, 13, 5, 18, 18, 13, 1, 8, 7, 13, 12, 3]
    colunas = [
        "tipo_registro", "data_pregao", "cod_bdi", "cod_negociacao", "tipo_mercado", 
        "nome_empresa", "especificacao_papel", "prazo_dias_merc_termo", "moeda", 
        "preco_abertura", "preco_maximo", "preco_minimo", "preco_medio", 
        "preco_ultimo_negocio", "preco_melhor_oferta_compra", "preco_melhor_oferta_venda", 
        "numero_negocios", "quantidade_papeis_negociados", "volume_total_negociado", 
        "preco_exercicio", "indicador_correcao_precos", "data_vencimento", 
        "fator_cotacao", "preco_exercicio_pontos", "codigo_isin", "num_distribuicao_papel"
    ]

    tamanho_chunk = 10000

    for arquivo in arquivos:
        if not os.path.isfile(arquivo):
            continue

        for chunk in pd.read_fwf(arquivo, widths=separar_campos, header=0, chunksize=tamanho_chunk):
            chunk.columns = colunas
            colunas_dinheiro = [
                "preco_abertura", "preco_maximo", "preco_minimo", 
                "preco_medio", "preco_ultimo_negocio", 
                "preco_melhor_oferta_compra", "preco_melhor_oferta_venda"
            ]

            for coluna in colunas_dinheiro:
                if coluna in chunk.columns:
                    chunk[coluna] = chunk[coluna].astype(float) / 100

            chunk['data_pregao'] = pd.to_datetime(chunk['data_pregao'], format='%Y%m%d', errors='coerce')
            chunk['data_pregao'] = chunk['data_pregao'].dt.strftime('%d/%m/%Y')
            
            carregar_stage(chunk.to_dict(orient='records'))

def inserir_dados_calendario(conn):
    with conn.cursor() as cursor:
        cursor.execute(""" 
        INSERT INTO dim_calendario (data, dia, mes, ano, trimestre, dia_da_semana)
        SELECT DISTINCT 
            TO_DATE(data_pregao, 'DD/MM/YYYY') AS data, 
            EXTRACT(DAY FROM TO_DATE(data_pregao, 'DD/MM/YYYY')),
            EXTRACT(MONTH FROM TO_DATE(data_pregao, 'DD/MM/YYYY')),
            EXTRACT(YEAR FROM TO_DATE(data_pregao, 'DD/MM/YYYY')),
            EXTRACT(QUARTER FROM TO_DATE(data_pregao, 'DD/MM/YYYY')),
            TO_CHAR(TO_DATE(data_pregao, 'DD/MM/YYYY'), 'Day')
        FROM stage_cotahist
        WHERE data_pregao IS NOT NULL AND data_pregao <> ''
        ON CONFLICT (data) DO NOTHING;
        """)
        conn.commit()

def transformar_e_enriquecer():
    conn = obter_conexao_db()
    
    try:
        inserir_dados_calendario(conn)

        with conn.cursor() as cursor:
            cursor.execute(""" 
            INSERT INTO dim_ativos (ativo) 
            SELECT DISTINCT cod_negociacao FROM stage_cotahist 
            ON CONFLICT (ativo) DO NOTHING;
            """)

            cursor.execute(""" 
            INSERT INTO fato_cotahist (ativo_id, data_id, preco_abertura, preco_fechamento, volume) 
            SELECT da.id, dc.id, sc.preco_abertura, sc.preco_ultimo_negocio, sc.volume_total_negociado 
            FROM stage_cotahist sc 
            JOIN dim_ativos da ON sc.cod_negociacao = da.ativo
            JOIN dim_calendario dc ON TO_DATE(sc.data_pregao, 'DD/MM/YYYY') = dc.data;
            """)
        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

with DAG('cotahist_etl', schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False) as dag:
    tarefa_extrair = PythonOperator(task_id='extrair', python_callable=extrair)
    tarefa_transformar_enriquecer = PythonOperator(task_id='transformar_e_enriquecer', python_callable=transformar_e_enriquecer)

    tarefa_extrair >> tarefa_transformar_enriquecer
