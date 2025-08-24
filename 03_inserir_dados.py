# -*- coding: utf-8 -*-
"""
Script 3: Inserção de Dados CNPJ
=================================

Este script é responsável por inserir todos os dados da base CNPJ nas tabelas criadas.
Ele inclui carregamento de tabelas de códigos, dados principais e verificação de integridade.

@author: rictom
https://github.com/rictom/cnpj-mysql
"""

import os
import sys
import time
import glob
import logging
import pandas as pd
import dask.dataframe as dd
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

# Configurar logging detalhado
def configurar_logging():
    """Configura o sistema de logging com arquivo e console"""
    # Criar pasta logs se não existir
    logs_dir = 'logs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Versão anterior: log_filename = f'cnpj_inserir_dados_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_filename = os.path.join(logs_dir, f'cnpj_inserir_dados_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def obter_configuracao_banco():
    """Obtém a configuração do banco de dados PostgreSQL com valores padrão"""
    print("\n" + "="*60)
    print("CONFIGURAÇÃO DO BANCO DE DADOS POSTGRESQL")
    print("="*60)
    
    # Verificar se existe arquivo de configuração do script anterior
    config_file = "cnpj_config.json"
    if os.path.exists(config_file):
        try:
            import json
            with open(config_file, 'r', encoding='utf-8') as f:
                config_anterior = json.load(f)
            
            print(f"Arquivo de configuração encontrado do script anterior:")
            print(f"  Tipo de banco: {config_anterior.get('tipo_banco', 'N/A')}")
            print(f"  Banco de dados: {config_anterior.get('dbname', 'N/A')}")
            print(f"  Usuário: {config_anterior.get('username', 'N/A')}")
            print(f"  Host: {config_anterior.get('host', 'N/A')}")
            
            # Usar automaticamente a configuração encontrada
            print("\n✅ Usando configuração do arquivo automaticamente")
            return config_anterior
            
        except Exception as e:
            logger.warning(f"Erro ao ler arquivo de configuração: {str(e)}")
            print("Arquivo de configuração corrompido, será solicitada nova configuração.")
    
    # Se não houver arquivo, usar valores padrão
    print("\nUsando configurações padrão PostgreSQL:")
    
    # Configurações padrão
    tipo_banco = 'postgres'
    dbname_padrao = 'cnpjbr'
    username_padrao = 'postgres'
    password_padrao = 'postgres123'
    host_padrao = '127.0.0.1'
    
    print(f"  Tipo de banco: {tipo_banco}")
    print(f"  Banco de dados: {dbname_padrao}")
    print(f"  Usuário: {username_padrao}")
    print(f"  Host: {host_padrao}")
    
    # Usar valores padrão automaticamente
    return {
        'tipo_banco': tipo_banco,
        'dbname': dbname_padrao,
        'username': username_padrao,
        'password': password_padrao,
        'host': host_padrao
    }

def verificar_pasta_dados():
    """Verifica se a pasta de dados descompactados existe"""
    pasta_saida = r"dados-publicos"
    
    if not os.path.exists(pasta_saida):
        raise FileNotFoundError(f"Pasta de dados não encontrada: {pasta_saida}")
    
    # Verificar se há arquivos CSV
    arquivos_csv = []
    extensoes = ['.EMPRECSV', '.ESTABELE', '.SOCIOCSV', '.CNAECSV', '.MOTICSV', '.MUNICCSV', '.NATJUCSV', '.PAISCSV', '.QUALSCSV', '.SIMPLES.CSV']
    
    for extensao in extensoes:
        arquivos = glob.glob(os.path.join(pasta_saida, f'*{extensao}'))
        arquivos_csv.extend(arquivos)
    
    if not arquivos_csv:
        raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em: {pasta_saida}")
    
    logger.info(f"Encontrados {len(arquivos_csv)} arquivos CSV para processar:")
    for arquivo in arquivos_csv:
        tamanho = os.path.getsize(arquivo) / (1024 * 1024)  # MB
        logger.info(f"  - {os.path.basename(arquivo)} ({tamanho:.2f} MB)")
    
    return pasta_saida, arquivos_csv

def conectar_banco(config):
    """Conecta ao banco de dados PostgreSQL"""
    try:
        logger.info("Conectando ao banco de dados PostgreSQL...")
        
        port = config.get('port', 5432)
        engine_url = f"postgresql://{config['username']}:{config['password']}@{config['host']}:{port}/{config['dbname']}"
        logger.info("Criando engine PostgreSQL...")
        
        engine_ = sqlalchemy.create_engine(engine_url)
        logger.info("Engine criado com sucesso")
        
        # Testar conexão
        with engine_.connect() as conn:
            conn.execute(text("SELECT 1 as test"))
            logger.info("✓ Conexão estabelecida com sucesso!")
        
        return engine_, engine_url
        
    except Exception as e:
        logger.error(f"✗ Erro ao conectar com o banco: {str(e)}")
        raise

def carregar_tabela_codigo(engine, pasta_saida, extensao_arquivo, nome_tabela):
    """Carrega uma tabela de códigos (CNAE, motivo, município, etc.)"""
    try:
        arquivo = list(glob.glob(os.path.join(pasta_saida, f'*{extensao_arquivo}')))[0]
        logger.info(f"Carregando tabela {nome_tabela} do arquivo: {os.path.basename(arquivo)}")
        
        # Verificar tamanho do arquivo
        tamanho_arquivo = os.path.getsize(arquivo) / (1024 * 1024)  # MB
        logger.info(f"Tamanho do arquivo: {tamanho_arquivo:.2f} MB")
        
        # Ler CSV
        logger.info("Lendo arquivo CSV...")
        start_time = time.time()
        dtab = pd.read_csv(arquivo, dtype=str, sep=';', encoding='latin1', header=None, names=['codigo', 'descricao'])
        end_time = time.time()
        
        logger.info(f"CSV lido com sucesso em {end_time - start_time:.2f}s. Linhas: {len(dtab)}")
        
        # Verificar dados
        logger.info(f"Primeiras linhas da tabela {nome_tabela}:")
        logger.info(dtab.head().to_string())
        
        # Verificar valores únicos
        codigos_unicos = dtab['codigo'].nunique()
        logger.info(f"Códigos únicos encontrados: {codigos_unicos}")
        
        # Verificar se já existem dados e se são os mesmos
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) as total FROM {nome_tabela}'))
            total_existente = result.fetchone()[0]
            
            if total_existente > 0:
                logger.info(f"Tabela {nome_tabela} já possui {total_existente} registros, verificando se são os mesmos...")
                
                # Verificar se os dados são idênticos (primeiro e último registro)
                primeiro_codigo = dtab.iloc[0]['codigo']
                ultimo_codigo = dtab.iloc[-1]['codigo']
                
                result = conn.execute(text(f"SELECT codigo FROM {nome_tabela} ORDER BY codigo LIMIT 1"))
                primeiro_existente = result.fetchone()
                primeiro_existente = primeiro_existente[0] if primeiro_existente else None
                
                result = conn.execute(text(f"SELECT codigo FROM {nome_tabela} ORDER BY codigo DESC LIMIT 1"))
                ultimo_existente = result.fetchone()
                ultimo_existente = ultimo_existente[0] if ultimo_existente else None
                
                # Verificar se o número de registros é o mesmo
                if total_existente == len(dtab):
                    if primeiro_existente == primeiro_codigo and ultimo_existente == ultimo_codigo:
                        logger.info(f"✓ Tabela {nome_tabela} já contém exatamente os mesmos dados ({total_existente} registros), pulando inserção...")
                        return True
                    else:
                        logger.warning(f"⚠ Número de registros igual mas dados diferentes detectados, inserindo novos registros...")
                else:
                    logger.info(f"⚠ Número de registros diferente (existente: {total_existente}, esperado: {len(dtab)}), inserindo novos registros...")
        
        # Inserir dados
        logger.info(f"Inserindo dados na tabela {nome_tabela}...")
        start_time = time.time()
        dtab.to_sql(nome_tabela, engine, if_exists='append', index=None)
        end_time = time.time()
        
        logger.info(f"Dados inseridos com sucesso em {end_time - start_time:.2f}s")
        
        # Criar índice (verificar se já existe)
        logger.info(f"Verificando/criando índice na tabela {nome_tabela}...")
        start_time = time.time()
        with engine.connect() as conn:
            try:
                # Verificar se o índice já existe
                result = conn.execute(text(f"SELECT indexname FROM pg_indexes WHERE tablename = '{nome_tabela}' AND indexname = 'idx_{nome_tabela}'"))
                
                if result.fetchone():
                    logger.info(f"Índice idx_{nome_tabela} já existe, pulando...")
                else:
                    # Criar índice se não existir
                    conn.execute(text(f'CREATE INDEX idx_{nome_tabela} ON {nome_tabela}(codigo);'))
                    conn.commit()
                    logger.info(f"Índice idx_{nome_tabela} criado com sucesso")
                    
            except Exception as e:
                logger.warning(f"Erro ao verificar/criar índice: {str(e)}")
                logger.info("Continuando sem criar índice...")
                
        end_time = time.time()
        logger.info(f"Processamento de índice concluído em {end_time - start_time:.2f}s")
        
        # Verificar dados inseridos
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) as total FROM {nome_tabela}'))
            total_inserido = result.fetchone()[0]
        
        logger.info(f"Total de registros na tabela {nome_tabela}: {total_inserido}")
        logger.info(f"Registros esperados do arquivo: {len(dtab)}")
        
        if total_inserido != len(dtab):
            logger.warning(f"⚠ Diferença no número de registros: esperado {len(dtab)}, inserido {total_inserido}")
        else:
            logger.info(f"✓ Contagem de registros confere: {total_inserido}/{len(dtab)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao carregar tabela {nome_tabela}: {str(e)}")
        raise

def carregar_tabela_principal(engine, engine_url, pasta_saida, nome_tabela, extensao, colunas):
    """Carrega uma tabela principal usando Dask para melhor performance"""
    try:
        arquivos = list(glob.glob(os.path.join(pasta_saida, f'*{extensao}')))
        logger.info(f"Encontrados {len(arquivos)} arquivos para tabela {nome_tabela}")
        
        total_registros = 0
        
        for i, arquivo in enumerate(arquivos, 1):
            logger.info(f"Processando arquivo {i}/{len(arquivos)}: {os.path.basename(arquivo)}")
            
            # Verificar tamanho do arquivo
            tamanho_arquivo = os.path.getsize(arquivo) / (1024 * 1024)  # MB
            logger.info(f"Tamanho do arquivo: {tamanho_arquivo:.2f} MB")
            
            # Ler CSV com Dask
            logger.info("Lendo CSV com Dask...")
            start_time = time.time()
            ddf = dd.read_csv(
                arquivo, 
                sep=';', 
                header=None, 
                names=colunas, 
                encoding='latin1', 
                dtype=str, 
                na_filter=None
            )
            end_time = time.time()
            
            logger.info(f"CSV lido com Dask em {end_time - start_time:.2f}s")
            
            # Verificar estrutura
            logger.info(f"Estrutura do DataFrame: {ddf.shape}")
            logger.info(f"Colunas: {list(ddf.columns)}")
            
            # Verificar se já existem dados na tabela e se estão completos
            with engine.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) as total FROM {nome_tabela}'))
                total_existente = result.fetchone()[0]
                
                if total_existente > 0:
                    # VERIFICAR SE A TABELA ESTÁ COMPLETA
                    logger.info(f"Verificando se tabela {nome_tabela} está completa...")
                    
                    # Contar registros esperados do arquivo CSV
                    registros_esperados = ddf.shape[0].compute()
                    logger.info(f"Registros esperados do arquivo: {registros_esperados}")
                    logger.info(f"Registros existentes no banco: {total_existente}")
                    
                    if total_existente >= registros_esperados:
                        logger.info(f"✓ Tabela {nome_tabela} já está completa ({total_existente}/{registros_esperados}), pulando inserção...")
                        continue
                    else:
                        logger.info(f"⚠ Tabela {nome_tabela} incompleta ({total_existente}/{registros_esperados}), inserindo dados faltantes...")
                        # Continuar para inserir apenas o que falta
        
        # Inserir dados
        logger.info("Inserindo dados no banco...")
        start_time = time.time()
        ddf.to_sql(
            nome_tabela, 
            engine_url, 
            index=None, 
            if_exists='append',
            dtype=sqlalchemy.sql.sqltypes.String
        )
        end_time = time.time()
        
        logger.info(f"Dados inseridos em {end_time - start_time:.2f}s")
        
        # Verificar registros inseridos e comparar com esperado
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) as total FROM {nome_tabela}'))
            total_atual = result.fetchone()[0]
        
        registros_arquivo = total_atual - total_registros
        total_registros = total_atual
        
        # Contar registros esperados do arquivo para comparação
        registros_esperados = ddf.shape[0].compute()
        
        logger.info(f"Registros inseridos deste arquivo: {registros_arquivo}")
        logger.info(f"Total acumulado na tabela {nome_tabela}: {total_registros}")
        logger.info(f"Registros esperados do arquivo: {registros_esperados}")
        
        # Verificar se há diferença (similar às tabelas de códigos)
        if total_registros != registros_esperados:
            logger.warning(f"⚠ Diferença no número de registros: esperado {registros_esperados}, inserido {total_registros}")
        else:
            logger.info(f"✓ Contagem de registros confere: {total_registros}/{registros_esperados}")
        
        logger.info(f"✓ Tabela {nome_tabela} carregada com sucesso. Total: {total_registros} registros")
        return total_registros
        
    except Exception as e:
        logger.error(f"Erro ao carregar tabela {nome_tabela}: {str(e)}")
        raise

def executar_sqls_finais(engine):
    """Executa os SQLs finais para otimização e criação de índices"""
    logger.info("Executando SQLs finais de otimização...")
    
    sqls = '''
    ALTER TABLE empresas ADD COLUMN capital_social DECIMAL(18,2);
    UPDATE empresas SET capital_social = CAST(REPLACE(capital_social_str,',', '.') AS DECIMAL(18,2));
    ALTER TABLE empresas DROP COLUMN capital_social_str;
    
    ALTER TABLE estabelecimento ADD COLUMN cnpj VARCHAR(14);
    UPDATE estabelecimento SET cnpj = CONCAT(cnpj_basico, cnpj_ordem, cnpj_dv);
    
    CREATE INDEX idx_estabelecimento_cnpj ON estabelecimento (cnpj);
    CREATE INDEX idx_empresas_cnpj_basico ON empresas (cnpj_basico);
    CREATE INDEX idx_empresas_razao_social ON empresas (razao_social);
    CREATE INDEX idx_estabelecimento_cnpj_basico ON estabelecimento (cnpj_basico);
    
    CREATE INDEX idx_socios_original_cnpj_basico ON socios_original(cnpj_basico);
    
    DROP TABLE IF EXISTS socios;
    CREATE TABLE socios AS 
    SELECT te.cnpj as cnpj, ts.*
    FROM socios_original ts
    LEFT JOIN estabelecimento te ON te.cnpj_basico = ts.cnpj_basico
    WHERE te.matriz_filial='1';
    
    DROP TABLE IF EXISTS socios_original;
    
    CREATE INDEX idx_socios_cnpj ON socios(cnpj);
    CREATE INDEX idx_socios_cnpj_basico ON socios(cnpj_basico);
    CREATE INDEX idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio);
    CREATE INDEX idx_socios_nome_socio ON socios(nome_socio);
    
    CREATE INDEX idx_simples_cnpj_basico ON simples(cnpj_basico);
    
    DROP TABLE IF EXISTS _referencia;
    CREATE TABLE _referencia (
        referencia VARCHAR(100),
        valor VARCHAR(100)
    );
    '''
    
    # Executar comandos SQL
    comandos = [cmd.strip() for cmd in sqls.split(';') if cmd.strip()]
    logger.info(f"Total de comandos SQL finais: {len(comandos)}")
    
    comandos_executados = 0
    comandos_com_erro = 0
    
    for i, comando in enumerate(comandos, 1):
        try:
            logger.info(f"Executando comando final {i}/{len(comandos)}")
            logger.debug(f"SQL: {comando[:100]}...")
            
            start_time = time.time()
            with engine.connect() as conn:
                conn.execute(text(comando))
                conn.commit()
            end_time = time.time()
            
            duration = end_time - start_time
            logger.info(f"✓ Comando {i} executado com sucesso em {duration:.2f}s")
            comandos_executados += 1
            
        except Exception as e:
            logger.error(f"✗ Erro no comando {i}: {str(e)}")
            logger.error(f"SQL problemático: {comando}")
            comandos_com_erro += 1
            
            # Perguntar se deve continuar
            resp = input(f"Erro no comando {i}. Deseja continuar? (S/N): ")
            if resp.upper() != 'S':
                logger.error("Execução interrompida pelo usuário")
                break
    
    logger.info(f"SQLs finais: {comandos_executados} executados, {comandos_com_erro} com erro")
    return comandos_executados, comandos_com_erro

def inserir_dados_referencia(engine, pasta_saida):
    """Insere dados de referência na tabela _referencia"""
    try:
        logger.info("Inserindo dados de referência...")
        
        # Determinar data de referência
        arquivos_empresas = glob.glob(os.path.join(pasta_saida, '*.EMPRECSV'))
        if arquivos_empresas:
            nome_arquivo = os.path.basename(arquivos_empresas[0])
            data_referencia = nome_arquivo.split('.')[2]  # formato DAMMDD
            
            if len(data_referencia) == len('D30610') and data_referencia.startswith('D'):
                data_referencia = data_referencia[4:6] + '/' + data_referencia[2:4] + '/202' + data_referencia[1]
            else:
                data_referencia = 'dd/mm/2025'
        else:
            data_referencia = 'dd/mm/2025'
        
        logger.info(f"Data de referência: {data_referencia}")
        
        # Contar registros
        with engine.connect() as conn:
            qtde_cnpjs = conn.execute(text('SELECT COUNT(*) as contagem FROM estabelecimento;')).fetchone()[0]
            logger.info(f"Quantidade de CNPJs encontrados: {qtde_cnpjs}")
            
            # Inserir dados de referência
            conn.execute(text(f"INSERT INTO _referencia (referencia, valor) VALUES ('CNPJ', '{data_referencia}')"))
            conn.execute(text(f"INSERT INTO _referencia (referencia, valor) VALUES ('cnpj_qtde', '{qtde_cnpjs}')"))
            conn.commit()
        
        logger.info("✓ Dados de referência inseridos com sucesso")
        return data_referencia, qtde_cnpjs
        
    except Exception as e:
        logger.error(f"Erro ao inserir dados de referência: {str(e)}")
        raise

def verificar_integridade_dados(engine):
    """Verifica a integridade dos dados inseridos"""
    logger.info("Verificando integridade dos dados...")
    
    verificacoes = []
    
    try:
        with engine.connect() as conn:
            # Contar registros em cada tabela
            tabelas = ['empresas', 'estabelecimento', 'socios', 'simples', 'cnae', 'motivo', 'municipio', 'natureza_juridica', 'pais', 'qualificacao_socio']
            
            for tabela in tabelas:
                try:
                    result = conn.execute(text(f'SELECT COUNT(*) as total FROM {tabela}'))
                    total = result.fetchone()[0]
                    verificacoes.append((tabela, total, True))
                    logger.info(f"✓ Tabela {tabela}: {total} registros")
                except Exception as e:
                    verificacoes.append((tabela, 0, False))
                    logger.warning(f"⚠ Tabela {tabela}: erro ao contar - {str(e)}")
            
            # Verificar relacionamentos
            logger.info("Verificando relacionamentos...")
            
            # Empresas vs Estabelecimento
            result = conn.execute(text('''
                SELECT COUNT(*) as total 
                FROM empresas e 
                INNER JOIN estabelecimento est ON e.cnpj_basico = est.cnpj_basico
            '''))
            relacionamento_emp_est = result.fetchone()[0]
            logger.info(f"Relacionamento empresas-estabelecimento: {relacionamento_emp_est} registros")
            
            # Estabelecimento vs Socios
            result = conn.execute(text('''
                SELECT COUNT(*) as total 
                FROM estabelecimento e 
                INNER JOIN socios s ON e.cnpj = s.cnpj
            '''))
            relacionamento_est_soc = result.fetchone()[0]
            logger.info(f"Relacionamento estabelecimento-socios: {relacionamento_est_soc} registros")
            
    except Exception as e:
        logger.error(f"Erro ao verificar integridade: {str(e)}")
    
    return verificacoes

def main():
    """Função principal do script"""
    global logger
    
    try:
        logger = configurar_logging()
        logger.info("=" * 60)
        logger.info("INICIANDO SCRIPT DE INSERÇÃO DE DADOS CNPJ")
        logger.info("=" * 60)
        
        # Verificar pasta de dados
        pasta_saida, arquivos_csv = verificar_pasta_dados()
        
        # Obter configuração do banco
        config = obter_configuracao_banco()
        logger.info(f"Configuração: tipo_banco={config['tipo_banco']}, dbname={config['dbname']}, username={config['username']}, host={config['host']}")
        
        # Confirmar execução
        print(f"\nEste script irá INSERIR DADOS nas tabelas do banco {config['dbname'].upper()}")
        print(f"no servidor {config['tipo_banco']} {config['host']}")
        print(f"Arquivos a processar: {len(arquivos_csv)}")
        print("✅ Prosseguindo automaticamente...")
        
        # Conectar ao banco
        engine, engine_url = conectar_banco(config)
        
        # Definir colunas das tabelas
        colunas_estabelecimento = ['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 'nome_fantasia', 'situacao_cadastral','data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais', 'data_inicio_atividades', 'cnae_fiscal', 'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento','bairro', 'cep','uf','municipio', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial']
        
        colunas_empresas = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social_str', 'porte_empresa', 'ente_federativo_responsavel']
        
        colunas_socios = ['cnpj_basico', 'identificador_de_socio', 'nome_socio', 'cnpj_cpf_socio', 'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal', 'nome_representante', 'qualificacao_representante_legal', 'faixa_etaria']
        
        colunas_simples = ['cnpj_basico', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei']
        
        # Verificar estado atual das tabelas antes de começar
        logger.info("\n" + "="*50)
        logger.info("VERIFICANDO ESTADO ATUAL DAS TABELAS")
        logger.info("="*50)
        
        with engine.connect() as conn:
            tabelas_verificar = ['cnae', 'motivo', 'municipio', 'natureza_juridica', 'pais', 'qualificacao_socio', 'estabelecimento', 'socios_original', 'empresas', 'simples']
            for tabela in tabelas_verificar:
                try:
                    result = conn.execute(text(f'SELECT COUNT(*) as total FROM {tabela}'))
                    total = result.fetchone()[0]
                    logger.info(f"  {tabela:20}: {total:>10,} registros")
                except Exception as e:
                    logger.info(f"  {tabela:20}: {'NÃO EXISTE':>10}")
        
        # Carregar tabelas de códigos
        logger.info("\n" + "="*50)
        logger.info("CARREGANDO TABELAS DE CÓDIGOS")
        logger.info("="*50)
        
        carregar_tabela_codigo(engine, pasta_saida, '.CNAECSV', 'cnae')
        carregar_tabela_codigo(engine, pasta_saida, '.MOTICSV', 'motivo')
        carregar_tabela_codigo(engine, pasta_saida, '.MUNICCSV', 'municipio')
        carregar_tabela_codigo(engine, pasta_saida, '.NATJUCSV', 'natureza_juridica')
        carregar_tabela_codigo(engine, pasta_saida, '.PAISCSV', 'pais')
        carregar_tabela_codigo(engine, pasta_saida, '.QUALSCSV', 'qualificacao_socio')
        
        # Carregar tabelas principais
        logger.info("\n" + "="*50)
        logger.info("CARREGANDO TABELAS PRINCIPAIS")
        logger.info("="*50)
        
        carregar_tabela_principal(engine, engine_url, pasta_saida, 'estabelecimento', '.ESTABELE', colunas_estabelecimento)
        carregar_tabela_principal(engine, engine_url, pasta_saida, 'socios_original', '.SOCIOCSV', colunas_socios)
        carregar_tabela_principal(engine, engine_url, pasta_saida, 'empresas', '.EMPRECSV', colunas_empresas)
        carregar_tabela_principal(engine, engine_url, pasta_saida, 'simples', '.SIMPLES.CSV.*', colunas_simples)
        
        # Executar SQLs finais
        logger.info("\n" + "="*50)
        logger.info("EXECUTANDO OTIMIZAÇÕES FINAIS")
        logger.info("="*50)
        
        comandos_executados, comandos_com_erro = executar_sqls_finais(engine)
        
        # Inserir dados de referência
        logger.info("\n" + "="*50)
        logger.info("INSERINDO DADOS DE REFERÊNCIA")
        logger.info("="*50)
        
        data_referencia, qtde_cnpjs = inserir_dados_referencia(engine, pasta_saida)
        
        # Verificar integridade
        logger.info("\n" + "="*50)
        logger.info("VERIFICANDO INTEGRIDADE DOS DADOS")
        logger.info("="*50)
        
        verificacoes = verificar_integridade_dados(engine)
        
        # Resumo final
        logger.info("\n" + "="*60)
        logger.info("RESUMO DA INSERÇÃO DE DADOS")
        logger.info("="*60)
        logger.info(f"Tipo de banco: {config['tipo_banco']}")
        logger.info(f"Banco de dados: {config['dbname']}")
        logger.info(f"Data de referência: {data_referencia}")
        logger.info(f"Total de CNPJs: {qtde_cnpjs}")
        logger.info(f"Comandos SQL finais: {comandos_executados} executados, {comandos_com_erro} com erro")
        
        # Resumo das tabelas
        logger.info("\nResumo das tabelas:")
        for tabela, total, sucesso in verificacoes:
            status = "✓" if sucesso else "⚠"
            logger.info(f"  {status} {tabela}: {total} registros")
        
        if comandos_com_erro == 0:
            logger.info("\n✓ INSERÇÃO DE DADOS CONCLUÍDA COM SUCESSO!")
        else:
            logger.warning(f"\n⚠ INSERÇÃO CONCLUÍDA COM {comandos_com_erro} ERRO(S)")
        
        # Fechar conexão
        engine.dispose()
        logger.info("Conexão com o banco fechada")
        
    except Exception as e:
        logger.error(f"Erro crítico no script: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
