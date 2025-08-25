# -*- coding: utf-8 -*-
"""
Script 2: Criação de Tabelas CNPJ
==================================

Este script é responsável por criar todas as tabelas necessárias para a base de dados CNPJ.
Ele inclui verificação de conexão, criação de tabelas e índices básicos.

@author: rictom
https://github.com/rictom/cnpj-mysql
"""

import os
import sys
import time
import logging
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
    
    # Versão anterior: log_filename = f'cnpj_criar_tabelas_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_filename = os.path.join(logs_dir, f'cnpj_criar_tabelas_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
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
    """Obtém a configuração do banco de dados com valores padrão PostgreSQL"""
    print("\n" + "="*60)
    print("CONFIGURAÇÃO DO BANCO DE DADOS POSTGRESQL")
    print("="*60)
    
    # Configurações padrão
    tipo_banco = 'postgres'
    dbname_padrao = 'cnpjbr'
    username_padrao = 'postgres'
    password_padrao = 'postgres123'
    host_padrao = '127.0.0.1'
    
    print(f"Configurações padrão:")
    print(f"  Tipo de banco: {tipo_banco}")
    print(f"  Banco de dados: {dbname_padrao}")
    print(f"  Usuário: {username_padrao}")
    print(f"  Host: {host_padrao}")
    
    # Parâmetros de conexão (com valores padrão)
    dbname = input(f"Nome do banco de dados [{dbname_padrao}]: ").strip() or dbname_padrao
    
    # Opção de usuário
    print("\nOpções de usuário:")
    print("1. Usar usuário existente")
    print("2. Criar novo usuário")
    
    while True:
        opcao_usuario = input("Escolha uma opção (1 ou 2): ").strip()
        if opcao_usuario in ['1', '2']:
            break
        print("Por favor, digite '1' ou '2'")
    
    if opcao_usuario == '1':
        # Usar usuário existente
        username = input(f"Nome do usuário existente [{username_padrao}]: ").strip() or username_padrao
        password = input(f"Senha do usuário [{password_padrao}]: ").strip() or password_padrao
        criar_usuario = False
    else:
        # Criar novo usuário
        username = input(f"Nome do novo usuário [{username_padrao}]: ").strip() or username_padrao
        password = input(f"Senha para o novo usuário [{password_padrao}]: ").strip() or password_padrao
        confirmar_senha = input("Confirme a senha: ").strip()
        
        if password != confirmar_senha:
            raise ValueError("As senhas não coincidem!")
        
        criar_usuario = True
    
    host = input(f"Host [{host_padrao}]: ").strip() or host_padrao
    
    return {
        'tipo_banco': tipo_banco,
        'dbname': dbname,
        'username': username,
        'password': password,
        'host': host,
        'criar_usuario': criar_usuario
    }

def testar_conexao(config):
    """Testa a conexão com o banco de dados e cria usuário se necessário"""
    try:
        logger.info("Testando conexão com o banco de dados...")
        
        # Primeiro tentar conectar diretamente com o usuário configurado
        logger.info("Tentando conectar com o usuário configurado...")
        
        port = config.get('port', 5432)
        engine_url = f"postgresql://{config['username']}:{config['password']}@{config['host']}:{port}/{config['dbname']}"
        logger.info("Criando engine PostgreSQL...")
        
        try:
            engine_ = sqlalchemy.create_engine(engine_url)
            logger.info("Engine criado com sucesso")
            
            # Testar conexão
            logger.info("Testando conexão...")
            with engine_.connect() as conn:
                # Executar query simples para testar
                result = conn.execute(text("SELECT 1 as test"))
                
                logger.info("✓ Conexão estabelecida com sucesso!")
                
                # Se precisar criar usuário, verificar se tem permissões para criar tabelas
                if config['criar_usuario']:
                    logger.info("Verificando permissões para criar tabelas...")
                    try:
                        # Tentar criar uma tabela temporária para testar permissões
                        conn.execute(text("CREATE TEMP TABLE test_permissions (id int)"))
                        conn.execute(text("DROP TABLE test_permissions"))
                        logger.info("✓ Usuário tem permissões para criar tabelas")
                        return engine_, engine_url
                    except Exception as perm_error:
                        logger.info(f"Usuário não tem permissões para criar tabelas: {str(perm_error)}")
                        logger.info("Tentando conceder permissões adicionais...")
                        # Fechar conexão atual para usar postgres
                        engine_.dispose()
                        raise Exception("Permissões insuficientes")
                
                return engine_, engine_url
                
        except Exception as e:
            logger.info(f"Falha na conexão direta ou permissões insuficientes: {str(e)}")
            
            # Se falhou e precisar criar usuário, tentar criar
            if config['criar_usuario']:
                logger.info("Tentando criar usuário e banco...")
                
                # Conectar como postgres para criar usuário
                postgres_username = 'postgres'
                postgres_password = 'postgres123'  # Corrigido: usar senha correta
                
                logger.info(f"Conectando como usuário administrador: {postgres_username}")
                
                postgres_engine_url = f"postgresql://{postgres_username}:{postgres_password}@{config['host']}:{port}/postgres"
                postgres_engine = sqlalchemy.create_engine(postgres_engine_url, isolation_level="AUTOCOMMIT")
                
                # Criar usuário e banco se não existir
                with postgres_engine.connect() as conn:
                    # Verificar se é superusuário
                    result = conn.execute(text("SELECT rolname, rolsuper FROM pg_roles WHERE rolname = current_user"))
                    user_info = result.fetchone()
                    
                    if not user_info or not user_info[1]:
                        raise Exception(f"Usuário '{postgres_username}' não tem privilégios de superusuário")
                    
                    logger.info(f"Conectado como superusuário: {postgres_username}")
                    
                    # Criar banco se não existir
                    conn.execute(text(f"CREATE DATABASE {config['dbname']}"))
                    logger.info(f"Banco de dados '{config['dbname']}' criado/verificado")
                    
                    # Criar usuário se não existir
                    conn.execute(text(f"CREATE USER {config['username']} WITH PASSWORD '{config['password']}'"))
                    logger.info(f"Usuário '{config['username']}' criado")
                    
                    # Conceder privilégios
                    conn.execute(text(f"GRANT ALL PRIVILEGES ON DATABASE {config['dbname']} TO {config['username']}"))
                    logger.info(f"Privilégios concedidos ao usuário '{config['username']}'")
                    
                    # Conceder permissões no schema public
                    conn.execute(text(f"GRANT ALL ON SCHEMA public TO {config['username']}"))
                    conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {config['username']}"))
                    conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {config['username']}"))
                    conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {config['username']}"))
                    conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {config['username']}"))
                    logger.info(f"Permissões no schema public concedidas ao usuário '{config['username']}'")
                
                postgres_engine.dispose()
                logger.info(f"✓ Usuário {config['username']} criado com sucesso no PostgreSQL")
                
                # Agora tentar conectar novamente com o usuário criado
                logger.info("Tentando conectar com o usuário recém-criado...")
                engine_ = sqlalchemy.create_engine(engine_url)
                
                with engine_.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test"))
                    logger.info("✓ Conexão estabelecida com sucesso após criação do usuário!")
                    return engine_, engine_url
            else:
                # Se não precisar criar usuário, re-raise o erro
                raise e
            
    except Exception as e:
        logger.error(f"✗ Erro ao conectar com o banco: {str(e)}")
        raise

def obter_sql_criacao_tabelas():
    """Retorna o SQL para criação das tabelas PostgreSQL"""
    
    sql_completo = '''
    DROP TABLE IF EXISTS cnae;
    CREATE TABLE cnae (
        codigo VARCHAR(7),
        descricao VARCHAR(200)
    );
    
    DROP TABLE IF EXISTS empresas;
    CREATE TABLE empresas (
        cnpj_basico VARCHAR(8),
        razao_social VARCHAR(200),
        natureza_juridica VARCHAR(4),
        qualificacao_responsavel VARCHAR(2),
        capital_social_str VARCHAR(20),
        porte_empresa VARCHAR(2),
        ente_federativo_responsavel VARCHAR(50)
    );
    
    DROP TABLE IF EXISTS estabelecimento;
    CREATE TABLE estabelecimento (
        cnpj_basico VARCHAR(8),
        cnpj_ordem VARCHAR(4),
        cnpj_dv VARCHAR(2),
        matriz_filial VARCHAR(1),
        nome_fantasia VARCHAR(200),
        situacao_cadastral VARCHAR(2),
        data_situacao_cadastral VARCHAR(8),
        motivo_situacao_cadastral VARCHAR(2),
        nome_cidade_exterior VARCHAR(200),
        pais VARCHAR(3),
        data_inicio_atividades VARCHAR(8),
        cnae_fiscal VARCHAR(7),
        cnae_fiscal_secundaria VARCHAR(1000),
        tipo_logradouro VARCHAR(20),
        logradouro VARCHAR(200),
        numero VARCHAR(10),
        complemento VARCHAR(200),
        bairro VARCHAR(200),
        cep VARCHAR(8),
        uf VARCHAR(2),
        municipio VARCHAR(4),
        ddd1 VARCHAR(4),
        telefone1 VARCHAR(8),
        ddd2 VARCHAR(4),
        telefone2 VARCHAR(8),
        ddd_fax VARCHAR(4),
        fax VARCHAR(8),
        correio_eletronico VARCHAR(200),
        situacao_especial VARCHAR(200),
        data_situacao_especial VARCHAR(8)
    );
    
    DROP TABLE IF EXISTS motivo;
    CREATE TABLE motivo (
        codigo VARCHAR(2),
        descricao VARCHAR(200)
    );
    
    DROP TABLE IF EXISTS municipio;
    CREATE TABLE municipio (
        codigo VARCHAR(4),
        descricao VARCHAR(200)
    );
    
    DROP TABLE IF EXISTS natureza_juridica;
    CREATE TABLE natureza_juridica (
        codigo VARCHAR(4),
        descricao VARCHAR(200)
    );
    
    DROP TABLE IF EXISTS pais;
    CREATE TABLE pais (
        codigo VARCHAR(3),
        descricao VARCHAR(200)
    );
    
    DROP TABLE IF EXISTS qualificacao_socio;
    CREATE TABLE qualificacao_socio (
        codigo VARCHAR(2),
        descricao VARCHAR(200)
    );
    
    DROP TABLE IF EXISTS simples;
    CREATE TABLE simples (
        cnpj_basico VARCHAR(8),
        opcao_simples VARCHAR(1),
        data_opcao_simples VARCHAR(8),
        data_exclusao_simples VARCHAR(8),
        opcao_mei VARCHAR(1),
        data_opcao_mei VARCHAR(8),
        data_exclusao_mei VARCHAR(8)
    );
    
    DROP TABLE IF EXISTS socios_original;
    CREATE TABLE socios_original (
        cnpj_basico VARCHAR(8),
        identificador_de_socio VARCHAR(1),
        nome_socio VARCHAR(200),
        cnpj_cpf_socio VARCHAR(14),
        qualificacao_socio VARCHAR(2),
        data_entrada_sociedade VARCHAR(8),
        pais VARCHAR(3),
        representante_legal VARCHAR(11),
        nome_representante VARCHAR(200),
        qualificacao_representante_legal VARCHAR(2),
        faixa_etaria VARCHAR(1)
    );
    
    -- Comentários nas tabelas para documentação
    COMMENT ON TABLE cnae IS 'Classificação Nacional de Atividades Econômicas';
    COMMENT ON TABLE empresas IS 'Dados das empresas matrizes';
    COMMENT ON TABLE estabelecimento IS 'Dados dos estabelecimentos (matrizes e filiais)';
    COMMENT ON TABLE motivo IS 'Motivos da situação cadastral';
    COMMENT ON TABLE municipio IS 'Municípios brasileiros';
    COMMENT ON TABLE natureza_juridica IS 'Naturezas jurídicas das empresas';
    COMMENT ON TABLE pais IS 'Países';
    COMMENT ON TABLE qualificacao_socio IS 'Qualificações dos sócios';
    COMMENT ON TABLE simples IS 'Dados do Simples Nacional';
    COMMENT ON TABLE socios_original IS 'Dados originais dos sócios';
    '''
    
    return sql_completo

def executar_sql_por_partes(engine, sql_completo, descricao):
    """Executa SQL dividido em partes para melhor controle e logging"""
    logger.info(f"Iniciando {descricao}...")
    
    # Dividir SQL em comandos individuais
    comandos = [cmd.strip() for cmd in sql_completo.split(';') if cmd.strip()]
    
    logger.info(f"Total de comandos SQL a executar: {len(comandos)}")
    
    comandos_executados = 0
    comandos_com_erro = 0
    
    for i, comando in enumerate(comandos, 1):
        try:
            logger.info(f"Executando comando {i}/{len(comandos)}")
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
    
    logger.info(f"Resumo {descricao}: {comandos_executados} executados, {comandos_com_erro} com erro")
    return comandos_executados, comandos_com_erro

def verificar_tabelas_criadas(engine):
    """Verifica se todas as tabelas foram criadas corretamente"""
    logger.info("Verificando tabelas criadas...")
    
    tabelas_esperadas = [
        'cnae', 'empresas', 'estabelecimento', 'motivo', 'municipio',
        'natureza_juridica', 'pais', 'qualificacao_socio', 'simples', 'socios_original'
    ]
    
    tabelas_criadas = []
    tabelas_faltando = []
    
    for tabela in tabelas_esperadas:
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT tablename FROM pg_tables WHERE tablename = '{tabela}'"))
                
                if result.fetchone():
                    tabelas_criadas.append(tabela)
                    logger.info(f"✓ Tabela {tabela} criada com sucesso")
                else:
                    tabelas_faltando.append(tabela)
                    logger.warning(f"⚠ Tabela {tabela} não encontrada")
                    
        except Exception as e:
            logger.error(f"Erro ao verificar tabela {tabela}: {str(e)}")
            tabelas_faltando.append(tabela)
    
    logger.info(f"Tabelas criadas: {len(tabelas_criadas)}/{len(tabelas_esperadas)}")
    
    if tabelas_faltando:
        logger.warning(f"Tabelas faltando: {tabelas_faltando}")
    
    return tabelas_criadas, tabelas_faltando

def main():
    """Função principal do script"""
    global logger
    
    try:
        logger = configurar_logging()
        logger.info("=" * 60)
        logger.info("INICIANDO SCRIPT DE CRIAÇÃO DE TABELAS CNPJ")
        logger.info("=" * 60)
        
        # Obter configuração do banco
        config = obter_configuracao_banco()
        logger.info(f"Configuração: tipo_banco={config['tipo_banco']}, dbname={config['dbname']}, username={config['username']}, host={config['host']}")
        
        if config['criar_usuario']:
            logger.info("Modo: Criar novo usuário e banco de dados")
            print(f"\nEste script irá:")
            print(f"1. CRIAR um novo usuário '{config['username']}' no {config['tipo_banco']}")
            print(f"2. CRIAR o banco de dados '{config['dbname']}' se não existir")
            print(f"3. CRIAR/REESCREVER TABELAS no banco {config['dbname'].upper()}")
            print(f"no servidor {config['tipo_banco']} {config['host']}")
        else:
            logger.info("Modo: Usar usuário existente")
            print(f"\nEste script irá CRIAR/REESCREVER TABELAS no banco {config['dbname'].upper()}")
            print(f"no servidor {config['tipo_banco']} {config['host']}")
            print(f"usando o usuário existente '{config['username']}'")
        
        resp = input("Deseja prosseguir? (S/N): ")
        if not resp or resp.upper() != 'S':
            logger.info("Execução cancelada pelo usuário")
            return
        
        # Testar conexão
        engine, engine_url = testar_conexao(config)
        
        # Obter SQL de criação
        sql_criacao = obter_sql_criacao_tabelas()
        
        # Executar criação das tabelas
        comandos_executados, comandos_com_erro = executar_sql_por_partes(
            engine, sql_criacao, "criação de tabelas"
        )
        
        # Verificar tabelas criadas
        tabelas_criadas, tabelas_faltando = verificar_tabelas_criadas(engine)
        
        # Salvar configuração para uso no próximo script
        try:
            import json
            config_para_salvar = {
                'tipo_banco': config['tipo_banco'],
                'dbname': config['dbname'],
                'username': config['username'],
                'host': config['host'],
                'criar_usuario': config['criar_usuario']
            }
            
            with open('cnpj_config.json', 'w', encoding='utf-8') as f:
                json.dump(config_para_salvar, f, indent=2, ensure_ascii=False)
            
            logger.info("Configuração salva em 'cnpj_config.json' para uso no próximo script")
        except Exception as e:
            logger.warning(f"Erro ao salvar configuração: {str(e)}")
        
        # Resumo final
        logger.info("\n" + "="*60)
        logger.info("RESUMO DA CRIAÇÃO DE TABELAS")
        logger.info("="*60)
        logger.info(f"Tipo de banco: {config['tipo_banco']}")
        logger.info(f"Banco de dados: {config['dbname']}")
        logger.info(f"Usuário: {config['username']}")
        if config['criar_usuario']:
            logger.info("Status: Novo usuário criado com sucesso")
        else:
            logger.info("Status: Usuário existente utilizado")
        logger.info(f"Comandos SQL executados: {comandos_executados}")
        logger.info(f"Comandos com erro: {comandos_com_erro}")
        logger.info(f"Tabelas criadas: {len(tabelas_criadas)}/{len(tabelas_criadas) + len(tabelas_faltando)}")
        
        if comandos_com_erro == 0 and len(tabelas_faltando) == 0:
            logger.info("\n✓ CRIAÇÃO DE TABELAS CONCLUÍDA COM SUCESSO!")
            logger.info("As tabelas estão prontas para receber os dados.")
            
            if config['criar_usuario']:
                logger.info(f"\nIMPORTANTE: Credenciais do novo usuário criado:")
                logger.info(f"  Usuário: {config['username']}")
                logger.info(f"  Senha: {config['password']}")
                logger.info(f"  Banco: {config['dbname']}")
                logger.info(f"  Host: {config['host']}")
                logger.info("Guarde essas informações para uso futuro!")
            
            logger.info(f"\nPróximo passo: Execute o script '03_inserir_dados.py' para carregar os dados")
        else:
            logger.warning(f"\n⚠ CRIAÇÃO DE TABELAS CONCLUÍDA COM PROBLEMAS")
            if comandos_com_erro > 0:
                logger.warning(f"  - {comandos_com_erro} comandos SQL com erro")
            if tabelas_faltando:
                logger.warning(f"  - Tabelas faltando: {tabelas_faltando}")
        
        # Fechar conexão
        engine.dispose()
        logger.info("Conexão com o banco fechada")
        
    except Exception as e:
        logger.error(f"Erro crítico no script: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
