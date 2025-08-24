# -*- coding: utf-8 -*-
"""
Script para Limpar Todas as Tabelas do Banco CNPJ
==================================================

Este script remove todos os dados de todas as tabelas do banco CNPJ.
Útil para limpar o banco antes de uma nova inserção de dados.

ATENÇÃO: Este script APAGA TODOS OS DADOS das tabelas!
"""

import os
import sys
import json
import logging
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

def configurar_logging():
    """Configura logging para o script"""
    # Criar pasta logs se não existir
    logs_dir = 'logs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Versão anterior: log_filename = f"cnpj_limpar_banco_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_filename = os.path.join(logs_dir, f"cnpj_limpar_banco_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def carregar_configuracao():
    """Carrega configuração do banco de dados"""
    try:
        if not os.path.exists('cnpj_config.json'):
            print("❌ Arquivo de configuração 'cnpj_config.json' não encontrado!")
            print("Execute primeiro o script 03_inserir_dados.py para criar a configuração.")
            return None
            
        with open('cnpj_config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        print("✅ Configuração carregada com sucesso!")
        return config
        
    except Exception as e:
        print(f"❌ Erro ao carregar configuração: {str(e)}")
        return None

def conectar_banco(config):
    """Conecta ao banco de dados"""
    try:
        logger.info("Conectando ao banco de dados...")
        
        if config['tipo_banco'] == 'postgres':
            port = config.get('port', 5432)
            engine_url = f"postgresql://{config['username']}:{config['password']}@{config['host']}:{port}/{config['dbname']}"
        else:
            port = config.get('port', 3306)
            engine_url = f"mysql+pymysql://{config['username']}:{config['password']}@{config['host']}:{port}/{config['dbname']}"
        
        engine = sqlalchemy.create_engine(engine_url)
        
        # Testar conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info("✅ Conexão estabelecida com sucesso!")
        return engine
        
    except Exception as e:
        logger.error(f"❌ Erro ao conectar com o banco: {str(e)}")
        return None

def listar_tabelas(engine):
    """Lista todas as tabelas do banco"""
    try:
        with engine.connect() as conn:
            if engine.url.drivername.startswith('postgresql'):
                # PostgreSQL
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """))
            else:
                # MySQL
                result = conn.execute(text("SHOW TABLES"))
            
            tabelas = [row[0] for row in result.fetchall()]
            return tabelas
            
    except Exception as e:
        logger.error(f"❌ Erro ao listar tabelas: {str(e)}")
        return []

def verificar_contadores(engine, tabelas):
    """Verifica contadores atuais das tabelas"""
    try:
        print("\n" + "="*80)
        print("CONTADORES ATUAIS DAS TABELAS")
        print("="*80)
        
        with engine.connect() as conn:
            for tabela in tabelas:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) as total FROM {tabela}"))
                    total = result.fetchone()[0]
                    print(f"  {tabela:25}: {total:>12,} registros")
                except Exception as e:
                    print(f"  {tabela:25}: {'ERRO':>12} - {str(e)}")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"❌ Erro ao verificar contadores: {str(e)}")

def confirmar_limpeza():
    """Solicita confirmação do usuário para limpar o banco"""
    print("\n" + "⚠️" * 20 + " ATENÇÃO " + "⚠️" * 20)
    print("Este script irá REMOVER TODOS OS DADOS de todas as tabelas!")
    print("Esta ação é IRREVERSÍVEL!")
    print("⚠️" * 50)
    
    while True:
        confirmacao = input("\nDigite 'LIMPAR' para confirmar a limpeza completa: ").strip()
        if confirmacao == 'LIMPAR':
            return True
        elif confirmacao.lower() in ['cancelar', 'cancel', 'exit', 'quit', 'sair']:
            return False
        else:
            print("❌ Confirmação incorreta. Digite 'LIMPAR' ou 'cancelar' para sair.")

def limpar_tabelas(engine, tabelas):
    """Remove todos os dados de todas as tabelas"""
    try:
        print("\n🔄 Iniciando limpeza das tabelas...")
        
        with engine.connect() as conn:
            for i, tabela in enumerate(tabelas, 1):
                try:
                    logger.info(f"Limpando tabela {i}/{len(tabelas)}: {tabela}")
                    
                    # Contar registros antes
                    result = conn.execute(text(f"SELECT COUNT(*) as total FROM {tabela}"))
                    total_antes = result.fetchone()[0]
                    
                    if total_antes == 0:
                        logger.info(f"  Tabela {tabela} já está vazia")
                        continue
                    
                    # Limpar tabela
                    start_time = datetime.now()
                    conn.execute(text(f"DELETE FROM {tabela}"))
                    end_time = datetime.now()
                    
                    # Verificar se foi limpa
                    result = conn.execute(text(f"SELECT COUNT(*) as total FROM {tabela}"))
                    total_depois = result.fetchone()[0]
                    
                    duracao = (end_time - start_time).total_seconds()
                    
                    if total_depois == 0:
                        logger.info(f"  ✅ {tabela}: {total_antes:,} registros removidos em {duracao:.2f}s")
                    else:
                        logger.warning(f"  ⚠️ {tabela}: {total_antes - total_depois:,} registros removidos, {total_depois:,} restantes")
                    
                    # Commit a cada tabela para não sobrecarregar a memória
                    conn.commit()
                    
                except Exception as e:
                    logger.error(f"  ❌ Erro ao limpar tabela {tabela}: {str(e)}")
                    conn.rollback()
                    continue
        
        logger.info("✅ Limpeza das tabelas concluída!")
        
    except Exception as e:
        logger.error(f"❌ Erro durante a limpeza: {str(e)}")
        raise

def verificar_limpeza(engine, tabelas):
    """Verifica se todas as tabelas foram limpas"""
    try:
        print("\n" + "="*80)
        print("VERIFICAÇÃO PÓS-LIMPEZA")
        print("="*80)
        
        with engine.connect() as conn:
            total_geral = 0
            tabelas_com_dados = []
            
            for tabela in tabelas:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) as total FROM {tabela}"))
                    total = result.fetchone()[0]
                    total_geral += total
                    
                    if total > 0:
                        tabelas_com_dados.append(tabela)
                        print(f"  ⚠️ {tabela:25}: {total:>12,} registros restantes")
                    else:
                        print(f"  ✅ {tabela:25}: {'0':>12} registros")
                        
                except Exception as e:
                    print(f"  ❌ {tabela:25}: {'ERRO':>12} - {str(e)}")
        
        print("="*80)
        print(f"Total geral de registros restantes: {total_geral:,}")
        
        if total_geral == 0:
            print("🎉 Todas as tabelas foram limpas com sucesso!")
        else:
            print(f"⚠️ {len(tabelas_com_dados)} tabelas ainda possuem dados")
            for tabela in tabelas_com_dados:
                print(f"  - {tabela}")
        
    except Exception as e:
        logger.error(f"❌ Erro ao verificar limpeza: {str(e)}")

def main():
    """Função principal do script"""
    print("="*80)
    print("SCRIPT DE LIMPEZA DO BANCO CNPJ")
    print("="*80)
    
    # Configurar logging
    global logger
    logger = configurar_logging()
    
    # Carregar configuração
    config = carregar_configuracao()
    if not config:
        return
    
    # Conectar ao banco
    engine = conectar_banco(config)
    if not engine:
        return
    
    try:
        # Listar tabelas
        logger.info("Listando tabelas do banco...")
        tabelas = listar_tabelas(engine)
        
        if not tabelas:
            logger.warning("Nenhuma tabela encontrada no banco!")
            return
        
        print(f"\n📋 Encontradas {len(tabelas)} tabelas:")
        for i, tabela in enumerate(tabelas, 1):
            print(f"  {i:2d}. {tabela}")
        
        # Verificar contadores atuais
        verificar_contadores(engine, tabelas)
        
        # Confirmar limpeza
        if not confirmar_limpeza():
            logger.info("Operação cancelada pelo usuário")
            return
        
        # Executar limpeza
        limpar_tabelas(engine, tabelas)
        
        # Verificar resultado
        verificar_limpeza(engine, tabelas)
        
        logger.info("🎯 Script de limpeza concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"❌ Erro durante execução: {str(e)}")
        raise
    
    finally:
        if engine:
            engine.dispose()
            logger.info("Conexão com o banco fechada")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Script interrompido pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro fatal: {str(e)}")
        sys.exit(1)
