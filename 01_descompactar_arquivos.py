# -*- coding: utf-8 -*-
"""
Script 1: Descompactação de Arquivos CNPJ
==========================================

Este script é responsável por descompactar os arquivos ZIP da base de dados CNPJ.
Ele verifica se os arquivos já foram descompactados para evitar reprocessamento.

@author: rictom
https://github.com/rictom/cnpj-mysql
"""

import os
import sys
import glob
import zipfile
import logging
from datetime import datetime

# Configurar logging detalhado
def configurar_logging():
    """Configura o sistema de logging com arquivo e console"""
    # Criar pasta logs se não existir
    logs_dir = 'logs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Versão anterior: log_filename = f'cnpj_descompactacao_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_filename = os.path.join(logs_dir, f'cnpj_descompactacao_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def verificar_configuracao():
    """Verifica e retorna as configurações das pastas"""
    pasta_compactados = r"dados-publicos-zip"
    pasta_saida = r"dados-publicos"
    
    # Verificar se as pastas existem
    if not os.path.exists(pasta_compactados):
        raise FileNotFoundError(f"Pasta de arquivos compactados não encontrada: {pasta_compactados}")
    
    # Criar pasta de saída se não existir
    if not os.path.exists(pasta_saida):
        os.makedirs(pasta_saida)
        logger.info(f"Pasta de saída criada: {pasta_saida}")
    
    return pasta_compactados, pasta_saida

def verificar_arquivos_zip(pasta_compactados):
    """Verifica os arquivos ZIP disponíveis para descompactação"""
    arquivos_zip = list(glob.glob(os.path.join(pasta_compactados, r'*.zip')))
    
    if not arquivos_zip:
        raise FileNotFoundError(f"Nenhum arquivo ZIP encontrado em: {pasta_compactados}")
    
    logger.info(f"Encontrados {len(arquivos_zip)} arquivos ZIP para processar:")
    for arq in arquivos_zip:
        tamanho = os.path.getsize(arq) / (1024 * 1024)  # MB
        logger.info(f"  - {os.path.basename(arq)} ({tamanho:.2f} MB)")
    
    return arquivos_zip

def verificar_arquivos_existentes(pasta_saida, arquivo_zip):
    """Verifica se os arquivos de um ZIP já foram descompactados"""
    try:
        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            files_to_extract = zip_ref.namelist()
            
            # Verificar se todos os arquivos já existem
            files_already_exist = all(
                os.path.exists(os.path.join(pasta_saida, file)) 
                for file in files_to_extract
            )
            
            if files_already_exist:
                logger.info(f"Todos os arquivos de {os.path.basename(arquivo_zip)} já foram descompactados")
                return True
            else:
                # Verificar quantos arquivos já existem
                existing_files = sum(
                    1 for file in files_to_extract 
                    if os.path.exists(os.path.join(pasta_saida, file))
                )
                logger.info(f"Arquivo {os.path.basename(arquivo_zip)}: {existing_files}/{len(files_to_extract)} arquivos já existem")
                return False
                
    except Exception as e:
        logger.error(f"Erro ao verificar arquivo {arquivo_zip}: {str(e)}")
        return False

def descompactar_arquivo(arquivo_zip, pasta_saida):
    """Descompacta um arquivo ZIP específico"""
    try:
        logger.info(f"Iniciando descompactação de: {os.path.basename(arquivo_zip)}")
        
        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            # Listar conteúdo do ZIP
            files_to_extract = zip_ref.namelist()
            logger.info(f"Conteúdo do ZIP ({len(files_to_extract)} arquivos):")
            for file in files_to_extract:
                logger.debug(f"  - {file}")
            
            # Verificar espaço em disco
            total_size = sum(zip_ref.getinfo(file).file_size for file in files_to_extract)
            available_space = os.statvfs(pasta_saida).f_frsize * os.statvfs(pasta_saida).f_bavail
            
            if total_size > available_space:
                raise OSError(f"Espaço insuficiente em disco. Necessário: {total_size / (1024**3):.2f} GB, Disponível: {available_space / (1024**3):.2f} GB")
            
            logger.info(f"Espaço em disco suficiente. Tamanho total: {total_size / (1024**3):.2f} GB")
            
            # Descompactar
            start_time = datetime.now()
            zip_ref.extractall(pasta_saida)
            end_time = datetime.now()
            
            # Verificar se todos os arquivos foram extraídos
            extracted_files = []
            for file in files_to_extract:
                extracted_path = os.path.join(pasta_saida, file)
                if os.path.exists(extracted_path):
                    extracted_files.append(file)
                    file_size = os.path.getsize(extracted_path)
                    logger.debug(f"Arquivo extraído: {file} ({file_size / (1024**2):.2f} MB)")
            
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Descompactação concluída em {duration:.2f} segundos")
            logger.info(f"Arquivos extraídos: {len(extracted_files)}/{len(files_to_extract)}")
            
            # Verificar se algum arquivo não foi extraído
            if len(extracted_files) != len(files_to_extract):
                missing_files = set(files_to_extract) - set(extracted_files)
                logger.warning(f"Arquivos não extraídos: {missing_files}")
            
            return True
            
    except Exception as e:
        logger.error(f"Erro ao descompactar {arquivo_zip}: {str(e)}")
        return False

def main():
    """Função principal do script"""
    global logger
    
    try:
        logger = configurar_logging()
        logger.info("=" * 60)
        logger.info("INICIANDO SCRIPT DE DESCOMPACTAÇÃO DE ARQUIVOS CNPJ")
        logger.info("=" * 60)
        
        # Verificar configuração
        pasta_compactados, pasta_saida = verificar_configuracao()
        logger.info(f"Pasta de arquivos compactados: {pasta_compactados}")
        logger.info(f"Pasta de saída: {pasta_saida}")
        
        # Verificar arquivos ZIP
        arquivos_zip = verificar_arquivos_zip(pasta_compactados)
        
        # Confirmar execução
        resp = input(f'\nEste script irá descompactar arquivos na pasta "{pasta_saida}". Deseja prosseguir? (S/N): ')
        if not resp or resp.upper() != 'S':
            logger.info("Execução cancelada pelo usuário")
            return
        
        # Processar cada arquivo ZIP
        total_arquivos = len(arquivos_zip)
        arquivos_processados = 0
        arquivos_com_erro = 0
        
        for i, arquivo_zip in enumerate(arquivos_zip, 1):
            logger.info(f"\n{'='*50}")
            logger.info(f"Processando arquivo {i}/{total_arquivos}: {os.path.basename(arquivo_zip)}")
            logger.info(f"{'='*50}")
            
            # Verificar se já foi descompactado
            if verificar_arquivos_existentes(pasta_saida, arquivo_zip):
                logger.info(f"Arquivo {os.path.basename(arquivo_zip)} já foi processado, pulando...")
                arquivos_processados += 1
                continue
            
            # Descompactar arquivo
            if descompactar_arquivo(arquivo_zip, pasta_saida):
                arquivos_processados += 1
                logger.info(f"✓ Arquivo {os.path.basename(arquivo_zip)} processado com sucesso")
            else:
                arquivos_com_erro += 1
                logger.error(f"✗ Erro ao processar arquivo {os.path.basename(arquivo_zip)}")
        
        # Resumo final
        logger.info("\n" + "="*60)
        logger.info("RESUMO DA EXECUÇÃO")
        logger.info("="*60)
        logger.info(f"Total de arquivos ZIP: {total_arquivos}")
        logger.info(f"Arquivos processados com sucesso: {arquivos_processados}")
        logger.info(f"Arquivos com erro: {arquivos_com_erro}")
        logger.info(f"Pasta de saída: {pasta_saida}")
        
        # Verificar arquivos extraídos
        arquivos_extraidos = []
        for extensao in ['.EMPRECSV', '.ESTABELE', '.SOCIOCSV', '.CNAECSV', '.MOTICSV', '.MUNICCSV', '.NATJUCSV', '.PAISCSV', '.QUALSCSV', '.SIMPLES.CSV']:
            arquivos = glob.glob(os.path.join(pasta_saida, f'*{extensao}'))
            arquivos_extraidos.extend(arquivos)
        
        logger.info(f"Total de arquivos extraídos: {len(arquivos_extraidos)}")
        for arquivo in arquivos_extraidos:
            tamanho = os.path.getsize(arquivo) / (1024 * 1024)  # MB
            logger.info(f"  - {os.path.basename(arquivo)} ({tamanho:.2f} MB)")
        
        if arquivos_com_erro == 0:
            logger.info("\n✓ DESCOMPACTAÇÃO CONCLUÍDA COM SUCESSO!")
            logger.info(f"\nPróximo passo: Execute o script '02_criar_tabelas.py' para criar as tabelas no banco")
        else:
            logger.warning(f"\n⚠ DESCOMPACTAÇÃO CONCLUÍDA COM {arquivos_com_erro} ERRO(S)")
        
    except Exception as e:
        logger.error(f"Erro crítico no script: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
