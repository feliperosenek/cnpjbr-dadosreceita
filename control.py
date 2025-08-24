# -*- coding: utf-8 -*-
"""
Sistema de Controle e Gerenciamento CNPJ
=========================================

Este script gerencia todos os processos relacionados ao sistema CNPJ:
- Control: Monitoramento e verificação
- Process: Gerenciamento de processos em background
- Database: Configuração interativa de conexão com banco de dados
"""

import os
import sys
import time
import logging
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
import psutil
import subprocess
import json
import signal
import getpass

# Dicionário para armazenar processos ativos
processos_ativos = {}

def configurar_logging():
    """Configura logging simples"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def conectar_banco():
    """Conecta ao banco usando configuração salva"""
    try:
        if not os.path.exists('cnpj_config.json'):
            print("Arquivo de configuração não encontrado!")
            return None
            
        with open('cnpj_config.json', 'r') as f:
            config = json.load(f)
        
        # Conectar ao banco
        if config['tipo_banco'] == 'mysql':
            port = config.get('port', 3306)
            engine_url = f"mysql+pymysql://{config['username']}:{config['password']}@{config['host']}:{port}/{config['dbname']}"
        else:
            port = config.get('port', 5432)
            engine_url = f"postgresql://{config['username']}:{config['password']}@{config['host']}:{port}/{config['dbname']}"
        
        engine = sqlalchemy.create_engine(engine_url)
        
        # Testar conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print(f"✓ Conectado ao banco {config['dbname']} ({config['tipo_banco']})")
        return engine
        
    except Exception as e:
        print(f"✗ Erro ao conectar: {str(e)}")
        return None





def verificar_contadores(engine):
    """Verifica contadores das tabelas principais"""
    try:
        with engine.connect() as conn:
            # Verificar tabelas principais
            tabelas = ['estabelecimento', 'empresas', 'socios_original', 'simples', 'cnae', 'motivo', 'municipio', 'natureza_juridica', 'pais', 'qualificacao_socio']
            
            print("\n" + "="*60)
            print("CONTADORES EM TEMPO REAL")
            print("="*60)
            
            for tabela in tabelas:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) as total FROM {tabela}"))
                    total = result.fetchone()[0]
                    print(f"  {tabela:20}: {total:>10,} registros")
                except Exception as e:
                    print(f"  {tabela:20}: {'ERRO':>10} - {str(e)}")
            
            print("="*60)
            
    except Exception as e:
        print(f"Erro ao verificar contadores: {str(e)}")

def verificar_ultimos_registros(engine):
    """Verifica os últimos registros inseridos"""
    try:
        with engine.connect() as conn:
            print("\nÚLTIMOS REGISTROS INSERIDOS:")
            print("-" * 40)
            
            # Verificar estabelecimento (tabela principal)
            try:
                result = conn.execute(text("""
                    SELECT cnpj_basico, nome_fantasia, uf, municipio 
                    FROM estabelecimento 
                    ORDER BY cnpj_basico DESC 
                    LIMIT 5
                """))
                
                registros = result.fetchall()
                if registros:
                    print("Estabelecimento (últimos 5):")
                    for reg in registros:
                        print(f"  CNPJ: {reg[0]}, Nome: {reg[1][:30]}, UF: {reg[2]}, Município: {reg[3]}")
                else:
                    print("  Nenhum registro na tabela estabelecimento")
                    
            except Exception as e:
                print(f"  Erro ao verificar estabelecimento: {str(e)}")
            
            # Verificar empresas
            try:
                result = conn.execute(text("""
                    SELECT cnpj_basico, razao_social, porte_empresa 
                    FROM empresas 
                    ORDER BY cnpj_basico DESC 
                    LIMIT 3
                """))
                
                registros = result.fetchall()
                if registros:
                    print("\nEmpresas (últimos 3):")
                    for reg in registros:
                        print(f"  CNPJ: {reg[0]}, Razão: {reg[1][:40]}, Porte: {reg[2]}")
                else:
                    print("  Nenhum registro na tabela empresas")
                    
            except Exception as e:
                print(f"  Erro ao verificar empresas: {str(e)}")
                
    except Exception as e:
        print(f"Erro ao verificar últimos registros: {str(e)}")

def monitorar_insercao(engine, intervalo=30):
    """Monitora a inserção de dados em tempo real"""
    print(f"\n🔍 MONITORANDO INSERÇÃO DE DADOS (a cada {intervalo} segundos)")
    print("Pressione Ctrl+C para parar\n")
    
    try:
        while True:
            # Limpar tela (simples)
            os.system('clear')
            
            # Mostrar timestamp
            print(f"📊 VERIFICAÇÃO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Verificar contadores
            verificar_contadores(engine)
            
            # Verificar últimos registros
            verificar_ultimos_registros(engine)
            
            print(f"\n⏰ Próxima verificação em {intervalo} segundos...")
            print("Pressione Ctrl+C para parar")
            
            time.sleep(intervalo)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Monitoramento interrompido pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro no monitoramento: {str(e)}")

def verificar_sistema():
    """Verifica dados de processamento, memória e disco"""
    try:
        print("\n" + "="*60)
        print("INFORMAÇÕES DO SISTEMA")
        print("="*60)
        
        # Informações de CPU
        print("\n🖥️  PROCESSAMENTO:")
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()
        cpu_freq = psutil.cpu_freq()
        print(f"  Uso de CPU: {cpu_percent}%")
        print(f"  Núcleos: {cpu_count}")
        if cpu_freq:
            print(f"  Frequência: {cpu_freq.current:.1f} MHz")
        
        # Informações de memória
        print("\n💾 MEMÓRIA:")
        memory = psutil.virtual_memory()
        print(f"  Total: {memory.total / (1024**3):.1f} GB")
        print(f"  Disponível: {memory.available / (1024**3):.1f} GB")
        print(f"  Usada: {memory.used / (1024**3):.1f} GB")
        print(f"  Percentual usado: {memory.percent}%")
        
        # Informações de disco
        print("\n💿 DISCO:")
        disk = psutil.disk_usage('/')
        print(f"  Total: {disk.total / (1024**3):.1f} GB")
        print(f"  Usado: {disk.used / (1024**3):.1f} GB")
        print(f"  Livre: {disk.free / (1024**3):.1f} GB")
        print(f"  Percentual usado: {disk.percent}%")
        
        # Verificar espaço na pasta de dados
        pasta_dados = "dados-publicos"
        if os.path.exists(pasta_dados):
            try:
                total_size = sum(os.path.getsize(os.path.join(dirpath, filename))
                    for dirpath, dirnames, filenames in os.walk(pasta_dados)
                    for filename in filenames)
                print(f"  Pasta dados: {total_size / (1024**3):.1f} GB")
            except Exception as e:
                print(f"  Pasta dados: Erro ao calcular tamanho - {str(e)}")
        
        # Informações de rede (opcional)
        print("\n🌐 REDE:")
        try:
            net_io = psutil.net_io_counters()
            print(f"  Bytes enviados: {net_io.bytes_sent / (1024**2):.1f} MB")
            print(f"  Bytes recebidos: {net_io.bytes_recv / (1024**2):.1f} MB")
        except Exception as e:
            print(f"  Erro ao obter informações de rede: {str(e)}")
        
        print("="*60)
        
    except Exception as e:
        print(f"Erro ao verificar sistema: {str(e)}")

def executar_limpar_banco():
    """Executa o script limpar_banco.py"""
    try:
        print("\n" + "="*60)
        print("EXECUTANDO SCRIPT DE LIMPEZA DO BANCO")
        print("="*60)
        print("⚠️  ATENÇÃO: Este script irá APAGAR TODOS OS DADOS das tabelas!")
        print("="*60)
        
        # Confirmar execução
        resp = input("\nTem certeza que deseja executar a limpeza? (S/N): ").strip()
        if resp.upper() != 'S':
            print("Operação cancelada pelo usuário")
            return
        
        # Verificar se o arquivo existe
        if not os.path.exists('limpar_banco.py'):
            print("❌ Arquivo 'limpar_banco.py' não encontrado!")
            return
        
        # Executar o script
        print("\n🚀 Executando script de limpeza...")
        
        try:
            # Executar o script em um processo separado
            result = subprocess.run([sys.executable, 'limpar_banco.py'], 
                                  capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                print("✅ Script de limpeza executado com sucesso!")
                if result.stdout:
                    print("\nSaída do script:")
                    print(result.stdout)
            else:
                print("❌ Erro na execução do script de limpeza!")
                if result.stderr:
                    print("\nErros:")
                    print(result.stderr)
                    
        except subprocess.TimeoutExpired:
            print("⏰ Timeout: O script demorou mais de 5 minutos para executar")
        except Exception as e:
            print(f"❌ Erro ao executar script: {str(e)}")
            
    except Exception as e:
        print(f"Erro ao executar limpeza: {str(e)}")

def menu_control():
    """Menu do Control - Monitoramento e verificação"""
    # Conectar ao banco
    engine = conectar_banco()
    if not engine:
        print("❌ Não foi possível conectar ao banco. Verifique a configuração.")
        return
    
    while True:
        print("\n" + "="*60)
        print("CONTROL - MONITORAMENTO E VERIFICAÇÃO")
        print("="*60)
        print("1. Verificar contadores atual")
        print("2. Verificar últimos registros")
        print("3. Monitorar inserção em tempo real (30s)")
        print("4. Monitorar inserção em tempo real (10s)")
        print("5. Verificar sistema (CPU, Memória, Disco)")
        print("6. Executar limpeza do banco")
        print("7. Voltar ao menu principal")
        
        opcao = input("\nEscolha uma opção (1-7): ").strip()
        
        if opcao == '1':
            verificar_contadores(engine)
        elif opcao == '2':
            verificar_ultimos_registros(engine)
        elif opcao == '3':
            monitorar_insercao(engine, 30)
        elif opcao == '4':
            monitorar_insercao(engine, 10)
        elif opcao == '5':
            verificar_sistema()
        elif opcao == '6':
            executar_limpar_banco()
        elif opcao == '7':
            break
        else:
            print("Opção inválida!")
    
    # Fechar conexão
    engine.dispose()
    print("✅ Conexão com o banco fechada")

def listar_processos():
    """Lista todos os processos ativos"""
    # Primeiro atualizar o status dos processos
    atualizar_status_processos_silencioso()
    
    print("\n" + "="*80)
    print("PROCESSOS ATIVOS")
    print("="*80)
    
    if not processos_ativos:
        print("Nenhum processo ativo encontrado.")
        return
    
    # Converter para lista numerada
    processos_lista = list(processos_ativos.items())
    
    for i, (script, info) in enumerate(processos_lista, 1):
        status = "🟢" if info['status'] == 'running' else "🔴"
        tempo_execucao = datetime.now() - info['inicio']
        print(f"{i:2d}. {status} {script}")
        print(f"    PID: {info['pid']}")
        print(f"    Status: {info['status']}")
        print(f"    Início: {info['inicio'].strftime('%H:%M:%S')}")
        print(f"    Tempo: {tempo_execucao}")
        print(f"    Log: {info['log_file']}")
        print()

def ver_log_processo(numero):
    """Mostra o log de um processo específico por número"""
    try:
        numero = int(numero)
        if numero < 1 or numero > len(processos_ativos):
            print(f"❌ Número inválido! Escolha entre 1 e {len(processos_ativos)}")
            return
        
        # Obter script pelo número
        processos_lista = list(processos_ativos.keys())
        script_name = processos_lista[numero - 1]
        
        info = processos_ativos[script_name]
        log_file = info['log_file']
        
        if not os.path.exists(log_file):
            print(f"❌ Arquivo de log '{log_file}' não encontrado!")
            return
        
        print(f"\n📋 LOG DO PROCESSO: {script_name}")
        print(f"Arquivo: {log_file}")
        print("="*60)
        
        try:
            with open(log_file, 'r') as f:
                # Mostrar últimas 50 linhas
                linhas = f.readlines()
                if len(linhas) > 50:
                    print("... (mostrando últimas 50 linhas)")
                    linhas = linhas[-50:]
                
                for linha in linhas:
                    print(linha.rstrip())
                    
        except Exception as e:
            print(f"❌ Erro ao ler log: {str(e)}")
            
    except ValueError:
        print("❌ Por favor, digite um número válido!")

def parar_processo(numero):
    """Para um processo específico por número"""
    try:
        numero = int(numero)
        if numero < 1 or numero > len(processos_ativos):
            print(f"❌ Número inválido! Escolha entre 1 e {len(processos_ativos)}")
            return
        
        # Obter script pelo número
        processos_lista = list(processos_ativos.keys())
        script_name = processos_lista[numero - 1]
        
        info = processos_ativos[script_name]
        pid = info['pid']
        
        try:
            # Primeiro verificar se o processo ainda existe
            try:
                os.kill(pid, 0)  # Verificar se processo existe
            except OSError:
                print(f"✅ Processo {pid} ({script_name}) já terminou")
                del processos_ativos[script_name]
                return
            
            # Tentar parar o processo
            os.kill(pid, signal.SIGTERM)
            print(f"🛑 Enviado sinal de parada para processo {pid} ({script_name})")
            
            # Aguardar um pouco e verificar se parou
            time.sleep(2)
            try:
                os.kill(pid, 0)  # Verificar se processo ainda existe
                # Se chegou aqui, processo ainda está rodando, forçar parada
                os.kill(pid, signal.SIGKILL)
                print(f"💀 Processo {pid} forçado a parar")
                time.sleep(1)  # Aguardar um pouco mais
            except OSError:
                print(f"✅ Processo {pid} parou com sucesso")
            
            # Remover do dicionário
            del processos_ativos[script_name]
            
        except Exception as e:
            print(f"❌ Erro ao parar processo: {str(e)}")
            
    except ValueError:
        print("❌ Por favor, digite um número válido!")

def menu_process():
    """Menu de gerenciamento de processos"""
    while True:
        print("\n" + "="*60)
        print("PROCESS - GERENCIAMENTO DE PROCESSOS")
        print("="*60)
        print("1. Listar processos ativos")
        print("2. Ver log de um processo")
        print("3. Parar processo")
        print("4. Atualizar status dos processos")
        print("5. Voltar ao menu principal")
        
        opcao = input("\nEscolha uma opção (1-5): ").strip()
        
        if opcao == '1':
            listar_processos()
        elif opcao == '2':
            if processos_ativos:
                print("\nProcessos disponíveis:")
                listar_processos()
                numero = input("\nDigite o número do processo: ").strip()
                ver_log_processo(numero)
            else:
                print("Nenhum processo ativo para verificar logs.")
        elif opcao == '3':
            if processos_ativos:
                print("\nProcessos disponíveis:")
                listar_processos()
                numero = input("\nDigite o número do processo para parar: ").strip()
                parar_processo(numero)
            else:
                print("Nenhum processo ativo para parar.")
        elif opcao == '4':
            atualizar_status_processos()
        elif opcao == '5':
            break
        else:
            print("Opção inválida!")

def configurar_database():
    """Configurar conexão com banco de dados"""
    print("\n" + "="*60)
    print("DATABASE - CONFIGURAÇÃO DE CONEXÃO")
    print("="*60)
    
    # Verificar se já existe configuração
    config_existente = None
    if os.path.exists('cnpj_config.json'):
        try:
            with open('cnpj_config.json', 'r', encoding='utf-8') as f:
                config_existente = json.load(f)
            print("📋 Configuração atual encontrada:")
            print(f"  Tipo de banco: {config_existente.get('tipo_banco', 'N/A')}")
            print(f"  Host: {config_existente.get('host', 'N/A')}")
            print(f"  Porta: {config_existente.get('port', 'N/A')}")
            print(f"  Banco de dados: {config_existente.get('dbname', 'N/A')}")
            print(f"  Usuário: {config_existente.get('username', 'N/A')}")
            print()
        except Exception as e:
            print(f"⚠️  Erro ao ler configuração existente: {str(e)}")
            config_existente = None
    
    print("🔧 Configure a conexão com o banco de dados:")
    print()
    
    # Tipo de banco
    print("Tipo de banco disponível:")
    print("  PostgreSQL")
    
    tipo_banco = 'postgres'
    porta_padrao = 5432
    
    print(f"\n✅ Tipo selecionado: {tipo_banco.upper()}")
    
    # Configurações de conexão
    print("\n📡 Configurações de conexão:")
    
    # Host
    host_atual = config_existente.get('host', '127.0.0.1') if config_existente else '127.0.0.1'
    host = input(f"Host [{host_atual}]: ").strip() or host_atual
    
    # Porta
    porta_atual = config_existente.get('port', porta_padrao) if config_existente else porta_padrao
    while True:
        porta_input = input(f"Porta [{porta_atual}]: ").strip()
        if not porta_input:
            porta = porta_atual
            break
        try:
            porta = int(porta_input)
            if 1 <= porta <= 65535:
                break
            else:
                print("❌ Porta deve estar entre 1 e 65535!")
        except ValueError:
            print("❌ Porta deve ser um número!")
    
    # Nome do banco
    dbname_atual = config_existente.get('dbname', 'cnpjbr') if config_existente else 'cnpjbr'
    dbname = input(f"Nome do banco de dados [{dbname_atual}]: ").strip() or dbname_atual
    
    # Usuário
    username_atual = config_existente.get('username', 'postgres') if config_existente else 'postgres'
    username = input(f"Usuário [{username_atual}]: ").strip() or username_atual
    
    # Senha
    senha_atual = config_existente.get('password', '') if config_existente else ''
    if senha_atual:
        usar_senha_atual = input(f"Usar senha atual? (S/N) [S]: ").strip().upper()
        if usar_senha_atual in ['', 'S', 'SIM']:
            password = senha_atual
        else:
            password = getpass.getpass("Nova senha: ")
    else:
        password = getpass.getpass("Senha: ")
    
    # Criar objeto de configuração
    config = {
        'tipo_banco': tipo_banco,
        'host': host,
        'port': porta,
        'dbname': dbname,
        'username': username,
        'password': password
    }
    
    # Mostrar resumo
    print("\n" + "="*60)
    print("RESUMO DA CONFIGURAÇÃO")
    print("="*60)
    print(f"Tipo de banco: {config['tipo_banco'].upper()}")
    print(f"Host: {config['host']}")
    print(f"Porta: {config['port']}")
    print(f"Banco de dados: {config['dbname']}")
    print(f"Usuário: {config['username']}")
    print(f"Senha: {'*' * len(config['password'])}")
    print()
    
    # Confirmar salvamento
    confirmar = input("Salvar esta configuração? (S/N) [S]: ").strip().upper()
    if confirmar in ['', 'S', 'SIM']:
        try:
            # Salvar configuração
            with open('cnpj_config.json', 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
            
            print("✅ Configuração salva com sucesso em 'cnpj_config.json'!")
            
            # Testar conexão
            testar = input("\nTestar conexão agora? (S/N) [S]: ").strip().upper()
            if testar in ['', 'S', 'SIM']:
                print("\n🔍 Testando conexão...")
                engine = conectar_banco()
                if engine:
                    print("✅ Conexão testada com sucesso!")
                    engine.dispose()
                else:
                    print("❌ Falha no teste de conexão. Verifique as configurações.")
            
        except Exception as e:
            print(f"❌ Erro ao salvar configuração: {str(e)}")
    else:
        print("❌ Configuração não salva.")
    
    input("\nPressione Enter para continuar...")

def atualizar_status_processos_silencioso():
    """Atualiza o status de todos os processos ativos sem mostrar mensagens"""
    processos_para_remover = []
    
    for script, info in processos_ativos.items():
        try:
            # Verificar se processo ainda existe
            os.kill(info['pid'], 0)
            info['status'] = 'running'
        except OSError:
            # Processo não existe mais
            info['status'] = 'finished'
            processos_para_remover.append(script)
    
    # Remover processos finalizados
    for script in processos_para_remover:
        del processos_ativos[script]
    
    return len(processos_para_remover)

def atualizar_status_processos():
    """Atualiza o status de todos os processos ativos"""
    print("🔄 Atualizando status dos processos...")
    
    processos_removidos = atualizar_status_processos_silencioso()
    
    if processos_removidos > 0:
        print(f"🔄 {processos_removidos} processo(s) atualizado(s)")
    else:
        print("✅ Todos os processos estão rodando")

def main():
    """Função principal - Menu principal"""
    print("=" * 80)
    print("SISTEMA DE CONTROLE E GERENCIAMENTO CNPJ")
    print("=" * 80)
    
    while True:
        print("\nMENU PRINCIPAL:")
        print("1. Control - Monitoramento e verificação")
        print("2. Process - Gerenciamento de processos")
        print("3. Database - Configurar conexão")
        print("4. Sair")
        
        opcao = input("\nEscolha uma opção (1-4): ").strip()
        
        if opcao == '1':
            menu_control()
        elif opcao == '2':
            menu_process()
        elif opcao == '3':
            configurar_database()
        elif opcao == '4':
            print("\n👋 Encerrando sistema...")
            # Parar todos os processos ativos
            if processos_ativos:
                print("🛑 Parando processos ativos...")
                for script in list(processos_ativos.keys()):
                    parar_processo(script)
            break
        else:
            print("Opção inválida!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Sistema interrompido pelo usuário")
        # Parar todos os processos ativos
        if processos_ativos:
            print("🛑 Parando processos ativos...")
            for script in list(processos_ativos.keys()):
                parar_processo(script)
    except Exception as e:
        print(f"\n❌ Erro fatal: {str(e)}")
        sys.exit(1)
