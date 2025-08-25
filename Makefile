# Makefile para Sistema CNPJ
# ============================
# 
# Este Makefile automatiza a execução dos scripts do sistema CNPJ
# 
# Comandos disponíveis:
#   make help        - Mostra esta ajuda
#   make download    - Executa download dos dados CNPJ
#   make unzip       - Descompacta os arquivos baixados
#   make tables      - Cria as tabelas no banco de dados
#   make insert      - Insere os dados nas tabelas
#   make all         - Executa todo o pipeline (download -> unzip -> tables -> insert)
#   make clean       - Remove arquivos temporários e logs
#   make status      - Mostra status dos arquivos e banco

# Configurações
PYTHON = python3
DATA_DIR = dados-publicos
LOG_DIR = logs

# Cores para output
RED = \033[0;31m
GREEN = \033[0;32m
YELLOW = \033[1;33m
BLUE = \033[0;34m
NC = \033[0m # No Color

# Arquivos de script
DOWNLOAD_SCRIPT = 00_dados_cnpj_baixa.py
UNZIP_SCRIPT = 01_descompactar_arquivos.py
TABLES_SCRIPT = 02_criar_tabelas.py
INSERT_SCRIPT = 03_inserir_dados.py

# Arquivos de log
DOWNLOAD_LOG = $(LOG_DIR)/download_$(shell date +%Y%m%d_%H%M%S).log
UNZIP_LOG = $(LOG_DIR)/unzip_$(shell date +%Y%m%d_%H%M%S).log
TABLES_LOG = $(LOG_DIR)/tables_$(shell date +%Y%m%d_%H%M%S).log
INSERT_LOG = $(LOG_DIR)/insert_$(shell date +%Y%m%d_%H%M%S).log

.PHONY: help download unzip tables insert insert-pm2 insert-stop insert-restart insert-logs insert-status insert-monitor insert-clean all clean status check-deps

# Target padrão
help:
	@echo "$(BLUE)Sistema CNPJ - Makefile$(NC)"
	@echo "=============================="
	@echo ""
	@echo "$(GREEN)Comandos disponíveis:$(NC)"
	@echo "  $(YELLOW)make help$(NC)        - Mostra esta ajuda"
	@echo "  $(YELLOW)make download$(NC)    - Executa download dos dados CNPJ"
	@echo "  $(YELLOW)make unzip$(NC)       - Descompacta os arquivos baixados"
	@echo "  $(YELLOW)make tables$(NC)      - Cria as tabelas no banco de dados"
	@echo "  $(YELLOW)make insert$(NC)      - Insere os dados nas tabelas (execução direta)"
	@echo "  $(YELLOW)make all$(NC)         - Executa todo o pipeline"
	@echo "  $(YELLOW)make clean$(NC)       - Remove arquivos temporários e logs"
	@echo "  $(YELLOW)make status$(NC)      - Mostra status dos arquivos e banco"
	@echo "  $(YELLOW)make check-deps$(NC)  - Verifica dependências"
	@echo ""
	@echo "$(GREEN)Comandos PM2 (para uso manual):$(NC)"
	@echo "  $(YELLOW)make insert-pm2$(NC)     - Insere dados usando PM2 (execução em background)"
	@echo "  $(YELLOW)make insert-stop$(NC)    - Para o processo PM2 de inserção"
	@echo "  $(YELLOW)make insert-restart$(NC) - Reinicia o processo PM2 de inserção"
	@echo "  $(YELLOW)make insert-logs$(NC)    - Mostra logs do PM2"
	@echo "  $(YELLOW)make insert-status$(NC)  - Mostra status do processo PM2"
	@echo "  $(YELLOW)make insert-monitor$(NC) - Dashboard de monitoramento PM2"
	@echo "  $(YELLOW)make insert-clean$(NC)   - Remove processo PM2"
	@echo ""
	@echo "$(GREEN)Pipeline completo:$(NC)"
	@echo "  download → unzip → tables → insert"
	@echo ""

# Verifica dependências
check-deps:
	@echo "$(BLUE)Verificando dependências...$(NC)"
	@command -v $(PYTHON) >/dev/null 2>&1 || { echo "$(RED)Python3 não encontrado!$(NC)"; exit 1; }
	@test -f $(DOWNLOAD_SCRIPT) || { echo "$(RED)$(DOWNLOAD_SCRIPT) não encontrado!$(NC)"; exit 1; }
	@test -f $(UNZIP_SCRIPT) || { echo "$(RED)$(UNZIP_SCRIPT) não encontrado!$(NC)"; exit 1; }
	@test -f $(TABLES_SCRIPT) || { echo "$(RED)$(TABLES_SCRIPT) não encontrado!$(NC)"; exit 1; }
	@test -f $(INSERT_SCRIPT) || { echo "$(RED)$(INSERT_SCRIPT) não encontrado!$(NC)"; exit 1; }
	@echo "$(GREEN)✓ Todas as dependências encontradas$(NC)"

# Cria diretório de logs se não existir
$(LOG_DIR):
	@mkdir -p $(LOG_DIR)

# Download dos dados CNPJ
download: check-deps $(LOG_DIR)
	@echo "$(BLUE)🔽 Iniciando download dos dados CNPJ...$(NC)"
	@echo "Log: $(DOWNLOAD_LOG)"
	@$(PYTHON) $(DOWNLOAD_SCRIPT) 2>&1 | tee $(DOWNLOAD_LOG)
	@if [ $$? -eq 0 ]; then \
		echo "$(GREEN)✓ Download concluído com sucesso!$(NC)"; \
	else \
		echo "$(RED)✗ Erro no download. Verifique o log: $(DOWNLOAD_LOG)$(NC)"; \
		exit 1; \
	fi

# Descompactação dos arquivos
unzip: check-deps $(LOG_DIR)
	@echo "$(BLUE)📦 Iniciando descompactação dos arquivos...$(NC)"
	@echo "Log: $(UNZIP_LOG)"
	@$(PYTHON) $(UNZIP_SCRIPT) 2>&1 | tee $(UNZIP_LOG)
	@if [ $$? -eq 0 ]; then \
		echo "$(GREEN)✓ Descompactação concluída com sucesso!$(NC)"; \
	else \
		echo "$(RED)✗ Erro na descompactação. Verifique o log: $(UNZIP_LOG)$(NC)"; \
		exit 1; \
	fi

# Criação das tabelas
tables: check-deps $(LOG_DIR)
	@echo "$(BLUE)🗃️  Iniciando criação das tabelas...$(NC)"
	@echo "Log: $(TABLES_LOG)"
	@$(PYTHON) $(TABLES_SCRIPT) 2>&1 | tee $(TABLES_LOG)
	@if [ $$? -eq 0 ]; then \
		echo "$(GREEN)✓ Tabelas criadas com sucesso!$(NC)"; \
	else \
		echo "$(RED)✗ Erro na criação das tabelas. Verifique o log: $(TABLES_LOG)$(NC)"; \
		exit 1; \
	fi

# Inserção dos dados (execução direta)
insert: check-deps $(LOG_DIR)
	@echo "$(BLUE)📊 Iniciando inserção dos dados...$(NC)"
	@echo "Log: $(INSERT_LOG)"
	@$(PYTHON) $(INSERT_SCRIPT) 2>&1 | tee $(INSERT_LOG)
	@if [ $$? -eq 0 ]; then \
		echo "$(GREEN)✓ Inserção concluída com sucesso!$(NC)"; \
	else \
		echo "$(RED)✗ Erro na inserção. Verifique o log: $(INSERT_LOG)$(NC)"; \
		exit 1; \
	fi

# Inserção dos dados com PM2 (execução em background)
insert-pm2: check-deps $(LOG_DIR)
	@echo "$(BLUE)📊 Iniciando inserção dos dados com PM2...$(NC)"
	@echo "Log: $(INSERT_LOG)"
	@pm2 start $(INSERT_SCRIPT) --name "cnpj-insert" --interpreter $(PYTHON) --log $(INSERT_LOG) --time
	@echo "$(GREEN)✓ Processo iniciado com PM2!$(NC)"
	@echo "$(BLUE)Comandos úteis:$(NC)"
	@echo "  $(YELLOW)make insert-logs$(NC)     - Ver logs em tempo real"
	@echo "  $(YELLOW)make insert-status$(NC)   - Ver status do processo"
	@echo "  $(YELLOW)make insert-stop$(NC)     - Parar o processo"
	@echo "  $(YELLOW)make insert-restart$(NC)  - Reiniciar o processo"

# Controle do processo PM2
insert-stop:
	@echo "$(YELLOW)🛑 Parando processo de inserção...$(NC)"
	@pm2 stop cnpj-insert 2>/dev/null || echo "$(RED)Processo não encontrado$(NC)"
	@echo "$(GREEN)✓ Processo parado$(NC)"

insert-restart:
	@echo "$(YELLOW)🔄 Reiniciando processo de inserção...$(NC)"
	@pm2 restart cnpj-insert 2>/dev/null || echo "$(RED)Processo não encontrado$(NC)"
	@echo "$(GREEN)✓ Processo reiniciado$(NC)"

insert-logs:
	@echo "$(BLUE)📋 Mostrando logs do processo de inserção...$(NC)"
	@pm2 logs cnpj-insert --lines 50

insert-status:
	@echo "$(BLUE)📊 Status do processo de inserção...$(NC)"
	@pm2 status cnpj-insert

insert-monitor:
	@echo "$(BLUE)📊 Monitorando processo de inserção...$(NC)"
	@echo "Pressione Ctrl+C para sair"
	@pm2 monit

# Limpeza PM2
insert-clean:
	@echo "$(YELLOW)🧹 Limpando processo PM2...$(NC)"
	@pm2 delete cnpj-insert 2>/dev/null || echo "$(RED)Processo não encontrado$(NC)"
	@echo "$(GREEN)✓ Processo removido$(NC)"

# Pipeline completo
all: download unzip tables insert
	@echo ""
	@echo "$(GREEN)🎉 Pipeline completo executado com sucesso!$(NC)"
	@echo "$(BLUE)Logs salvos em: $(LOG_DIR)/$(NC)"

# Status do sistema
status:
	@echo "$(BLUE)📊 Status do Sistema CNPJ$(NC)"
	@echo "=========================="
	@echo ""
	@echo "$(YELLOW)Arquivos de script:$(NC)"
	@test -f $(DOWNLOAD_SCRIPT) && echo "  ✓ $(DOWNLOAD_SCRIPT)" || echo "  ✗ $(DOWNLOAD_SCRIPT)"
	@test -f $(UNZIP_SCRIPT) && echo "  ✓ $(UNZIP_SCRIPT)" || echo "  ✗ $(UNZIP_SCRIPT)"
	@test -f $(TABLES_SCRIPT) && echo "  ✓ $(TABLES_SCRIPT)" || echo "  ✗ $(TABLES_SCRIPT)"
	@test -f $(INSERT_SCRIPT) && echo "  ✓ $(INSERT_SCRIPT)" || echo "  ✗ $(INSERT_SCRIPT)"
	@echo ""
	@echo "$(YELLOW)Configuração:$(NC)"
	@test -f cnpj_config.json && echo "  ✓ cnpj_config.json" || echo "  ✗ cnpj_config.json"
	@echo ""
	@echo "$(YELLOW)Dados:$(NC)"
	@if [ -d "$(DATA_DIR)" ]; then \
		echo "  ✓ Diretório $(DATA_DIR) existe"; \
		echo "  📁 Arquivos: $$(find $(DATA_DIR) -name "*.zip" 2>/dev/null | wc -l) ZIP, $$(find $(DATA_DIR) -name "*.csv" -o -name "*.CSV" -o -name "*ESTABELE*" -o -name "*EMPRECSV*" 2>/dev/null | wc -l) CSV"; \
	else \
		echo "  ✗ Diretório $(DATA_DIR) não existe"; \
	fi
	@echo ""
	@echo "$(YELLOW)Logs:$(NC)"
	@if [ -d "$(LOG_DIR)" ]; then \
		echo "  ✓ Diretório $(LOG_DIR) existe"; \
		echo "  📄 Arquivos de log: $$(find $(LOG_DIR) -name "*.log" 2>/dev/null | wc -l)"; \
	else \
		echo "  ✗ Diretório $(LOG_DIR) não existe"; \
	fi

# Limpeza
clean:
	@echo "$(YELLOW)🧹 Limpando arquivos temporários...$(NC)"
	@rm -rf $(LOG_DIR)/*.log 2>/dev/null || true
	@rm -rf *.log 2>/dev/null || true
	@rm -rf __pycache__ 2>/dev/null || true
	@rm -rf .pytest_cache 2>/dev/null || true
	@echo "$(GREEN)✓ Limpeza concluída$(NC)"

# Limpeza completa (incluindo dados)
clean-all: clean
	@echo "$(RED)⚠️  Removendo TODOS os dados baixados...$(NC)"
	@read -p "Tem certeza? Esta ação não pode ser desfeita [y/N]: " confirm && [ "$$confirm" = "y" ]
	@rm -rf $(DATA_DIR) 2>/dev/null || true
	@echo "$(GREEN)✓ Limpeza completa concluída$(NC)"

# Monitoramento em tempo real
control:
	@echo "$(BLUE)📊 Monitorando sistema...$(NC)"
	@echo "Pressione Ctrl+C para parar"
	@$(PYTHON) control.py

# Informações do sistema
info:
	@echo "$(BLUE)ℹ️  Informações do Sistema$(NC)"
	@echo "=========================="
	@echo "Python: $$($(PYTHON) --version)"
	@echo "Make: $$(make --version | head -1)"
	@echo "Sistema: $$(uname -a)"
	@echo "Espaço em disco: $$(df -h . | tail -1 | awk '{print $$4}') disponível"
	@echo "Memória: $$(free -h | grep Mem | awk '{print $$7}') disponível"
