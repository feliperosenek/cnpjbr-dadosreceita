# cnpjbr-dadosreceita

Este projeto automatiza o download, descompactação, preparação e inserção dos dados públicos do CNPJ (Cadastro Nacional da Pessoa Jurídica) da Receita Federal em um banco de dados PostgreSQL. Ele também oferece ferramentas para monitoramento, limpeza e controle do processo de ETL (Extract, Transform, Load).

## Funcionalidades

- **Download automatizado** dos arquivos públicos do CNPJ diretamente da Receita Federal.
- **Descompactação** dos arquivos ZIP baixados.
- **Criação automática das tabelas** no banco de dados PostgreSQL.
- **Inserção eficiente dos dados** utilizando Pandas e Dask para grandes volumes.
- **Monitoramento e controle** do status do banco e dos processos via script interativo.
- **Limpeza completa** das tabelas do banco de dados.
- **Logs detalhados** de todas as etapas do processo.

## Estrutura dos Arquivos

- [`00_dados_cnpj_baixa.py`](00_dados_cnpj_baixa.py): Script para baixar os arquivos de dados públicos do CNPJ.
- [`01_descompactar_arquivos.py`](01_descompactar_arquivos.py): Descompacta os arquivos ZIP baixados.
- [`02_criar_tabelas.py`](02_criar_tabelas.py): Cria todas as tabelas necessárias no banco de dados.
- [`03_inserir_dados.py`](03_inserir_dados.py): Insere os dados nas tabelas do banco.
- [`limpar_banco.py`](limpar_banco.py): Limpa todas as tabelas do banco de dados.
- [`control.py`](control.py): Script interativo para monitoramento, controle de processos e configuração do banco.
- [`dados_cnpj_postgres.py`](dados_cnpj_postgres.py): Script alternativo para manipulação dos dados no PostgreSQL.
- [`cnpj_config.json`](cnpj_config.json): Arquivo de configuração do banco de dados (gerado pelos scripts).
- [`Makefile`](Makefile): Automatiza a execução dos scripts.
- [`requirements.txt`](requirements.txt): Lista de dependências Python.
- Diretórios `dados-publicos-zip/` e `dados-publicos/`: Armazenam os arquivos ZIP e os arquivos descompactados, respectivamente.
- Diretório `logs/`: Armazena os logs de execução dos scripts.

## Comandos do Makefile

O projeto inclui um [Makefile](Makefile) para facilitar a execução das etapas principais. Os comandos disponíveis são:

| Comando             | Descrição                                                        |
|---------------------|------------------------------------------------------------------|
| `make help`         | Mostra a ajuda e os comandos disponíveis                         |
| `make download`     | Baixa os arquivos públicos do CNPJ                               |
| `make unzip`        | Descompacta os arquivos baixados                                 |
| `make tables`       | Cria as tabelas no banco de dados                                |
| `make insert`       | Insere os dados nas tabelas                                      |
| `make all`          | Executa todo o pipeline: download → unzip → tables → insert      |
| `make clean`        | Remove arquivos temporários e logs                               |
| `make status`       | Mostra o status dos arquivos e do banco de dados                 |
| `make check-deps`   | Verifica se todas as dependências estão instaladas               |
| `make control`      | Inicia o sistema de monitoramento interativo                     |
| `make info`         | Mostra informações do sistema e ambiente                         |

## Requisitos

- Python 3.7+
- PostgreSQL (ou MySQL, com adaptações)
- Dependências listadas em [`requirements.txt`](requirements.txt)

Instale as dependências com:

```sh
pip install -r requirements.txt
```


