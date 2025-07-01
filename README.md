# MBA Big Data - Dashboard de Indicadores Econômicos Brasileiros

**Autor:** Márcio Lemos  
**Projeto:** Dashboard de Indicadores Econômicos Brasileiros  
**MBA:** Gestão Analítica em BI e Big Data  
**Data:** 2025-06-23

## **Recursos da Aplicação**

### **APIs Reais Implementadas**

*   **IBGE API**: Coleta real de IPCA, PIB e Taxa de Desemprego
*   **Banco Central API**: Dados de Selic e Câmbio USD/BRL
*   **Tesouro Nacional API**: Informações fiscais e déficit primário
*   **Receita Federal API**: Dados de arrecadação e IOF

### **PostgreSQL com Docker**

*   **Banco de Dados**: PostgreSQL 15 com schemas organizados
*   **Docker Compose**: Ambiente completo com PostgreSQL, Adminer e Redis
*   **Pool de Conexões**: Gerenciamento eficiente de conexões
*   **Migrations**: Scripts de inicialização e estrutura do banco

### **Suporte Completo ao Windows**

*   **Requirements Otimizado**: `requirements-windows.txt` para Windows
*   **Script de Instalação**: `install_windows.bat` automatizado
*   **Docker Alternativo**: `docker-compose-dev.yml` para desenvolvimento
*   **Guia de Solução**: Documentação para resolver problemas

## **INSTALAÇÃO**

### ** Windows (Recomendado)**

#### **Opção 1: Script Automatizado**

```shell
# Execute o script de instalação
install_windows.bat
```

#### **Opção 2: Manual**

```shell
# Atualiza pip e setuptools
python.exe -m pip install --upgrade pip setuptools wheel

# Instala dependências otimizadas
pip install -r requirements-windows.txt
```

#### **Opção 3: Docker (Mais Fácil)**

```shell
# Instala Docker Desktop primeiro
# https://www.docker.com/products/docker-desktop/

# Executar ambiente completo
docker-compose -f docker-compose-dev.yml up -d

# Acessar:
# Dashboard: http://localhost:8501
# Adminer: http://localhost:8080
```

### ** Linux/Mac**

```shell
# Cria ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac

# Instala dependências
pip install -r requirements.txt

# Inicia banco de dados
docker-compose up -d
```

## **COMO USAR**

### **1. Coleta de Dados**

```shell
# Atualizar todos os indicadores
python scripts/update_data.py

# Atualizar indicador específico
python scripts/update_data.py ipca

# Forçar atualização (ignorar cache)
FORCE_UPDATE=true python scripts/update_data.py
```

### **2. Validação de Qualidade**

```shell
# Executar validação completa
python scripts/validate_data_quality.py

# Testar implementações
python scripts/test_implementations.py
```

### **3. Dashboard**

```shell
# Iniciar dashboard
streamlit run src/dashboard/main.py

# Ou com configurações específicas
streamlit run src/dashboard/main.py --server.port 8501
```

##  **ARQUITETURA DO SISTEMA**

### **Estrutura de Diretórios**

```
MBA_BIGDATA/
├── src/
│   ├── data_sources/                    # Conectores de APIs 
│   │   ├── data_manager.py             # Gerenciador principal
│   │   ├── ibge_connector.py           # Conector IBGE
│   │   ├── bcb_connector.py            # Conector Banco Central
│   │   ├── tesouro_connector.py        # Conector Tesouro Nacional
│   │   └── receita_connector.py        # Conector Receita Federal
│   ├── database/                       # Módulos de banco 
│   │   └── postgres_manager.py         # Gerenciador PostgreSQL
│   └── dashboard/                      # Interface Streamlit
├── scripts/                            # Scripts de automação
│   ├── update_data.py                  # Atualização de dados 
│   ├── validate_data_quality.py        # Validação 
│   └── test_implementations.py         # Testes 
├── database/                           # Configurações de banco 
│   └── init/                          # Scripts de inicialização
├── docker-compose.yml                 # Configuração Docker principal
├── docker-compose-dev.yml             # Docker para desenvolvimento 
├── Dockerfile                         # Container da aplicação 
├── requirements.txt                   # Dependências principais
├── requirements-windows.txt           # Dependências Windows 
├── install_windows.bat               # Instalação Windows 
├── guia_solucao_windows.md           # Guia de problemas 
└── .env.example                      # Configurações de ambiente
```
    
### **Processo ETL**
1. **Extract**: Conectores fazem requisições às APIs oficiais
2. **Transform**: Dados são validados, limpos e padronizados
3. **Load**: Persistência em PostgreSQL, JSON e Redis
4. **Monitor**: Validação de qualidade e relatórios automáticos

---

## **INDICADORES DISPONÍVEIS**

| Indicador | Fonte | Frequência | Descrição |
|-----------|-------|------------|-----------|
| **IPCA** | IBGE | Mensal | Índice Nacional de Preços ao Consumidor Amplo |
| **PIB** | IBGE | Trimestral | Produto Interno Bruto |
| **Desemprego** | IBGE | Mensal | Taxa de Desocupação (PNAD Contínua) |
| **Selic** | BCB | Diária | Taxa Básica de Juros |
| **Câmbio** | BCB | Diária | Taxa de Câmbio USD/BRL (PTAX) |
| **Déficit Primário** | Tesouro | Mensal | Resultado Primário do Governo Central |
| **Arrecadação IOF** | Receita | Mensal | Arrecadação do IOF |

---

## **PERFORMANCE E QUALIDADE**

### **Métricas Atuais**
- **Coleta Completa**: 30-60 segundos (7 indicadores)
- **Validação**: 10-20 segundos
- **Dashboard**: Carregamento < 5 segundos
- **Cache Hit Rate**: ~85% (após primeira execução)
- **Score de Qualidade**: 96.8%

### **Automação**
- **GitHub Actions**: Executa 2x por dia (8h e 18h UTC)
- **Retry Logic**: 3 tentativas com backoff exponencial
- **Monitoramento**: Logs detalhados e relatórios de qualidade

---

##  **CONFIGURAÇÕES**

### **Variáveis de Ambiente (.env)**
```bash
# Cache
REDIS_HOST=localhost
REDIS_PORT=6379
CACHE_TTL_SECONDS=3600

# APIs
IBGE_API_TIMEOUT=30
BCB_API_TIMEOUT=30
TESOURO_API_TIMEOUT=45
RECEITA_API_TIMEOUT=45

# Sistema
LOG_LEVEL=INFO
MAX_REQUESTS_PER_MINUTE=60
RETRY_ATTEMPTS=3
```

## **SOLUÇÃO DE PROBLEMAS**

### **Problemas no Windows**

1.  **Consulte**: `guia_solucao_windows.md`
2.  **Execute**: `install_windows.bat`
3.  **Use Docker**: `docker-compose -f docker-compose-dev.yml up -d`

### **Problemas de Conectividade**

```shell
# Verificar APIs
curl -I https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados

# Executar com debug
DEBUG=true python scripts/update_data.py
```

### **Problemas de Banco**

```shell
# Verificar Docker
docker-compose ps

# Reiniciar serviços
docker-compose restart postgres

# Ver logs
docker-compose logs postgres
```

## **SUPORTE**

Para dúvidas, problemas ou sugestões:

*   **Autor**: Márcio Lemos
*   **Projeto**: MBA em Gestão Analítica em BI e Big Data
*   **GitHub**: [https://github.com/marciolemosti/MBA_BIGDATA]()

## **LICENÇA**

Este projeto é desenvolvido para fins acadêmicos como parte do MBA em Gestão Analítica em BI e Big Data.

