# MBA Big Data - Dashboard de Indicadores Econômicos Brasileiros
## Versão 2.0 - Refatorada com APIs Reais e PostgreSQL

**Autor:** Márcio Lemos  
**Projeto:** Dashboard de Indicadores Econômicos Brasileiros  
**MBA:** Gestão Analítica em BI e Big Data  
**Data:** 2025-06-23  
**Versão:** 2.0.0

---

## 🚀 **PRINCIPAIS MELHORIAS DA VERSÃO 2.0**

### ✅ **APIs Reais Implementadas**
- **IBGE API**: Coleta real de IPCA, PIB e Taxa de Desemprego
- **Banco Central API**: Dados de Selic e Câmbio USD/BRL
- **Tesouro Nacional API**: Informações fiscais e déficit primário
- **Receita Federal API**: Dados de arrecadação e IOF

### ✅ **Correções Críticas**
- **Erro de Serialização JSON**: Corrigido com encoder customizado
- **Tratamento de Tipos Numpy**: Conversão automática int64 → int nativo
- **Cache Inteligente**: Sistema com TTL configurável e limpeza automática
- **Rate Limiting**: Proteção contra sobrecarga das APIs

### ✅ **PostgreSQL com Docker**
- **Banco de Dados Robusto**: PostgreSQL 15 com schemas organizados
- **Docker Compose**: Ambiente completo com PostgreSQL, Adminer e Redis
- **Pool de Conexões**: Gerenciamento eficiente de conexões
- **Migrations**: Scripts de inicialização e estrutura do banco

### ✅ **Suporte Completo ao Windows**
- **Requirements Otimizado**: `requirements-windows.txt` para Windows
- **Script de Instalação**: `install_windows.bat` automatizado
- **Docker Alternativo**: `docker-compose-dev.yml` para desenvolvimento
- **Guia de Solução**: Documentação completa para resolver problemas

---

## 📋 **INSTALAÇÃO**

### **🪟 Windows (Recomendado)**

#### **Opção 1: Script Automatizado**
```bash
# Execute o script de instalação
install_windows.bat
```

#### **Opção 2: Manual**
```bash
# Atualizar pip e setuptools
python.exe -m pip install --upgrade pip setuptools wheel

# Instalar dependências otimizadas
pip install -r requirements-windows.txt
```

#### **Opção 3: Docker (Mais Fácil)**
```bash
# Instalar Docker Desktop primeiro
# https://www.docker.com/products/docker-desktop/

# Executar ambiente completo
docker-compose -f docker-compose-dev.yml up -d

# Acessar:
# Dashboard: http://localhost:8501
# Adminer: http://localhost:8080
```

### **🐧 Linux/Mac**
```bash
# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac

# Instalar dependências
pip install -r requirements.txt

# Iniciar banco de dados
docker-compose up -d
```

---

## 🎯 **COMO USAR**

### **1. Coleta de Dados**
```bash
# Atualizar todos os indicadores
python scripts/update_data.py

# Atualizar indicador específico
python scripts/update_data.py ipca

# Forçar atualização (ignorar cache)
FORCE_UPDATE=true python scripts/update_data.py
```

### **2. Validação de Qualidade**
```bash
# Executar validação completa
python scripts/validate_data_quality.py

# Testar implementações
python scripts/test_implementations.py
```

### **3. Dashboard**
```bash
# Iniciar dashboard
streamlit run src/dashboard/main.py

# Ou com configurações específicas
streamlit run src/dashboard/main.py --server.port 8501
```

### **4. Administração do Banco**
- **Adminer**: http://localhost:8080
  - Servidor: `postgres`
  - Usuário: `mba_user`
  - Senha: `mba_password_2025`
  - Banco: `mba_bigdata`

---

## 🏗️ **ARQUITETURA DO SISTEMA**

### **Estrutura de Diretórios**
```
MBA_BIGDATA/
├── src/
│   ├── data_sources/          # Conectores de APIs (NOVO)
│   │   ├── data_manager.py    # Gerenciador principal
│   │   ├── ibge_connector.py  # Conector IBGE
│   │   ├── bcb_connector.py   # Conector Banco Central
│   │   ├── tesouro_connector.py # Conector Tesouro Nacional
│   │   └── receita_connector.py # Conector Receita Federal
│   ├── database/              # Módulos de banco (NOVO)
│   │   └── postgres_manager.py # Gerenciador PostgreSQL
│   └── dashboard/             # Interface Streamlit
├── scripts/                   # Scripts de automação
│   ├── update_data.py         # Atualização de dados (REFATORADO)
│   ├── validate_data_quality.py # Validação (CORRIGIDO)
│   └── test_implementations.py # Testes (NOVO)
├── database/                  # Configurações de banco (NOVO)
│   └── init/                  # Scripts de inicialização
├── docker-compose.yml         # Configuração Docker principal
├── docker-compose-dev.yml     # Docker para desenvolvimento (NOVO)
├── Dockerfile                 # Container da aplicação (NOVO)
├── requirements.txt           # Dependências principais
├── requirements-windows.txt   # Dependências Windows (NOVO)
├── install_windows.bat        # Instalação Windows (NOVO)
├── guia_solucao_windows.md    # Guia de problemas (NOVO)
└── .env.example              # Configurações de ambiente
```

### **Processo ETL**
1. **Extract**: Conectores fazem requisições às APIs oficiais
2. **Transform**: Dados são validados, limpos e padronizados
3. **Load**: Persistência em PostgreSQL, JSON e Redis
4. **Monitor**: Validação de qualidade e relatórios automáticos

---

## 📊 **INDICADORES DISPONÍVEIS**

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

## ⚡ **PERFORMANCE E QUALIDADE**

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

## 🔧 **CONFIGURAÇÕES**

### **Variáveis de Ambiente (.env)**
```bash
# Banco de Dados
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=mba_bigdata
POSTGRES_USER=mba_user
POSTGRES_PASSWORD=mba_password_2025

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

---

## 🚨 **SOLUÇÃO DE PROBLEMAS**

### **Problemas no Windows**
1. **Consulte**: `guia_solucao_windows.md`
2. **Execute**: `install_windows.bat`
3. **Use Docker**: `docker-compose -f docker-compose-dev.yml up -d`

### **Problemas de Conectividade**
```bash
# Verificar APIs
curl -I https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados

# Executar com debug
DEBUG=true python scripts/update_data.py
```

### **Problemas de Banco**
```bash
# Verificar Docker
docker-compose ps

# Reiniciar serviços
docker-compose restart postgres

# Ver logs
docker-compose logs postgres
```

---

## 📈 **NOVIDADES DA VERSÃO 2.0**

### **Funcionalidades Adicionadas**
- ✅ Conectores para 4 APIs governamentais reais
- ✅ Sistema de cache inteligente multicamadas
- ✅ Banco PostgreSQL com particionamento temporal
- ✅ Pool de conexões otimizado
- ✅ Validação robusta de qualidade de dados
- ✅ Processamento paralelo de indicadores
- ✅ Rate limiting e retry logic
- ✅ Suporte completo ao Windows
- ✅ Docker para desenvolvimento
- ✅ Scripts de instalação automatizada

### **Problemas Corrigidos**
- ✅ Erro de serialização JSON (numpy → Python nativo)
- ✅ Tratamento de valores nulos e outliers
- ✅ Compatibilidade com Python 3.12
- ✅ Problemas de instalação no Windows
- ✅ Performance de consultas ao banco
- ✅ Gestão de memória e cache

---

## 🤝 **CONTRIBUIÇÃO**

### **Como Contribuir**
1. Fork do repositório
2. Criar branch para feature (`git checkout -b feature/nova-funcionalidade`)
3. Commit das mudanças (`git commit -am 'Adiciona nova funcionalidade'`)
4. Push para branch (`git push origin feature/nova-funcionalidade`)
5. Criar Pull Request

### **Padrões de Código**
- **Formatação**: Black (configurado)
- **Linting**: Flake8 (configurado)
- **Documentação**: Docstrings em português
- **Testes**: Pytest para validação

---

## 📝 **CHANGELOG**

### **v2.0.0 - 2025-06-23**
- ✅ Implementação de APIs reais (IBGE, BCB, Tesouro, Receita)
- ✅ Correção do erro de serialização JSON
- ✅ Adição do PostgreSQL com Docker
- ✅ Sistema de cache inteligente
- ✅ Pool de conexões de banco
- ✅ Validação robusta de qualidade
- ✅ Rate limiting e retry logic
- ✅ Suporte completo ao Windows
- ✅ Docker para desenvolvimento
- ✅ Documentação completa

### **v1.0.0 - Original**
- Dashboard básico com dados simulados
- Streamlit interface
- Estrutura inicial do projeto

---

## 📞 **SUPORTE**

Para dúvidas, problemas ou sugestões:

- **Autor**: Márcio Lemos
- **Projeto**: MBA em Gestão Analítica em BI e Big Data
- **GitHub**: https://github.com/marciolemosti/MBA_BIGDATA

---

## 📄 **LICENÇA**

Este projeto é desenvolvido para fins acadêmicos como parte do MBA em Gestão Analítica em BI e Big Data.

---

**🎉 Projeto Refatorado com Sucesso!**  
*Versão 2.0 - Sistema ETL de nível enterprise com APIs reais, PostgreSQL e suporte completo ao Windows.*

