# Guia de Solução - Erros de Instalação Windows

## 🚨 **PROBLEMA IDENTIFICADO**

Você está enfrentando um erro comum no Windows com Python 3.12.1 e pyenv-win relacionado ao setuptools. O erro principal é:

```
pip._vendor.pyproject_hooks._impl.BackendUnavailable: Cannot import 'setuptools.build_meta'
```

## 🔧 **SOLUÇÕES PASSO A PASSO**

### **SOLUÇÃO 1: Atualizar pip e setuptools (RECOMENDADA)**

```bash
# 1. Atualizar pip primeiro
python.exe -m pip install --upgrade pip

# 2. Atualizar setuptools
python.exe -m pip install --upgrade setuptools

# 3. Instalar wheel (necessário para builds)
python.exe -m pip install --upgrade wheel

# 4. Tentar instalar requirements novamente
pip install -r requirements.txt
```

### **SOLUÇÃO 2: Instalação com Cache Limpo**

```bash
# Limpar cache do pip
pip cache purge

# Instalar com cache limpo
pip install --no-cache-dir -r requirements.txt
```

### **SOLUÇÃO 3: Instalação Individual de Pacotes Problemáticos**

```bash
# Instalar pacotes um por vez para identificar problemas
pip install streamlit
pip install pandas
pip install numpy
pip install psycopg2-binary
pip install plotly
pip install prophet
pip install requests
pip install sqlalchemy
pip install redis
pip install python-dotenv
```

### **SOLUÇÃO 4: Usar Conda (ALTERNATIVA RECOMENDADA)**

Se os problemas persistirem, recomendo usar Conda que é mais estável no Windows:

```bash
# 1. Baixar e instalar Miniconda
# https://docs.conda.io/en/latest/miniconda.html

# 2. Criar ambiente conda
conda create -n mba_bigdata python=3.11

# 3. Ativar ambiente
conda activate mba_bigdata

# 4. Instalar pacotes via conda
conda install pandas numpy streamlit plotly requests sqlalchemy
conda install -c conda-forge psycopg2 redis-py python-dotenv

# 5. Instalar prophet via pip (se necessário)
pip install prophet
```

## 📋 **REQUIREMENTS.TXT OTIMIZADO PARA WINDOWS**

Crie um novo arquivo `requirements-windows.txt`:

```txt
# Framework Principal
streamlit>=1.28.0

# Manipulação de Dados
pandas>=2.0.0
numpy>=1.24.0

# Banco de Dados
psycopg2-binary>=2.9.0
sqlalchemy>=2.0.0

# Visualização
plotly>=5.15.0

# APIs e HTTP
requests>=2.31.0

# Cache
redis>=4.5.0

# Configuração
python-dotenv>=1.0.0

# Previsões (instalar separadamente se der erro)
# prophet>=1.1.4
```

Instalar com:
```bash
pip install -r requirements-windows.txt
```

## 🐳 **SOLUÇÃO ALTERNATIVA: USAR DOCKER (MAIS FÁCIL)**

Se os problemas persistirem, use Docker que elimina problemas de ambiente:

### **1. Instalar Docker Desktop**
- Baixar: https://www.docker.com/products/docker-desktop/
- Instalar e reiniciar o computador

### **2. Usar Docker Compose**
```bash
# No diretório do projeto
docker-compose up -d

# Acessar container
docker-compose exec app bash

# Dentro do container, executar comandos normalmente
python scripts/update_data.py
streamlit run src/dashboard/main.py
```

### **3. Dockerfile para o Projeto**

Crie um `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

# Expor porta
EXPOSE 8501

# Comando padrão
CMD ["streamlit", "run", "src/dashboard/main.py", "--server.headless", "true", "--server.address", "0.0.0.0"]
```

## 🔍 **DIAGNÓSTICO ADICIONAL**

### **Verificar Versões**
```bash
python --version
pip --version
pip list | findstr setuptools
```

### **Verificar Ambiente Virtual**
```bash
# Verificar se está em ambiente virtual
echo $VIRTUAL_ENV

# Se não estiver, criar um
python -m venv venv
venv\Scripts\activate
```

## ⚡ **SOLUÇÃO RÁPIDA PARA TESTAR**

Se quiser testar rapidamente sem instalar tudo:

### **1. Versão Mínima**
```bash
# Instalar apenas o essencial
pip install streamlit pandas plotly requests

# Executar dashboard básico
streamlit run src/dashboard/main.py
```

### **2. Usar Dados Locais**
O projeto já tem dados em cache na pasta `data/`, então pode funcionar mesmo sem conectar às APIs.

## 🆘 **SE NADA FUNCIONAR**

### **Opção 1: Python 3.11**
```bash
# Instalar Python 3.11 (mais estável)
pyenv install 3.11.7
pyenv local 3.11.7
```

### **Opção 2: Usar Google Colab**
```python
# No Google Colab
!git clone https://github.com/marciolemosti/MBA_BIGDATA.git
%cd MBA_BIGDATA
!pip install -r requirements.txt
!streamlit run src/dashboard/main.py --server.headless true
```

### **Opção 3: Usar GitHub Codespaces**
- Abrir repositório no GitHub
- Clicar em "Code" > "Codespaces" > "Create codespace"
- Ambiente já configurado automaticamente

## 📞 **PRÓXIMOS PASSOS**

1. **Tente a Solução 1 primeiro** (atualizar pip/setuptools)
2. **Se não funcionar, use Conda** (Solução 4)
3. **Como último recurso, use Docker** (mais fácil)

Me informe qual solução funcionou ou se encontrar outros erros!

