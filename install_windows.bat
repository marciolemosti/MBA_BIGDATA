@echo off
REM Script de Instalação Automatizada - MBA_BIGDATA Windows
REM Autor: Márcio Lemos
REM Data: 2025-06-23

echo ========================================
echo  MBA_BIGDATA - Instalacao Automatizada
echo ========================================
echo.

REM Verificar se Python está instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo ERRO: Python nao encontrado!
    echo Por favor, instale Python 3.11 ou superior
    echo Download: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo ✓ Python encontrado
python --version

REM Verificar se pip está disponível
pip --version >nul 2>&1
if errorlevel 1 (
    echo ERRO: pip nao encontrado!
    echo Reinstale Python com pip incluido
    pause
    exit /b 1
)

echo ✓ pip encontrado
pip --version

echo.
echo === ETAPA 1: Atualizando pip e setuptools ===
python.exe -m pip install --upgrade pip
if errorlevel 1 (
    echo ERRO: Falha ao atualizar pip
    pause
    exit /b 1
)

pip install --upgrade setuptools wheel
if errorlevel 1 (
    echo ERRO: Falha ao atualizar setuptools
    pause
    exit /b 1
)

echo ✓ pip e setuptools atualizados

echo.
echo === ETAPA 2: Criando ambiente virtual ===
if exist venv (
    echo Ambiente virtual já existe, removendo...
    rmdir /s /q venv
)

python -m venv venv
if errorlevel 1 (
    echo ERRO: Falha ao criar ambiente virtual
    pause
    exit /b 1
)

echo ✓ Ambiente virtual criado

echo.
echo === ETAPA 3: Ativando ambiente virtual ===
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERRO: Falha ao ativar ambiente virtual
    pause
    exit /b 1
)

echo ✓ Ambiente virtual ativado

echo.
echo === ETAPA 4: Instalando dependências ===
echo Instalando pacotes essenciais primeiro...

pip install --no-cache-dir streamlit pandas numpy plotly requests
if errorlevel 1 (
    echo ERRO: Falha ao instalar pacotes essenciais
    pause
    exit /b 1
)

echo ✓ Pacotes essenciais instalados

echo Instalando banco de dados...
pip install --no-cache-dir psycopg2-binary sqlalchemy
if errorlevel 1 (
    echo AVISO: Falha ao instalar psycopg2, tentando alternativa...
    pip install --no-cache-dir psycopg2
)

echo Instalando utilitários...
pip install --no-cache-dir redis python-dotenv pyyaml
if errorlevel 1 (
    echo AVISO: Alguns utilitários falharam, continuando...
)

echo.
echo === ETAPA 5: Testando instalação ===
python -c "import streamlit, pandas, numpy, plotly; print('✓ Pacotes principais OK')"
if errorlevel 1 (
    echo ERRO: Falha no teste de importação
    pause
    exit /b 1
)

echo ✓ Teste de importação bem-sucedido

echo.
echo === ETAPA 6: Verificando estrutura do projeto ===
if not exist "src\dashboard\main.py" (
    echo ERRO: Arquivo principal do dashboard não encontrado
    echo Certifique-se de estar no diretório correto do projeto
    pause
    exit /b 1
)

echo ✓ Estrutura do projeto verificada

echo.
echo ========================================
echo  INSTALACAO CONCLUIDA COM SUCESSO!
echo ========================================
echo.
echo Para usar o projeto:
echo 1. Ative o ambiente virtual: venv\Scripts\activate
echo 2. Execute o dashboard: streamlit run src\dashboard\main.py
echo 3. Acesse: http://localhost:8501
echo.
echo Para atualizar dados: python scripts\update_data.py
echo Para validar qualidade: python scripts\validate_data_quality.py
echo.
echo Pressione qualquer tecla para continuar...
pause >nul

