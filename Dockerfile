# Dockerfile para Docker Hub: marciolemos/mba-bigdata:db
# Autor: Márcio Lemos
# Projeto: Dashboard de Indicadores Econômicos Brasileiros

FROM python:3.11.8-slim

# Metadados da imagem
LABEL maintainer="Márcio Lemos"
LABEL description="Dashboard de Indicadores Econômicos Brasileiros - MBA UNIFOR"
LABEL version="1.0.0"
LABEL org.opencontainers.image.source="https://github.com/marciolemosti/MBA_BIGDATA"

# Variáveis de ambiente
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_HEADLESS=true

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Criar usuário não-root
RUN useradd --create-home --shell /bin/bash app

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements primeiro (para cache do Docker)
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação
COPY . .

# Criar diretórios necessários
RUN mkdir -p data logs reports cache .streamlit && \
    chown -R app:app /app

# Copiar configuração do Streamlit
COPY .streamlit/config.toml .streamlit/

# Mudar para usuário não-root
USER app

# Expor porta
EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Comando padrão
CMD ["streamlit", "run", "src/dashboard/main.py"]
