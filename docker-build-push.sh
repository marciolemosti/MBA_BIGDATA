#!/bin/bash
# Script para build e push no Docker Hub
# Autor: Márcio Lemos

set -e

# Configurações
DOCKER_USERNAME="marciolemos"
IMAGE_NAME="mba-bigdata"
TAG="db"
FULL_IMAGE_NAME="${DOCKER_USERNAME}/${IMAGE_NAME}:${TAG}"

echo "=== Build e Push para Docker Hub ==="
echo "Imagem: ${FULL_IMAGE_NAME}"
echo

# Verificar se Docker está rodando
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker não está rodando. Inicie o Docker Desktop primeiro."
    exit 1
fi

# Build da imagem
echo "🔨 Fazendo build da imagem..."
docker build -t ${FULL_IMAGE_NAME} .

if [ $? -eq 0 ]; then
    echo "Build concluído com sucesso!"
else
    echo "Erro no build da imagem"
    exit 1
fi

# Verificar se está logado no Docker Hub
echo "Verificando login no Docker Hub..."
if ! docker info | grep -q "Username: ${DOCKER_USERNAME}"; then
    echo "Fazendo login no Docker Hub..."
    docker login
fi

# Push da imagem
echo "📤 Fazendo push para Docker Hub..."
docker push ${FULL_IMAGE_NAME}

if [ $? -eq 0 ]; then
    echo "Push concluído com sucesso!"
    echo "Imagem disponível em: https://hub.docker.com/r/${DOCKER_USERNAME}/${IMAGE_NAME}"
    echo
    echo "Para usar a imagem:"
    echo "docker pull ${FULL_IMAGE_NAME}"
    echo "docker run -p 8501:8501 ${FULL_IMAGE_NAME}"
else
    echo "Erro no push da imagem"
    exit 1
fi

# Também criar tag 'latest'
echo "Criando tag 'latest'..."
docker tag ${FULL_IMAGE_NAME} ${DOCKER_USERNAME}/${IMAGE_NAME}:latest
docker push ${DOCKER_USERNAME}/${IMAGE_NAME}:latest

echo "Tag 'latest' criada e enviada!"
echo
echo "=== Resumo ==="
echo "Imagem: ${FULL_IMAGE_NAME}"
echo "Latest: ${DOCKER_USERNAME}/${IMAGE_NAME}:latest"
echo "Disponível no Docker Hub"
