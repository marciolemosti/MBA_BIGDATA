#!/usr/bin/env python3
"""
Script de Limpeza de Cache
Responsável pela manutenção e otimização do sistema de cache.

Autor: Márcio Lemos
Projeto: MBA em Gestão Analítica em BI e Big Data
"""

import sys
from datetime import datetime
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.logger import get_logger
from data_services.cache_service import cache_service

logger = get_logger("cache_cleanup")


def main():
    """Função principal de limpeza."""
    try:
        logger.info("=== Iniciando Limpeza de Cache ===")
        
        # Limpar cache expirado
        cache_service.clear_pattern("*")
        
        # Log de estatísticas
        stats = cache_service.get_stats()
        logger.info(f"Cache limpo - Estatísticas: {stats}")
        
        logger.info("Limpeza de cache concluída com sucesso")
        
    except Exception as e:
        logger.error(f"Erro na limpeza de cache: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

