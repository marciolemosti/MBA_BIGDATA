#!/usr/bin/env python3
"""
Script de Arquivamento de Logs
Responsável pela manutenção e arquivamento de logs antigos.

Autor: Márcio Lemos
Projeto: MBA em Gestão Analítica em BI e Big Data
"""

import os
import gzip
import shutil
from datetime import datetime, timedelta
from pathlib import Path

def main():
    """Função principal de arquivamento."""
    try:
        print("=== Iniciando Arquivamento de Logs ===")
        
        logs_dir = Path(__file__).parent.parent / "logs"
        
        if not logs_dir.exists():
            print("Diretório de logs não encontrado")
            return
        
        # Arquivar logs com mais de 7 dias
        cutoff_date = datetime.now() - timedelta(days=7)
        archived_count = 0
        
        for log_file in logs_dir.glob("*.log"):
            if log_file.stat().st_mtime < cutoff_date.timestamp():
                # Comprimir arquivo
                with open(log_file, 'rb') as f_in:
                    with gzip.open(f"{log_file}.gz", 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # Remover arquivo original
                log_file.unlink()
                archived_count += 1
        
        print(f"Arquivados {archived_count} arquivos de log")
        print("Arquivamento concluído com sucesso")
        
    except Exception as e:
        print(f"Erro no arquivamento: {e}")


if __name__ == "__main__":
    main()

