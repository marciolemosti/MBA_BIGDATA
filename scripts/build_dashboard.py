#!/usr/bin/env python3
"""
Script de Build do Dashboard
Responsável pela preparação e validação do dashboard para produção.

Autor: Márcio Lemos
Projeto: MBA em Gestão Analítica em BI e Big Data
"""

import sys
import subprocess
from pathlib import Path

def main():
    """Função principal de build."""
    try:
        print("=== Iniciando Build do Dashboard ===")
        
        project_root = Path(__file__).parent.parent
        
        # Validar estrutura do projeto
        required_dirs = ["src", "config", "data"]
        for dir_name in required_dirs:
            if not (project_root / dir_name).exists():
                raise FileNotFoundError(f"Diretório obrigatório não encontrado: {dir_name}")
        
        # Validar arquivos principais
        main_dashboard = project_root / "src" / "dashboard" / "main.py"
        if not main_dashboard.exists():
            raise FileNotFoundError("Arquivo principal do dashboard não encontrado")
        
        # Testar importações
        print("Validando importações...")
        result = subprocess.run([
            sys.executable, "-c", 
            "import sys; sys.path.insert(0, 'src'); "
            "from common.config_manager import config_manager; "
            "from analytics.data_manager import data_manager; "
            "print('Importações OK')"
        ], cwd=project_root, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Erro nas importações: {result.stderr}")
            sys.exit(1)
        
        print("Build concluído com sucesso")
        print("Dashboard pronto para execução")
        
    except Exception as e:
        print(f"Erro no build: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

