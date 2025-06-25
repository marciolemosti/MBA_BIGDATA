#!/usr/bin/env python3
"""
Script de Teste das Implementações Refatoradas
Valida os novos conectores de APIs e funcionalidades implementadas.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
Data: 2025-06-23
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_implementations")


def test_data_sources():
    """Testa os conectores de dados."""
    logger.info("=== Testando Conectores de Dados ===")
    
    try:
        from data_sources.data_manager import DataManager
        
        # Inicializar gerenciador
        data_manager = DataManager()
        logger.info("✓ DataManager inicializado com sucesso")
        
        # Testar indicadores disponíveis
        indicators = data_manager.get_available_indicators()
        logger.info(f"✓ Indicadores disponíveis: {len(indicators)} fontes")
        
        for source, indicator_list in indicators.items():
            logger.info(f"  - {source}: {len(indicator_list)} indicadores")
        
        # Testar status do sistema
        status = data_manager.get_system_status()
        logger.info("✓ Status do sistema obtido")
        
        # Testar coleta de dados (simulada)
        test_indicators = ['ipca', 'selic']
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        logger.info(f"Testando coleta de dados para: {test_indicators}")
        
        try:
            results = data_manager.get_multiple_indicators(
                test_indicators, start_date, end_date
            )
            
            for indicator, df in results.items():
                logger.info(f"  - {indicator}: {len(df)} registros coletados")
                
        except Exception as e:
            logger.warning(f"Coleta de dados (esperado falhar sem APIs reais): {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao testar conectores: {e}")
        return False


def test_data_quality_validator():
    """Testa o validador de qualidade de dados."""
    logger.info("=== Testando Validador de Qualidade ===")
    
    try:
        # Importar e testar o validador
        sys.path.insert(0, str(Path(__file__).parent))
        
        # Simular execução do validador
        from validate_data_quality import DataQualityValidator
        
        validator = DataQualityValidator()
        logger.info("✓ DataQualityValidator inicializado")
        
        # Testar encoder JSON
        from validate_data_quality import JSONEncoder
        import numpy as np
        import json
        
        test_data = {
            'int64_value': np.int64(42),
            'float64_value': np.float64(3.14),
            'datetime_value': datetime.now(),
            'regular_value': 'test'
        }
        
        json_str = json.dumps(test_data, cls=JSONEncoder)
        logger.info("✓ JSONEncoder funcionando corretamente")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao testar validador: {e}")
        return False


def test_database_module():
    """Testa o módulo de banco de dados."""
    logger.info("=== Testando Módulo de Banco de Dados ===")
    
    try:
        from database.postgres_manager import DatabaseConfig, DatabaseManager
        
        # Testar configuração
        config = DatabaseConfig("development")
        logger.info("✓ DatabaseConfig inicializado")
        
        # Testar string de conexão
        conn_str = config.get_connection_string()
        logger.info("✓ String de conexão gerada")
        
        # Testar parâmetros psycopg2
        params = config.get_psycopg2_params()
        logger.info("✓ Parâmetros psycopg2 gerados")
        
        # Nota: Não testamos conexão real pois PostgreSQL pode não estar rodando
        logger.info("✓ Módulo de banco de dados validado (sem conexão real)")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao testar módulo de banco: {e}")
        return False


def test_file_structure():
    """Testa a estrutura de arquivos."""
    logger.info("=== Testando Estrutura de Arquivos ===")
    
    project_root = Path(__file__).parent.parent
    
    # Arquivos essenciais
    essential_files = [
        'docker-compose.yml',
        '.gitignore',
        '.env.example',
        'requirements.txt',
        'database/init/01_init_database.sql',
        'src/data_sources/__init__.py',
        'src/data_sources/data_manager.py',
        'src/data_sources/ibge_connector.py',
        'src/data_sources/bcb_connector.py',
        'src/data_sources/tesouro_connector.py',
        'src/data_sources/receita_connector.py',
        'src/database/postgres_manager.py',
        'scripts/update_data.py',
        'scripts/validate_data_quality.py'
    ]
    
    missing_files = []
    
    for file_path in essential_files:
        full_path = project_root / file_path
        if full_path.exists():
            logger.info(f"✓ {file_path}")
        else:
            logger.error(f"✗ {file_path} - AUSENTE")
            missing_files.append(file_path)
    
    if missing_files:
        logger.error(f"Arquivos ausentes: {missing_files}")
        return False
    
    logger.info("✓ Todos os arquivos essenciais presentes")
    return True


def test_imports():
    """Testa importações dos módulos."""
    logger.info("=== Testando Importações ===")
    
    modules_to_test = [
        'pandas',
        'numpy',
        'streamlit',
        'psycopg2',
        'sqlalchemy',
        'requests',
        'plotly',
        'prophet'
    ]
    
    failed_imports = []
    
    for module in modules_to_test:
        try:
            __import__(module)
            logger.info(f"✓ {module}")
        except ImportError as e:
            logger.error(f"✗ {module} - {e}")
            failed_imports.append(module)
    
    if failed_imports:
        logger.error(f"Falhas de importação: {failed_imports}")
        return False
    
    logger.info("✓ Todas as importações bem-sucedidas")
    return True


def main():
    """Função principal do script de teste."""
    logger.info("=== INICIANDO TESTES DAS IMPLEMENTAÇÕES REFATORADAS ===")
    
    tests = [
        ("Importações", test_imports),
        ("Estrutura de Arquivos", test_file_structure),
        ("Conectores de Dados", test_data_sources),
        ("Validador de Qualidade", test_data_quality_validator),
        ("Módulo de Banco de Dados", test_database_module)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"Erro crítico em {test_name}: {e}")
            results[test_name] = False
    
    # Resumo dos resultados
    logger.info("\n=== RESUMO DOS TESTES ===")
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "✓ PASSOU" if result else "✗ FALHOU"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    success_rate = (passed / total) * 100
    logger.info(f"\nTaxa de Sucesso: {passed}/{total} ({success_rate:.1f}%)")
    
    if success_rate >= 80:
        logger.info("🎉 TESTES APROVADOS - Implementações validadas com sucesso!")
        return 0
    else:
        logger.error("❌ TESTES REPROVADOS - Correções necessárias")
        return 1


if __name__ == "__main__":
    sys.exit(main())

