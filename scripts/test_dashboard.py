#!/usr/bin/env python3
"""
Script de Teste do Dashboard
Responsável por testar todas as funcionalidades do dashboard.

Autor: Márcio Lemos
Projeto: MBA em Gestão Analítica em BI e Big Data
"""

import sys
import time
import subprocess
import requests
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.logger import get_logger

logger = get_logger("dashboard_test")


class DashboardTester:
    """
    Testador do dashboard com validação de funcionalidades.
    """
    
    def __init__(self):
        """Inicializa o testador."""
        self.project_root = Path(__file__).parent.parent
        self.dashboard_port = 8501
        self.dashboard_url = f"http://localhost:{self.dashboard_port}"
        
    def run_all_tests(self):
        """Executa todos os testes do dashboard."""
        try:
            logger.info("=== Iniciando Testes do Dashboard ===")
            
            # Teste 1: Validar estrutura do projeto
            if not self.test_project_structure():
                return False
            
            # Teste 2: Validar imports
            if not self.test_imports():
                return False
            
            # Teste 3: Validar dados
            if not self.test_data_availability():
                return False
            
            # Teste 4: Validar configurações
            if not self.test_configurations():
                return False
            
            # Teste 5: Testar funcionalidades principais
            if not self.test_core_functionality():
                return False
            
            logger.info("🎉 Todos os testes passaram com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"Erro crítico nos testes: {e}")
            return False
    
    def test_project_structure(self):
        """Testa a estrutura do projeto."""
        try:
            logger.info("Testando estrutura do projeto...")
            
            required_dirs = [
                "src/analytics",
                "src/data_services", 
                "src/dashboard",
                "src/common",
                "config",
                "data",
                "scripts"
            ]
            
            for dir_path in required_dirs:
                full_path = self.project_root / dir_path
                if not full_path.exists():
                    logger.error(f"❌ Diretório obrigatório não encontrado: {dir_path}")
                    return False
            
            required_files = [
                "src/dashboard/main.py",
                "config/application.yaml",
                "requirements.txt"
            ]
            
            for file_path in required_files:
                full_path = self.project_root / file_path
                if not full_path.exists():
                    logger.error(f"❌ Arquivo obrigatório não encontrado: {file_path}")
                    return False
            
            logger.info("✅ Estrutura do projeto validada")
            return True
            
        except Exception as e:
            logger.error(f"Erro no teste de estrutura: {e}")
            return False
    
    def test_imports(self):
        """Testa se todos os imports funcionam."""
        try:
            logger.info("Testando imports...")
            
            # Testar imports principais
            test_script = """
import sys
sys.path.insert(0, 'src')

# Testar imports básicos
import streamlit
import pandas
import numpy
import plotly.graph_objects
import plotly.express

# Testar imports do projeto
from common.config_manager import config_manager
from common.logger import get_logger
from analytics.data_manager import data_manager
from analytics.forecast_models import forecast_engine
from data_services.cache_service import cache_service

print("Todos os imports OK")
"""
            
            result = subprocess.run([
                sys.executable, "-c", test_script
            ], cwd=self.project_root, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"❌ Erro nos imports: {result.stderr}")
                return False
            
            logger.info("✅ Imports validados")
            return True
            
        except Exception as e:
            logger.error(f"Erro no teste de imports: {e}")
            return False
    
    def test_data_availability(self):
        """Testa disponibilidade dos dados."""
        try:
            logger.info("Testando disponibilidade dos dados...")
            
            from analytics.data_manager import data_manager
            
            # Verificar indicadores disponíveis
            available_indicators = data_manager.get_available_indicators()
            
            if not available_indicators:
                logger.error("❌ Nenhum indicador disponível")
                return False
            
            logger.info(f"✅ {len(available_indicators)} indicadores disponíveis")
            
            # Testar carregamento de dados
            for indicator in available_indicators[:3]:  # Testar primeiros 3
                df = data_manager.load_indicator_data(indicator)
                if df is None or df.empty:
                    logger.warning(f"⚠️ Dados vazios para {indicator}")
                else:
                    logger.info(f"✅ Dados carregados para {indicator}: {len(df)} registros")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro no teste de dados: {e}")
            return False
    
    def test_configurations(self):
        """Testa as configurações do sistema."""
        try:
            logger.info("Testando configurações...")
            
            from common.config_manager import config_manager
            
            # Testar configurações principais
            app_config = config_manager.get_section("application")
            if not app_config:
                logger.error("❌ Configurações da aplicação não encontradas")
                return False
            
            analytics_config = config_manager.get_section("analytics")
            if not analytics_config:
                logger.error("❌ Configurações de analytics não encontradas")
                return False
            
            # Verificar horizonte de previsão
            horizon = config_manager.get("analytics.forecast_horizon_months", 0)
            if horizon != 24:
                logger.error(f"❌ Horizonte de previsão incorreto: {horizon} (esperado: 24)")
                return False
            
            logger.info("✅ Configurações validadas")
            return True
            
        except Exception as e:
            logger.error(f"Erro no teste de configurações: {e}")
            return False
    
    def test_core_functionality(self):
        """Testa funcionalidades principais."""
        try:
            logger.info("Testando funcionalidades principais...")
            
            from analytics.data_manager import data_manager
            from analytics.forecast_models import forecast_engine
            
            # Testar geração de previsão
            available_indicators = data_manager.get_available_indicators()
            
            if available_indicators:
                test_indicator = available_indicators[0]
                df = data_manager.load_indicator_data(test_indicator)
                
                if df is not None and len(df) >= 24:
                    forecast_result = forecast_engine.generate_forecast(df, test_indicator)
                    
                    if forecast_result is None:
                        logger.warning(f"⚠️ Não foi possível gerar previsão para {test_indicator}")
                    else:
                        logger.info(f"✅ Previsão gerada para {test_indicator}")
                        
                        # Validar previsão
                        if len(forecast_result.forecast_dates) != 24:
                            logger.error(f"❌ Previsão com horizonte incorreto: {len(forecast_result.forecast_dates)}")
                            return False
            
            # Testar relatório de qualidade
            quality_report = data_manager.get_data_quality_report()
            if not quality_report:
                logger.warning("⚠️ Relatório de qualidade vazio")
            else:
                logger.info("✅ Relatório de qualidade gerado")
            
            logger.info("✅ Funcionalidades principais validadas")
            return True
            
        except Exception as e:
            logger.error(f"Erro no teste de funcionalidades: {e}")
            return False


def main():
    """Função principal de teste."""
    try:
        logger.info("=== Iniciando Testes do Dashboard ===")
        
        tester = DashboardTester()
        success = tester.run_all_tests()
        
        if success:
            logger.info("🎉 Todos os testes passaram - Dashboard pronto para uso!")
            sys.exit(0)
        else:
            logger.error("❌ Alguns testes falharam")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Erro crítico nos testes: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

