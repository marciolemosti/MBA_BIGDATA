"""
Validação e Teste dos Modelos de Previsão
Script para validar se as previsões estão configuradas para 24 meses.

Autor: Márcio Lemos
Projeto: MBA em Gestão Analítica em BI e Big Data
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.logger import get_logger
from analytics.forecast_models import forecast_engine
from analytics.data_manager import data_manager

logger = get_logger("forecast_validation")


def test_forecast_horizon():
    """Testa se as previsões estão configuradas para 24 meses."""
    try:
        logger.info("=== Testando Horizonte de Previsão ===")
        
        # Verificar configuração do motor
        assert forecast_engine.horizon_months == 24, f"Horizonte incorreto: {forecast_engine.horizon_months}"
        logger.info("✅ Horizonte configurado para 24 meses")
        
        # Testar com dados simulados
        test_data = create_test_data()
        
        # Gerar previsão
        forecast_result = forecast_engine.generate_forecast(test_data, "test_indicator")
        
        if forecast_result is None:
            logger.error("❌ Falha ao gerar previsão de teste")
            return False
        
        # Verificar se a previsão tem 24 pontos
        forecast_length = len(forecast_result.forecast_dates)
        assert forecast_length == 24, f"Previsão tem {forecast_length} pontos, esperado 24"
        logger.info(f"✅ Previsão gerada com {forecast_length} pontos (24 meses)")
        
        # Verificar se as datas cobrem 2 anos
        first_date = forecast_result.forecast_dates[0]
        last_date = forecast_result.forecast_dates[-1]
        date_diff = (last_date - first_date).days
        
        # Aproximadamente 2 anos (considerando variação de dias por mês)
        expected_days = 365 * 2  # 2 anos
        tolerance = 60  # 2 meses de tolerância
        
        assert abs(date_diff - expected_days) <= tolerance, f"Período incorreto: {date_diff} dias"
        logger.info(f"✅ Período de previsão: {date_diff} dias (~2 anos)")
        
        # Verificar intervalos de confiança
        assert len(forecast_result.lower_bound) == 24, "Intervalo inferior incorreto"
        assert len(forecast_result.upper_bound) == 24, "Intervalo superior incorreto"
        logger.info("✅ Intervalos de confiança configurados corretamente")
        
        logger.info("=== Teste de Horizonte Concluído com Sucesso ===")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro no teste de horizonte: {e}")
        return False


def create_test_data():
    """Cria dados de teste para validação."""
    # Gerar 60 pontos mensais (5 anos de histórico)
    dates = pd.date_range(start='2019-01-01', periods=60, freq='M')
    
    # Simular série temporal com tendência e sazonalidade
    trend = np.linspace(100, 120, 60)
    seasonal = 5 * np.sin(2 * np.pi * np.arange(60) / 12)
    noise = np.random.normal(0, 2, 60)
    
    values = trend + seasonal + noise
    
    return pd.DataFrame({
        'data': dates,
        'valor': values
    })


def test_real_indicators():
    """Testa previsões com indicadores reais."""
    try:
        logger.info("=== Testando Indicadores Reais ===")
        
        available_indicators = data_manager.get_available_indicators()
        
        if not available_indicators:
            logger.warning("Nenhum indicador disponível para teste")
            return True
        
        # Testar com primeiro indicador disponível
        test_indicator = available_indicators[0]
        logger.info(f"Testando com indicador: {test_indicator}")
        
        # Carregar dados
        df = data_manager.load_indicator_data(test_indicator)
        
        if df is None or len(df) < 24:
            logger.warning(f"Dados insuficientes para {test_indicator}")
            return True
        
        # Gerar previsão
        forecast_result = forecast_engine.generate_forecast(df, test_indicator)
        
        if forecast_result is None:
            logger.warning(f"Não foi possível gerar previsão para {test_indicator}")
            return True
        
        # Validar resultado
        assert len(forecast_result.forecast_dates) == 24, "Horizonte incorreto"
        logger.info(f"✅ Previsão real gerada: {len(forecast_result.forecast_dates)} meses")
        
        # Log de informações da previsão
        logger.info(f"Modelo usado: {forecast_result.model_type}")
        logger.info(f"Confiança: {forecast_result.confidence_level:.0%}")
        logger.info(f"Período: {forecast_result.forecast_dates[0].strftime('%Y-%m')} a {forecast_result.forecast_dates[-1].strftime('%Y-%m')}")
        
        logger.info("=== Teste com Indicadores Reais Concluído ===")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro no teste com indicadores reais: {e}")
        return False


def main():
    """Função principal de validação."""
    try:
        logger.info("=== Iniciando Validação de Previsões ===")
        
        # Teste 1: Horizonte de previsão
        test1_passed = test_forecast_horizon()
        
        # Teste 2: Indicadores reais
        test2_passed = test_real_indicators()
        
        # Resultado final
        if test1_passed and test2_passed:
            logger.info("🎉 Todos os testes passaram - Previsões configuradas para 24 meses")
            sys.exit(0)
        else:
            logger.error("❌ Alguns testes falharam")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Erro crítico na validação: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

