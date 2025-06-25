"""
Modelos de Previsão Econômica
Módulo responsável pela implementação de modelos preditivos para indicadores econômicos.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import warnings

# Suprimir warnings do Prophet
warnings.filterwarnings('ignore', category=FutureWarning)

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

from common.logger import get_logger
from common.config_manager import config_manager
from common.validators import BusinessRuleValidator

logger = get_logger("forecast_models")


@dataclass
class ForecastResult:
    """Resultado de uma previsão."""
    indicator_code: str
    forecast_dates: List[datetime]
    forecast_values: List[float]
    lower_bound: List[float]
    upper_bound: List[float]
    confidence_level: float
    model_type: str
    model_performance: Dict[str, float]
    created_at: datetime


class EconomicForecastEngine:
    """
    Motor de previsões econômicas com múltiplos modelos.
    
    Esta classe implementa diferentes algoritmos de previsão para séries temporais
    econômicas, incluindo modelos estatísticos tradicionais e machine learning.
    """
    
    def __init__(self):
        """Inicializa o motor de previsões."""
        self.horizon_months = config_manager.get("analytics.forecast_horizon_months", 24)
        self.confidence_level = config_manager.get("analytics.confidence_interval", 0.95)
        self.min_data_points = config_manager.get("analytics.min_data_points", 24)
        
        # Garantir que o horizonte seja de 24 meses (2 anos)
        if self.horizon_months != 24:
            self.horizon_months = 24
            logger.info("Horizonte de previsão ajustado para 24 meses (2 anos)")
        
        logger.info(f"Motor de previsões inicializado - Horizonte: {self.horizon_months} meses")
    
    def generate_forecast(self, df: pd.DataFrame, indicator_code: str, 
                         model_type: str = "auto") -> Optional[ForecastResult]:
        """
        Gera previsão para um indicador econômico.
        
        Args:
            df (pd.DataFrame): Dados históricos do indicador
            indicator_code (str): Código do indicador
            model_type (str): Tipo de modelo ("auto", "prophet", "linear", "seasonal")
            
        Returns:
            Optional[ForecastResult]: Resultado da previsão ou None se erro
        """
        try:
            # Validar parâmetros
            is_valid, errors = BusinessRuleValidator.validate_forecast_parameters(
                self.horizon_months, self.confidence_level
            )
            
            if not is_valid:
                logger.error(f"Parâmetros de previsão inválidos: {errors}")
                return None
            
            # Validar dados de entrada
            if df is None or df.empty:
                logger.warning(f"Dados vazios para previsão do indicador {indicator_code}")
                return None
            
            if len(df) < self.min_data_points:
                logger.warning(f"Dados insuficientes para previsão: {len(df)} < {self.min_data_points}")
                return None
            
            # Preparar dados
            df_clean = self._prepare_data(df)
            
            # Selecionar modelo
            if model_type == "auto":
                model_type = self._select_best_model(df_clean, indicator_code)
            
            # Gerar previsão baseada no modelo selecionado
            if model_type == "prophet" and PROPHET_AVAILABLE:
                result = self._forecast_with_prophet(df_clean, indicator_code)
            elif model_type == "seasonal":
                result = self._forecast_with_seasonal_decomposition(df_clean, indicator_code)
            elif model_type == "linear":
                result = self._forecast_with_linear_trend(df_clean, indicator_code)
            else:
                # Fallback para modelo simples
                result = self._forecast_with_moving_average(df_clean, indicator_code)
            
            if result:
                logger.info(f"Previsão gerada para {indicator_code} usando modelo {model_type}")
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao gerar previsão para {indicator_code}: {e}")
            return None
    
    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara dados para modelagem.
        
        Args:
            df (pd.DataFrame): Dados brutos
            
        Returns:
            pd.DataFrame: Dados preparados
        """
        df_clean = df.copy()
        
        # Garantir ordenação por data
        df_clean = df_clean.sort_values('data').reset_index(drop=True)
        
        # Remover valores nulos
        df_clean = df_clean.dropna(subset=['valor'])
        
        # Detectar e tratar outliers extremos
        df_clean = self._handle_outliers(df_clean)
        
        return df_clean
    
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Trata outliers extremos nos dados.
        
        Args:
            df (pd.DataFrame): Dados com possíveis outliers
            
        Returns:
            pd.DataFrame: Dados com outliers tratados
        """
        try:
            values = df['valor']
            
            # Método IQR para detecção de outliers extremos
            Q1 = values.quantile(0.25)
            Q3 = values.quantile(0.75)
            IQR = Q3 - Q1
            
            # Usar fator mais conservador para outliers extremos
            lower_bound = Q1 - 3 * IQR
            upper_bound = Q3 + 3 * IQR
            
            # Substituir outliers extremos por valores interpolados
            df_clean = df.copy()
            outlier_mask = (values < lower_bound) | (values > upper_bound)
            
            if outlier_mask.any():
                # Interpolação linear para outliers
                df_clean.loc[outlier_mask, 'valor'] = np.nan
                df_clean['valor'] = df_clean['valor'].interpolate(method='linear')
                
                logger.info(f"Tratados {outlier_mask.sum()} outliers extremos")
            
            return df_clean
            
        except Exception as e:
            logger.warning(f"Erro ao tratar outliers: {e}")
            return df
    
    def _select_best_model(self, df: pd.DataFrame, indicator_code: str) -> str:
        """
        Seleciona o melhor modelo baseado nas características dos dados.
        
        Args:
            df (pd.DataFrame): Dados históricos
            indicator_code (str): Código do indicador
            
        Returns:
            str: Tipo de modelo recomendado
        """
        try:
            # Análise das características dos dados
            data_length = len(df)
            values = df['valor']
            
            # Verificar sazonalidade
            has_seasonality = self._detect_seasonality(values)
            
            # Verificar tendência
            has_trend = self._detect_trend(values)
            
            # Verificar volatilidade
            volatility = values.std() / values.mean() if values.mean() != 0 else 0
            
            # Regras de seleção de modelo - PRIORIZAR PROPHET para IPCA
            if indicator_code.lower() == "ipca":
                # Para IPCA, sempre usar Prophet se disponível
                if PROPHET_AVAILABLE and data_length >= 24:
                    logger.info(f"Selecionado Prophet para {indicator_code} (modelo preferencial)")
                    return "prophet"
                else:
                    logger.warning(f"Prophet não disponível para {indicator_code}, usando seasonal")
                    return "seasonal"
            elif PROPHET_AVAILABLE and data_length >= 48 and has_seasonality:
                return "prophet"
            elif has_seasonality and data_length >= 24:
                return "seasonal"
            elif has_trend:
                return "linear"
            else:
                return "moving_average"
                
        except Exception as e:
            logger.warning(f"Erro na seleção de modelo: {e}")
            return "moving_average"
    
    def _detect_seasonality(self, values: pd.Series) -> bool:
        """
        Detecta padrões sazonais nos dados.
        
        Args:
            values (pd.Series): Série de valores
            
        Returns:
            bool: True se há sazonalidade detectada
        """
        try:
            if len(values) < 24:  # Mínimo 2 anos para detectar sazonalidade anual
                return False
            
            # Análise de autocorrelação simples
            # Verificar correlação com lag de 12 meses
            if len(values) >= 24:
                lag_12 = values.autocorr(lag=12)
                return abs(lag_12) > 0.3  # Threshold para sazonalidade
            
            return False
            
        except Exception:
            return False
    
    def _detect_trend(self, values: pd.Series) -> bool:
        """
        Detecta tendência nos dados.
        
        Args:
            values (pd.Series): Série de valores
            
        Returns:
            bool: True se há tendência detectada
        """
        try:
            if len(values) < 12:
                return False
            
            # Regressão linear simples
            x = np.arange(len(values))
            slope = np.polyfit(x, values, 1)[0]
            
            # Verificar significância da tendência
            correlation = np.corrcoef(x, values)[0, 1]
            
            return abs(correlation) > 0.3 and abs(slope) > 0.01
            
        except Exception:
            return False
    
    def _forecast_with_prophet(self, df: pd.DataFrame, indicator_code: str) -> Optional[ForecastResult]:
        """
        Gera previsão usando o modelo Prophet.
        
        Args:
            df (pd.DataFrame): Dados históricos
            indicator_code (str): Código do indicador
            
        Returns:
            Optional[ForecastResult]: Resultado da previsão
        """
        try:
            # Preparar dados para Prophet
            prophet_df = pd.DataFrame({
                'ds': df['data'],
                'y': df['valor']
            })
            
            # Configurar modelo Prophet
            model = Prophet(
                interval_width=self.confidence_level,
                yearly_seasonality=True,
                weekly_seasonality=False,
                daily_seasonality=False,
                changepoint_prior_scale=0.05,
                seasonality_prior_scale=10.0
            )
            
            # Treinar modelo
            model.fit(prophet_df)
            
            # Gerar datas futuras
            future_dates = model.make_future_dataframe(
                periods=self.horizon_months,
                freq='M'
            )
            
            # Fazer previsão
            forecast = model.predict(future_dates)
            
            # Extrair apenas previsões futuras
            future_forecast = forecast.tail(self.horizon_months)
            
            # Calcular métricas de performance
            performance = self._calculate_model_performance(
                prophet_df['y'], forecast.head(len(prophet_df))['yhat']
            )
            
            return ForecastResult(
                indicator_code=indicator_code,
                forecast_dates=future_forecast['ds'].tolist(),
                forecast_values=future_forecast['yhat'].tolist(),
                lower_bound=future_forecast['yhat_lower'].tolist(),
                upper_bound=future_forecast['yhat_upper'].tolist(),
                confidence_level=self.confidence_level,
                model_type="prophet",
                model_performance=performance,
                created_at=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Erro no modelo Prophet: {e}")
            return None
    
    def _forecast_with_seasonal_decomposition(self, df: pd.DataFrame, 
                                            indicator_code: str) -> Optional[ForecastResult]:
        """
        Gera previsão usando decomposição sazonal.
        
        Args:
            df (pd.DataFrame): Dados históricos
            indicator_code (str): Código do indicador
            
        Returns:
            Optional[ForecastResult]: Resultado da previsão
        """
        try:
            from statsmodels.tsa.seasonal import seasonal_decompose
            
            # Preparar série temporal
            ts = pd.Series(df['valor'].values, index=pd.to_datetime(df['data']))
            ts = ts.asfreq('M')  # Frequência mensal
            
            # Decomposição sazonal
            decomposition = seasonal_decompose(ts, model='additive', period=12)
            
            # Extrair componentes
            trend = decomposition.trend.dropna()
            seasonal = decomposition.seasonal
            
            # Projetar tendência
            trend_forecast = self._extrapolate_trend(trend, self.horizon_months)
            
            # Projetar sazonalidade
            seasonal_pattern = seasonal.iloc[-12:].values  # Último ano de sazonalidade
            seasonal_forecast = np.tile(seasonal_pattern, 
                                      (self.horizon_months // 12) + 1)[:self.horizon_months]
            
            # Combinar previsões
            forecast_values = trend_forecast + seasonal_forecast
            
            # Gerar datas futuras
            last_date = df['data'].max()
            forecast_dates = pd.date_range(
                start=last_date + timedelta(days=30),
                periods=self.horizon_months,
                freq='M'
            ).tolist()
            
            # Calcular intervalos de confiança (estimativa simples)
            residuals_std = (ts - (trend + seasonal)).std()
            margin = 1.96 * residuals_std  # 95% de confiança
            
            lower_bound = (forecast_values - margin).tolist()
            upper_bound = (forecast_values + margin).tolist()
            
            # Calcular performance
            fitted_values = (trend + seasonal).dropna()
            actual_values = ts.loc[fitted_values.index]
            performance = self._calculate_model_performance(actual_values, fitted_values)
            
            return ForecastResult(
                indicator_code=indicator_code,
                forecast_dates=forecast_dates,
                forecast_values=forecast_values.tolist(),
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                confidence_level=self.confidence_level,
                model_type="seasonal",
                model_performance=performance,
                created_at=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Erro no modelo sazonal: {e}")
            return None
    
    def _forecast_with_linear_trend(self, df: pd.DataFrame, 
                                  indicator_code: str) -> Optional[ForecastResult]:
        """
        Gera previsão usando tendência linear.
        
        Args:
            df (pd.DataFrame): Dados históricos
            indicator_code (str): Código do indicador
            
        Returns:
            Optional[ForecastResult]: Resultado da previsão
        """
        try:
            values = df['valor'].values
            x = np.arange(len(values))
            
            # Regressão linear
            coeffs = np.polyfit(x, values, 1)
            slope, intercept = coeffs
            
            # Gerar previsões
            future_x = np.arange(len(values), len(values) + self.horizon_months)
            forecast_values = slope * future_x + intercept
            
            # Calcular erro padrão
            fitted_values = slope * x + intercept
            residuals = values - fitted_values
            mse = np.mean(residuals ** 2)
            std_error = np.sqrt(mse)
            
            # Intervalos de confiança
            margin = 1.96 * std_error
            lower_bound = (forecast_values - margin).tolist()
            upper_bound = (forecast_values + margin).tolist()
            
            # Gerar datas futuras
            last_date = df['data'].max()
            forecast_dates = pd.date_range(
                start=last_date + timedelta(days=30),
                periods=self.horizon_months,
                freq='M'
            ).tolist()
            
            # Calcular performance
            performance = self._calculate_model_performance(values, fitted_values)
            
            return ForecastResult(
                indicator_code=indicator_code,
                forecast_dates=forecast_dates,
                forecast_values=forecast_values.tolist(),
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                confidence_level=self.confidence_level,
                model_type="linear",
                model_performance=performance,
                created_at=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Erro no modelo linear: {e}")
            return None
    
    def _forecast_with_moving_average(self, df: pd.DataFrame, 
                                    indicator_code: str) -> Optional[ForecastResult]:
        """
        Gera previsão usando média móvel.
        
        Args:
            df (pd.DataFrame): Dados históricos
            indicator_code (str): Código do indicador
            
        Returns:
            Optional[ForecastResult]: Resultado da previsão
        """
        try:
            values = df['valor']
            
            # Calcular média móvel dos últimos 12 meses
            window_size = min(12, len(values) // 2)
            moving_avg = values.rolling(window=window_size).mean().iloc[-1]
            
            # Previsão constante baseada na média móvel
            forecast_values = [moving_avg] * self.horizon_months
            
            # Calcular volatilidade histórica
            volatility = values.std()
            margin = 1.96 * volatility
            
            lower_bound = [moving_avg - margin] * self.horizon_months
            upper_bound = [moving_avg + margin] * self.horizon_months
            
            # Gerar datas futuras
            last_date = df['data'].max()
            forecast_dates = pd.date_range(
                start=last_date + timedelta(days=30),
                periods=self.horizon_months,
                freq='M'
            ).tolist()
            
            # Performance simples
            performance = {
                'mae': volatility,
                'rmse': volatility,
                'mape': abs(volatility / values.mean()) * 100 if values.mean() != 0 else 0
            }
            
            return ForecastResult(
                indicator_code=indicator_code,
                forecast_dates=forecast_dates,
                forecast_values=forecast_values,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                confidence_level=self.confidence_level,
                model_type="moving_average",
                model_performance=performance,
                created_at=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Erro no modelo de média móvel: {e}")
            return None
    
    def _extrapolate_trend(self, trend: pd.Series, periods: int) -> np.ndarray:
        """
        Extrapola tendência para períodos futuros.
        
        Args:
            trend (pd.Series): Série de tendência
            periods (int): Número de períodos para extrapolação
            
        Returns:
            np.ndarray: Valores de tendência extrapolados
        """
        try:
            # Usar últimos pontos válidos para extrapolação
            valid_trend = trend.dropna()
            
            if len(valid_trend) < 2:
                # Se não há tendência suficiente, usar último valor
                return np.full(periods, valid_trend.iloc[-1])
            
            # Regressão linear nos últimos pontos
            x = np.arange(len(valid_trend))
            y = valid_trend.values
            
            slope, intercept = np.polyfit(x, y, 1)
            
            # Extrapolação
            future_x = np.arange(len(valid_trend), len(valid_trend) + periods)
            return slope * future_x + intercept
            
        except Exception:
            # Fallback: repetir último valor
            return np.full(periods, trend.iloc[-1])
    
    def _calculate_model_performance(self, actual: np.ndarray, predicted: np.ndarray) -> Dict[str, float]:
        """
        Calcula métricas de performance do modelo.
        
        Args:
            actual (np.ndarray): Valores reais
            predicted (np.ndarray): Valores previstos
            
        Returns:
            Dict[str, float]: Métricas de performance
        """
        try:
            # Garantir que os arrays tenham o mesmo tamanho
            min_length = min(len(actual), len(predicted))
            actual = actual[:min_length]
            predicted = predicted[:min_length]
            
            # Calcular métricas
            mae = np.mean(np.abs(actual - predicted))
            rmse = np.sqrt(np.mean((actual - predicted) ** 2))
            
            # MAPE (Mean Absolute Percentage Error)
            mape = np.mean(np.abs((actual - predicted) / actual)) * 100
            mape = np.clip(mape, 0, 1000)  # Limitar valores extremos
            
            # R²
            ss_res = np.sum((actual - predicted) ** 2)
            ss_tot = np.sum((actual - np.mean(actual)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
            
            return {
                'mae': float(mae),
                'rmse': float(rmse),
                'mape': float(mape),
                'r_squared': float(r_squared)
            }
            
        except Exception as e:
            logger.warning(f"Erro ao calcular performance: {e}")
            return {
                'mae': 0.0,
                'rmse': 0.0,
                'mape': 0.0,
                'r_squared': 0.0
            }


# Instância global do motor de previsões
forecast_engine = EconomicForecastEngine()

