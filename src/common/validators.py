"""
Validadores de Dados
Módulo responsável pela validação e sanitização de dados do sistema.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
"""

import re
import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union, Tuple
from decimal import Decimal, InvalidOperation

from common.logger import get_logger

logger = get_logger("validators")


class DataValidationError(Exception):
    """Exceção customizada para erros de validação de dados."""
    pass


class EconomicDataValidator:
    """
    Validador especializado para dados econômicos.
    
    Esta classe implementa validações específicas para indicadores econômicos,
    garantindo a integridade e qualidade dos dados utilizados nas análises.
    """
    
    # Padrões de validação
    PERCENTAGE_PATTERN = re.compile(r'^-?\d+(\.\d+)?$')
    CURRENCY_PATTERN = re.compile(r'^\d+(\.\d{2})?$')
    DATE_FORMATS = ['%Y-%m-%d', '%d/%m/%Y', '%Y-%m', '%m/%Y']
    
    # Limites para indicadores econômicos brasileiros
    VALIDATION_LIMITS = {
        'ipca': {'min': -5.0, 'max': 50.0},  # IPCA mensal em %
        'selic': {'min': 0.0, 'max': 50.0},  # Taxa Selic em % a.a.
        'cambio': {'min': 1.0, 'max': 10.0},  # USD/BRL
        'pib': {'min': -20.0, 'max': 20.0},  # PIB trimestral em %
        'desemprego': {'min': 0.0, 'max': 30.0},  # Taxa de desemprego em %
        'deficit': {'min': -500.0, 'max': 500.0},  # Déficit em bilhões
        'iof': {'min': 0.0, 'max': 50000.0}  # IOF em milhões
    }
    
    @classmethod
    def validate_dataframe(cls, df: pd.DataFrame, indicator_type: str) -> Tuple[bool, List[str]]:
        """
        Valida um DataFrame completo de indicador econômico.
        
        Args:
            df (pd.DataFrame): DataFrame a ser validado
            indicator_type (str): Tipo do indicador (ipca, selic, etc.)
            
        Returns:
            Tuple[bool, List[str]]: (é_válido, lista_de_erros)
        """
        errors = []
        
        try:
            # Validações estruturais
            if df.empty:
                errors.append("DataFrame está vazio")
                return False, errors
            
            # Verificar colunas obrigatórias
            required_columns = ['data', 'valor']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                errors.append(f"Colunas obrigatórias ausentes: {missing_columns}")
            
            # Validar coluna de data
            date_errors = cls._validate_date_column(df['data'])
            errors.extend(date_errors)
            
            # Validar coluna de valor
            value_errors = cls._validate_value_column(df['valor'], indicator_type)
            errors.extend(value_errors)
            
            # Validar ordenação temporal
            if not cls._is_chronologically_ordered(df['data']):
                errors.append("Dados não estão ordenados cronologicamente")
            
            # Verificar duplicatas
            duplicates = df['data'].duplicated().sum()
            if duplicates > 0:
                errors.append(f"Encontradas {duplicates} datas duplicadas")
            
            # Verificar gaps temporais
            gap_errors = cls._validate_temporal_gaps(df['data'], indicator_type)
            errors.extend(gap_errors)
            
            logger.info(f"Validação do DataFrame {indicator_type}: {len(errors)} erros encontrados")
            
            return len(errors) == 0, errors
            
        except Exception as e:
            logger.error(f"Erro durante validação do DataFrame: {e}")
            errors.append(f"Erro interno de validação: {str(e)}")
            return False, errors
    
    @classmethod
    def _validate_date_column(cls, date_series: pd.Series) -> List[str]:
        """
        Valida a coluna de datas.
        
        Args:
            date_series (pd.Series): Série com datas
            
        Returns:
            List[str]: Lista de erros encontrados
        """
        errors = []
        
        # Verificar valores nulos
        null_count = date_series.isnull().sum()
        if null_count > 0:
            errors.append(f"Encontrados {null_count} valores nulos na coluna de data")
        
        # Verificar formato das datas
        for idx, date_value in date_series.items():
            if pd.isnull(date_value):
                continue
                
            if not cls._is_valid_date(date_value):
                errors.append(f"Data inválida na linha {idx}: {date_value}")
        
        return errors
    
    @classmethod
    def _validate_value_column(cls, value_series: pd.Series, indicator_type: str) -> List[str]:
        """
        Valida a coluna de valores.
        
        Args:
            value_series (pd.Series): Série com valores
            indicator_type (str): Tipo do indicador
            
        Returns:
            List[str]: Lista de erros encontrados
        """
        errors = []
        
        # Verificar valores nulos
        null_count = value_series.isnull().sum()
        if null_count > 0:
            errors.append(f"Encontrados {null_count} valores nulos na coluna de valor")
        
        # Verificar tipos de dados
        non_numeric = value_series.apply(lambda x: not cls._is_numeric(x) if pd.notnull(x) else False)
        if non_numeric.any():
            errors.append(f"Encontrados valores não numéricos: {non_numeric.sum()} ocorrências")
        
        # Verificar limites do indicador
        if indicator_type in cls.VALIDATION_LIMITS:
            limits = cls.VALIDATION_LIMITS[indicator_type]
            
            # Valores abaixo do mínimo
            below_min = value_series < limits['min']
            if below_min.any():
                errors.append(f"Valores abaixo do mínimo ({limits['min']}): {below_min.sum()} ocorrências")
            
            # Valores acima do máximo
            above_max = value_series > limits['max']
            if above_max.any():
                errors.append(f"Valores acima do máximo ({limits['max']}): {above_max.sum()} ocorrências")
        
        # Verificar outliers estatísticos
        outlier_errors = cls._detect_statistical_outliers(value_series, indicator_type)
        errors.extend(outlier_errors)
        
        return errors
    
    @classmethod
    def _is_valid_date(cls, date_value: Any) -> bool:
        """
        Verifica se um valor é uma data válida.
        
        Args:
            date_value (Any): Valor a ser verificado
            
        Returns:
            bool: True se for uma data válida
        """
        if isinstance(date_value, (datetime, date)):
            return True
        
        if isinstance(date_value, str):
            for date_format in cls.DATE_FORMATS:
                try:
                    datetime.strptime(date_value, date_format)
                    return True
                except ValueError:
                    continue
        
        return False
    
    @classmethod
    def _is_numeric(cls, value: Any) -> bool:
        """
        Verifica se um valor é numérico.
        
        Args:
            value (Any): Valor a ser verificado
            
        Returns:
            bool: True se for numérico
        """
        if isinstance(value, (int, float, Decimal)):
            return not (isinstance(value, float) and (np.isnan(value) or np.isinf(value)))
        
        if isinstance(value, str):
            try:
                float(value)
                return True
            except ValueError:
                return False
        
        return False
    
    @classmethod
    def _is_chronologically_ordered(cls, date_series: pd.Series) -> bool:
        """
        Verifica se as datas estão em ordem cronológica.
        
        Args:
            date_series (pd.Series): Série com datas
            
        Returns:
            bool: True se estiver ordenado
        """
        try:
            date_series_converted = pd.to_datetime(date_series)
            return date_series_converted.is_monotonic_increasing
        except Exception:
            return False
    
    @classmethod
    def _validate_temporal_gaps(cls, date_series: pd.Series, indicator_type: str) -> List[str]:
        """
        Valida gaps temporais nos dados.
        
        Args:
            date_series (pd.Series): Série com datas
            indicator_type (str): Tipo do indicador
            
        Returns:
            List[str]: Lista de erros encontrados
        """
        errors = []
        
        try:
            dates = pd.to_datetime(date_series).sort_values()
            
            # Definir frequência esperada por tipo de indicador
            expected_frequencies = {
                'ipca': 'M',      # Mensal
                'selic': 'M',     # Mensal (aproximadamente)
                'cambio': 'D',    # Diário
                'pib': 'Q',       # Trimestral
                'desemprego': 'Q', # Trimestral
                'deficit': 'M',   # Mensal
                'iof': 'M'        # Mensal
            }
            
            if indicator_type in expected_frequencies:
                freq = expected_frequencies[indicator_type]
                
                # Verificar gaps significativos
                if freq in ['M', 'Q']:  # Para dados mensais e trimestrais
                    gaps = dates.diff()
                    
                    if freq == 'M':
                        max_gap = pd.Timedelta(days=45)  # Máximo 45 dias entre observações mensais
                    else:  # Trimestral
                        max_gap = pd.Timedelta(days=120)  # Máximo 120 dias entre observações trimestrais
                    
                    large_gaps = gaps > max_gap
                    if large_gaps.any():
                        gap_count = large_gaps.sum()
                        errors.append(f"Encontrados {gap_count} gaps temporais significativos")
        
        except Exception as e:
            logger.warning(f"Erro ao validar gaps temporais: {e}")
        
        return errors
    
    @classmethod
    def _detect_statistical_outliers(cls, value_series: pd.Series, indicator_type: str) -> List[str]:
        """
        Detecta outliers estatísticos nos dados.
        
        Args:
            value_series (pd.Series): Série com valores
            indicator_type (str): Tipo do indicador
            
        Returns:
            List[str]: Lista de warnings sobre outliers
        """
        errors = []
        
        try:
            # Remover valores nulos para análise
            clean_values = value_series.dropna()
            
            if len(clean_values) < 10:  # Poucos dados para análise estatística
                return errors
            
            # Método IQR para detecção de outliers
            Q1 = clean_values.quantile(0.25)
            Q3 = clean_values.quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = clean_values[(clean_values < lower_bound) | (clean_values > upper_bound)]
            
            if len(outliers) > 0:
                outlier_percentage = (len(outliers) / len(clean_values)) * 100
                
                if outlier_percentage > 5:  # Mais de 5% de outliers
                    errors.append(
                        f"Alto percentual de outliers detectados: {outlier_percentage:.1f}% "
                        f"({len(outliers)} de {len(clean_values)} observações)"
                    )
        
        except Exception as e:
            logger.warning(f"Erro ao detectar outliers: {e}")
        
        return errors
    
    @classmethod
    def sanitize_dataframe(cls, df: pd.DataFrame, indicator_type: str) -> pd.DataFrame:
        """
        Sanitiza um DataFrame removendo/corrigindo dados problemáticos.
        
        Args:
            df (pd.DataFrame): DataFrame a ser sanitizado
            indicator_type (str): Tipo do indicador
            
        Returns:
            pd.DataFrame: DataFrame sanitizado
        """
        try:
            df_clean = df.copy()
            
            # Converter coluna de data
            df_clean['data'] = pd.to_datetime(df_clean['data'], errors='coerce')
            
            # Converter coluna de valor para numérico
            df_clean['valor'] = pd.to_numeric(df_clean['valor'], errors='coerce')
            
            # Remover linhas com dados inválidos
            df_clean = df_clean.dropna(subset=['data', 'valor'])
            
            # Remover duplicatas mantendo a primeira ocorrência
            df_clean = df_clean.drop_duplicates(subset=['data'], keep='first')
            
            # Ordenar por data
            df_clean = df_clean.sort_values('data').reset_index(drop=True)
            
            # Aplicar limites do indicador se definidos
            if indicator_type in cls.VALIDATION_LIMITS:
                limits = cls.VALIDATION_LIMITS[indicator_type]
                df_clean = df_clean[
                    (df_clean['valor'] >= limits['min']) & 
                    (df_clean['valor'] <= limits['max'])
                ]
            
            logger.info(f"DataFrame sanitizado: {len(df)} -> {len(df_clean)} registros")
            
            return df_clean
            
        except Exception as e:
            logger.error(f"Erro durante sanitização: {e}")
            return df


class BusinessRuleValidator:
    """
    Validador de regras de negócio específicas para indicadores econômicos.
    """
    
    @classmethod
    def validate_forecast_parameters(cls, horizon_months: int, confidence_level: float) -> Tuple[bool, List[str]]:
        """
        Valida parâmetros de previsão.
        
        Args:
            horizon_months (int): Horizonte de previsão em meses
            confidence_level (float): Nível de confiança
            
        Returns:
            Tuple[bool, List[str]]: (é_válido, lista_de_erros)
        """
        errors = []
        
        # Validar horizonte de previsão
        if not isinstance(horizon_months, int) or horizon_months <= 0:
            errors.append("Horizonte de previsão deve ser um número inteiro positivo")
        elif horizon_months > 60:  # Máximo 5 anos
            errors.append("Horizonte de previsão não deve exceder 60 meses")
        
        # Validar nível de confiança
        if not isinstance(confidence_level, (int, float)):
            errors.append("Nível de confiança deve ser numérico")
        elif not (0.5 <= confidence_level <= 0.99):
            errors.append("Nível de confiança deve estar entre 0.5 e 0.99")
        
        return len(errors) == 0, errors
    
    @classmethod
    def validate_data_freshness(cls, last_update: datetime, indicator_type: str) -> Tuple[bool, List[str]]:
        """
        Valida se os dados estão atualizados conforme a frequência esperada.
        
        Args:
            last_update (datetime): Data da última atualização
            indicator_type (str): Tipo do indicador
            
        Returns:
            Tuple[bool, List[str]]: (é_válido, lista_de_warnings)
        """
        warnings = []
        
        now = datetime.now()
        days_since_update = (now - last_update).days
        
        # Definir limites de atualização por tipo de indicador
        freshness_limits = {
            'ipca': 45,       # IPCA: máximo 45 dias
            'selic': 60,      # Selic: máximo 60 dias
            'cambio': 7,      # Câmbio: máximo 7 dias
            'pib': 120,       # PIB: máximo 120 dias
            'desemprego': 120, # Desemprego: máximo 120 dias
            'deficit': 60,    # Déficit: máximo 60 dias
            'iof': 60         # IOF: máximo 60 dias
        }
        
        if indicator_type in freshness_limits:
            limit = freshness_limits[indicator_type]
            
            if days_since_update > limit:
                warnings.append(
                    f"Dados desatualizados: {days_since_update} dias desde a última atualização "
                    f"(limite: {limit} dias)"
                )
        
        return len(warnings) == 0, warnings

