"""
Conector para API do Banco Central do Brasil (BCB).

Este módulo implementa a coleta de dados econômicos do BCB, incluindo:
- Taxa Selic
- Taxa de Câmbio (USD/BRL)
- Outros indicadores monetários

Autor: Márcio Lemos
Data: 2025-06-23
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from . import APIConnectorBase


class BCBConnector(APIConnectorBase):
    """
    Conector para API do Banco Central do Brasil.
    
    Implementa coleta de dados dos principais indicadores monetários
    disponibilizados pelo BCB através de sua API de dados abertos.
    """
    
    # URL base da API do BCB
    BCB_BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"
    
    # Códigos das séries do BCB para cada indicador
    INDICATOR_SERIES = {
        'selic': {
            'code': '11',  # Taxa de juros - Selic
            'name': 'Taxa Selic',
            'unit': '% a.a.',
            'frequency': 'daily'
        },
        'cambio_ptax_venda': {
            'code': '1',  # Taxa de câmbio - R$/US$ - PTAX - venda
            'name': 'Taxa de Câmbio USD/BRL - PTAX Venda',
            'unit': 'R$/US$',
            'frequency': 'daily'
        },
        'cambio_ptax_compra': {
            'code': '10813',  # Taxa de câmbio - R$/US$ - PTAX - compra
            'name': 'Taxa de Câmbio USD/BRL - PTAX Compra',
            'unit': 'R$/US$',
            'frequency': 'daily'
        },
        'ipca_bcb': {
            'code': '433',  # IPCA - Variação mensal
            'name': 'IPCA - Variação Mensal (BCB)',
            'unit': '% a.m.',
            'frequency': 'monthly'
        }
    }
    
    def __init__(self):
        """
        Inicializa o conector do Banco Central.
        """
        super().__init__(
            base_url=self.BCB_BASE_URL,
            timeout=30,
            max_retries=3
        )
        
        # Cache médio para dados do BCB (dados diários mudam frequentemente)
        self.default_cache_duration = 1800  # 30 minutos
        
        self.logger.info("Conector BCB inicializado com sucesso")
    
    def get_available_indicators(self) -> List[str]:
        """
        Retorna lista de indicadores disponíveis no BCB.
        
        Returns:
            Lista de códigos de indicadores disponíveis
        """
        return list(self.INDICATOR_SERIES.keys())
    
    def _build_bcb_endpoint(self, indicator: str, start_date: datetime, 
                           end_date: datetime) -> str:
        """
        Constrói endpoint para consulta à API do BCB.
        
        Args:
            indicator: Código do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            Endpoint da API
        """
        if indicator not in self.INDICATOR_SERIES:
            raise ValueError(f"Indicador '{indicator}' não suportado. "
                           f"Disponíveis: {list(self.INDICATOR_SERIES.keys())}")
        
        series_code = self.INDICATOR_SERIES[indicator]['code']
        start_str = self._format_date(start_date, "%d/%m/%Y")
        end_str = self._format_date(end_date, "%d/%m/%Y")
        
        return f"{series_code}/dados?formato=json&dataInicial={start_str}&dataFinal={end_str}"
    
    def _parse_bcb_response(self, data: List[Dict], indicator: str) -> pd.DataFrame:
        """
        Converte resposta da API do BCB em DataFrame padronizado.
        
        Args:
            data: Dados da resposta JSON
            indicator: Código do indicador
            
        Returns:
            DataFrame com colunas 'data' e 'valor'
        """
        if not data:
            self.logger.warning(f"Resposta vazia para {indicator}")
            return pd.DataFrame(columns=['data', 'valor'])
        
        df_data = []
        for record in data:
            try:
                # Extrair data e valor
                data_str = record.get('data', '')
                valor_str = record.get('valor', '')
                
                if not data_str or not valor_str:
                    continue
                
                # Converter data (formato DD/MM/YYYY)
                try:
                    data_obj = datetime.strptime(data_str, "%d/%m/%Y")
                except ValueError:
                    self.logger.warning(f"Data inválida para {indicator}: {data_str}")
                    continue
                
                # Converter valor para float
                try:
                    valor = float(valor_str.replace(',', '.'))
                except (ValueError, TypeError):
                    self.logger.warning(f"Valor inválido para {indicator}: {valor_str}")
                    continue
                
                df_data.append({
                    'data': data_obj.strftime('%Y-%m-%d'),
                    'valor': valor
                })
                
            except Exception as e:
                self.logger.warning(f"Erro ao processar registro {record}: {e}")
                continue
        
        if not df_data:
            self.logger.warning(f"Nenhum dado válido encontrado para {indicator}")
            return pd.DataFrame(columns=['data', 'valor'])
        
        df = pd.DataFrame(df_data)
        df['data'] = pd.to_datetime(df['data'])
        df = df.sort_values('data').reset_index(drop=True)
        
        # Remover duplicatas mantendo o último valor
        df = df.drop_duplicates(subset=['data'], keep='last').reset_index(drop=True)
        
        self.logger.info(f"Processados {len(df)} registros para {indicator}")
        return df
    
    def get_data(self, indicator: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Obtém dados de um indicador específico do BCB.
        
        Args:
            indicator: Código do indicador ('selic', 'cambio_ptax_venda', etc.)
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com colunas 'data' e 'valor'
            
        Raises:
            ValueError: Se o indicador não for suportado
            requests.RequestException: Em caso de erro na API
        """
        self.logger.info(f"Coletando dados do BCB para {indicator}")
        
        # Validar parâmetros
        self._validate_date_range(start_date, end_date)
        
        if indicator not in self.INDICATOR_SERIES:
            raise ValueError(f"Indicador '{indicator}' não suportado pelo BCB. "
                           f"Disponíveis: {list(self.INDICATOR_SERIES.keys())}")
        
        try:
            # Construir endpoint
            endpoint = self._build_bcb_endpoint(indicator, start_date, end_date)
            
            # Fazer requisição
            response_data = self._make_request(
                endpoint=endpoint,
                params=None,  # Parâmetros já estão no endpoint
                cache_ttl=self.default_cache_duration
            )
            
            # Processar resposta
            df = self._parse_bcb_response(response_data, indicator)
            
            self.logger.info(f"Coletados {len(df)} registros do BCB para {indicator}")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao coletar dados do BCB para {indicator}: {e}")
            
            # Tentar fallback com dados históricos se disponível
            fallback_data = self._get_fallback_data(indicator, start_date, end_date)
            if fallback_data is not None:
                self.logger.info(f"Usando dados de fallback para {indicator}")
                return fallback_data
            
            raise
    
    def _get_fallback_data(self, indicator: str, start_date: datetime, 
                          end_date: datetime) -> Optional[pd.DataFrame]:
        """
        Obtém dados de fallback em caso de falha na API.
        
        Args:
            indicator: Código do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados de fallback ou None se não disponível
        """
        try:
            # Tentar carregar dados históricos de arquivo local se existir
            import os
            fallback_file = f"data/{indicator}.json"
            
            if os.path.exists(fallback_file):
                self.logger.info(f"Carregando dados de fallback de {fallback_file}")
                df = pd.read_json(fallback_file)
                df['data'] = pd.to_datetime(df['data'])
                
                # Filtrar por intervalo
                mask = (df['data'] >= start_date) & (df['data'] <= end_date)
                return df[mask].reset_index(drop=True)
            
        except Exception as e:
            self.logger.warning(f"Erro ao carregar dados de fallback: {e}")
        
        return None
    
    def get_latest_data(self, indicator: str, days: int = 30) -> pd.DataFrame:
        """
        Obtém os dados mais recentes de um indicador.
        
        Args:
            indicator: Código do indicador
            days: Número de dias para buscar (padrão: 30)
            
        Returns:
            DataFrame com os dados mais recentes
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        return self.get_data(indicator, start_date, end_date)
    
    def get_selic_rate(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Obtém dados da Taxa Selic.
        
        Args:
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados da Taxa Selic
        """
        return self.get_data('selic', start_date, end_date)
    
    def get_exchange_rate(self, start_date: datetime, end_date: datetime, 
                         rate_type: str = 'venda') -> pd.DataFrame:
        """
        Obtém dados da Taxa de Câmbio USD/BRL.
        
        Args:
            start_date: Data inicial
            end_date: Data final
            rate_type: Tipo da taxa ('venda' ou 'compra')
            
        Returns:
            DataFrame com dados da Taxa de Câmbio
        """
        indicator = f'cambio_ptax_{rate_type}'
        return self.get_data(indicator, start_date, end_date)
    
    def get_current_selic(self) -> Optional[float]:
        """
        Obtém a Taxa Selic atual (último valor disponível).
        
        Returns:
            Taxa Selic atual ou None se não disponível
        """
        try:
            df = self.get_latest_data('selic', days=7)
            if not df.empty:
                return float(df.iloc[-1]['valor'])
        except Exception as e:
            self.logger.error(f"Erro ao obter Selic atual: {e}")
        
        return None
    
    def get_current_exchange_rate(self) -> Optional[float]:
        """
        Obtém a Taxa de Câmbio atual (último valor disponível).
        
        Returns:
            Taxa de câmbio atual ou None se não disponível
        """
        try:
            df = self.get_latest_data('cambio_ptax_venda', days=7)
            if not df.empty:
                return float(df.iloc[-1]['valor'])
        except Exception as e:
            self.logger.error(f"Erro ao obter câmbio atual: {e}")
        
        return None
    
    def get_indicator_metadata(self, indicator: str) -> Dict[str, Any]:
        """
        Obtém metadados de um indicador.
        
        Args:
            indicator: Código do indicador
            
        Returns:
            Dicionário com metadados do indicador
        """
        if indicator not in self.INDICATOR_SERIES:
            raise ValueError(f"Indicador '{indicator}' não suportado")
        
        config = self.INDICATOR_SERIES[indicator]
        
        metadata = {
            'indicator': indicator,
            'source': 'BCB',
            'series_code': config['code'],
            'name': config['name'],
            'unit': config['unit'],
            'frequency': config['frequency'],
            'description': f"{config['name']} - Banco Central do Brasil"
        }
        
        return metadata
    
    def get_multiple_indicators(self, indicators: List[str], start_date: datetime, 
                               end_date: datetime) -> Dict[str, pd.DataFrame]:
        """
        Obtém dados de múltiplos indicadores de uma vez.
        
        Args:
            indicators: Lista de códigos de indicadores
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            Dicionário com DataFrames para cada indicador
        """
        results = {}
        
        for indicator in indicators:
            try:
                self.logger.info(f"Coletando {indicator}...")
                results[indicator] = self.get_data(indicator, start_date, end_date)
            except Exception as e:
                self.logger.error(f"Erro ao coletar {indicator}: {e}")
                results[indicator] = pd.DataFrame(columns=['data', 'valor'])
        
        return results
    
    def validate_data_quality(self, df: pd.DataFrame, indicator: str) -> Dict[str, Any]:
        """
        Valida a qualidade dos dados coletados.
        
        Args:
            df: DataFrame com os dados
            indicator: Código do indicador
            
        Returns:
            Dicionário com métricas de qualidade
        """
        if df.empty:
            return {
                'valid': False,
                'errors': ['DataFrame vazio'],
                'warnings': [],
                'metrics': {}
            }
        
        errors = []
        warnings = []
        
        # Verificar valores nulos
        null_count = df['valor'].isnull().sum()
        if null_count > 0:
            errors.append(f"{null_count} valores nulos encontrados")
        
        # Verificar valores negativos (dependendo do indicador)
        if indicator in ['selic', 'cambio_ptax_venda', 'cambio_ptax_compra']:
            negative_count = (df['valor'] < 0).sum()
            if negative_count > 0:
                warnings.append(f"{negative_count} valores negativos encontrados")
        
        # Verificar outliers (valores muito altos ou baixos)
        if len(df) > 10:
            q1 = df['valor'].quantile(0.25)
            q3 = df['valor'].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            outliers = ((df['valor'] < lower_bound) | (df['valor'] > upper_bound)).sum()
            if outliers > 0:
                warnings.append(f"{outliers} possíveis outliers encontrados")
        
        # Verificar continuidade temporal
        df_sorted = df.sort_values('data')
        gaps = []
        for i in range(1, len(df_sorted)):
            gap_days = (df_sorted.iloc[i]['data'] - df_sorted.iloc[i-1]['data']).days
            if gap_days > 7:  # Gap maior que 1 semana
                gaps.append(gap_days)
        
        if gaps:
            warnings.append(f"{len(gaps)} gaps temporais encontrados (máximo: {max(gaps)} dias)")
        
        metrics = {
            'total_records': len(df),
            'date_range': {
                'start': df['data'].min().strftime('%Y-%m-%d'),
                'end': df['data'].max().strftime('%Y-%m-%d')
            },
            'value_stats': {
                'min': float(df['valor'].min()),
                'max': float(df['valor'].max()),
                'mean': float(df['valor'].mean()),
                'std': float(df['valor'].std())
            },
            'null_count': int(null_count),
            'outlier_count': outliers if 'outliers' in locals() else 0
        }
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'metrics': metrics
        }

