"""
Gerenciador integrado de dados econômicos.

Este módulo coordena a coleta de dados de múltiplas fontes (IBGE, BCB, Tesouro Nacional, 
Receita Federal) e fornece uma interface unificada para o sistema.

Autor: Márcio Lemos
Data: 2025-06-23
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os

from .ibge_connector import IBGEConnector
from .bcb_connector import BCBConnector
from .tesouro_connector import TesouroNacionalConnector
from .receita_connector import ReceitaFederalConnector


class DataManager:
    """
    Gerenciador integrado para coleta de dados econômicos.
    
    Coordena múltiplos conectores de APIs e fornece interface unificada
    para coleta, cache e validação de dados econômicos brasileiros.
    """
    
    # Mapeamento de indicadores para conectores
    INDICATOR_MAPPING = {
        'ipca': 'ibge',
        'pib': 'ibge', 
        'desemprego': 'ibge',
        'selic': 'bcb',
        'cambio_ptax_venda': 'bcb',
        'deficit_primario': 'tesouro',
        'arrecadacao_iof': 'receita'
    }
    
    def __init__(self, data_dir: str = "data", enable_parallel: bool = True):
        """
        Inicializa o gerenciador de dados.
        
        Args:
            data_dir: Diretório para armazenar dados
            enable_parallel: Habilitar coleta paralela de dados
        """
        self.data_dir = data_dir
        self.enable_parallel = enable_parallel
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Criar diretório de dados se não existir
        os.makedirs(data_dir, exist_ok=True)
        
        # Inicializar conectores
        self.connectors = self._initialize_connectors()
        
        # Cache de metadados
        self._metadata_cache = {}
        
        self.logger.info("DataManager inicializado com sucesso")
    
    def _initialize_connectors(self) -> Dict[str, Any]:
        """
        Inicializa todos os conectores de dados.
        
        Returns:
            Dicionário com conectores inicializados
        """
        connectors = {}
        
        try:
            connectors['ibge'] = IBGEConnector()
            self.logger.info("Conector IBGE inicializado")
        except Exception as e:
            self.logger.error(f"Erro ao inicializar conector IBGE: {e}")
            connectors['ibge'] = None
        
        try:
            connectors['bcb'] = BCBConnector()
            self.logger.info("Conector BCB inicializado")
        except Exception as e:
            self.logger.error(f"Erro ao inicializar conector BCB: {e}")
            connectors['bcb'] = None
        
        try:
            connectors['tesouro'] = TesouroNacionalConnector()
            self.logger.info("Conector Tesouro Nacional inicializado")
        except Exception as e:
            self.logger.error(f"Erro ao inicializar conector Tesouro Nacional: {e}")
            connectors['tesouro'] = None
        
        try:
            connectors['receita'] = ReceitaFederalConnector()
            self.logger.info("Conector Receita Federal inicializado")
        except Exception as e:
            self.logger.error(f"Erro ao inicializar conector Receita Federal: {e}")
            connectors['receita'] = None
        
        return connectors
    
    def get_available_indicators(self) -> Dict[str, List[str]]:
        """
        Retorna todos os indicadores disponíveis por fonte.
        
        Returns:
            Dicionário com indicadores por fonte
        """
        indicators = {}
        
        for source, connector in self.connectors.items():
            if connector is not None:
                try:
                    indicators[source] = connector.get_available_indicators()
                except Exception as e:
                    self.logger.error(f"Erro ao obter indicadores de {source}: {e}")
                    indicators[source] = []
            else:
                indicators[source] = []
        
        return indicators
    
    def get_data(self, indicator: str, start_date: datetime, 
                end_date: datetime, force_update: bool = False) -> pd.DataFrame:
        """
        Obtém dados de um indicador específico.
        
        Args:
            indicator: Código do indicador
            start_date: Data inicial
            end_date: Data final
            force_update: Forçar atualização ignorando cache
            
        Returns:
            DataFrame com dados do indicador
            
        Raises:
            ValueError: Se o indicador não for suportado
            Exception: Em caso de erro na coleta
        """
        self.logger.info(f"Coletando dados para {indicator}")
        
        # Verificar se o indicador é suportado
        if indicator not in self.INDICATOR_MAPPING:
            raise ValueError(f"Indicador '{indicator}' não suportado. "
                           f"Disponíveis: {list(self.INDICATOR_MAPPING.keys())}")
        
        # Verificar cache local primeiro (se não forçar atualização)
        if not force_update:
            cached_data = self._load_cached_data(indicator, start_date, end_date)
            if cached_data is not None and not cached_data.empty:
                self.logger.info(f"Dados de cache utilizados para {indicator}")
                return cached_data
        
        # Obter conector apropriado
        source = self.INDICATOR_MAPPING[indicator]
        connector = self.connectors.get(source)
        
        if connector is None:
            self.logger.error(f"Conector {source} não disponível para {indicator}")
            # Tentar carregar dados históricos como fallback
            return self._load_fallback_data(indicator, start_date, end_date)
        
        try:
            # Coletar dados da API
            df = connector.get_data(indicator, start_date, end_date)
            
            # Salvar no cache local
            self._save_cached_data(indicator, df)
            
            self.logger.info(f"Dados coletados com sucesso para {indicator}: {len(df)} registros")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao coletar dados para {indicator}: {e}")
            
            # Tentar fallback com dados históricos
            fallback_data = self._load_fallback_data(indicator, start_date, end_date)
            if fallback_data is not None:
                self.logger.info(f"Usando dados de fallback para {indicator}")
                return fallback_data
            
            raise
    
    def get_multiple_indicators(self, indicators: List[str], start_date: datetime,
                               end_date: datetime, force_update: bool = False) -> Dict[str, pd.DataFrame]:
        """
        Obtém dados de múltiplos indicadores.
        
        Args:
            indicators: Lista de códigos de indicadores
            start_date: Data inicial
            end_date: Data final
            force_update: Forçar atualização ignorando cache
            
        Returns:
            Dicionário com DataFrames para cada indicador
        """
        self.logger.info(f"Coletando dados para {len(indicators)} indicadores")
        
        results = {}
        
        if self.enable_parallel and len(indicators) > 1:
            # Coleta paralela
            with ThreadPoolExecutor(max_workers=4) as executor:
                # Submeter tarefas
                future_to_indicator = {
                    executor.submit(self.get_data, indicator, start_date, end_date, force_update): indicator
                    for indicator in indicators
                }
                
                # Coletar resultados
                for future in as_completed(future_to_indicator):
                    indicator = future_to_indicator[future]
                    try:
                        results[indicator] = future.result()
                    except Exception as e:
                        self.logger.error(f"Erro ao coletar {indicator}: {e}")
                        results[indicator] = pd.DataFrame(columns=['data', 'valor'])
        else:
            # Coleta sequencial
            for indicator in indicators:
                try:
                    results[indicator] = self.get_data(indicator, start_date, end_date, force_update)
                except Exception as e:
                    self.logger.error(f"Erro ao coletar {indicator}: {e}")
                    results[indicator] = pd.DataFrame(columns=['data', 'valor'])
        
        self.logger.info(f"Coleta concluída para {len(indicators)} indicadores")
        return results
    
    def _load_cached_data(self, indicator: str, start_date: datetime, 
                         end_date: datetime) -> Optional[pd.DataFrame]:
        """
        Carrega dados do cache local.
        
        Args:
            indicator: Código do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados ou None se não disponível
        """
        try:
            cache_file = os.path.join(self.data_dir, f"{indicator}.json")
            
            if not os.path.exists(cache_file):
                return None
            
            # Verificar se o arquivo não é muito antigo (1 hora)
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
            if file_age > timedelta(hours=1):
                self.logger.debug(f"Cache expirado para {indicator}")
                return None
            
            # Carregar dados
            df = pd.read_json(cache_file)
            df['data'] = pd.to_datetime(df['data'])
            
            # Filtrar por intervalo de datas
            mask = (df['data'] >= start_date) & (df['data'] <= end_date)
            filtered_df = df[mask].reset_index(drop=True)
            
            if not filtered_df.empty:
                self.logger.debug(f"Cache hit para {indicator}: {len(filtered_df)} registros")
                return filtered_df
            
        except Exception as e:
            self.logger.warning(f"Erro ao carregar cache para {indicator}: {e}")
        
        return None
    
    def _save_cached_data(self, indicator: str, df: pd.DataFrame) -> None:
        """
        Salva dados no cache local.
        
        Args:
            indicator: Código do indicador
            df: DataFrame com dados
        """
        try:
            if df.empty:
                return
            
            cache_file = os.path.join(self.data_dir, f"{indicator}.json")
            
            # Converter para formato JSON-friendly
            df_copy = df.copy()
            df_copy['data'] = df_copy['data'].dt.strftime('%Y-%m-%d')
            
            # Salvar arquivo
            df_copy.to_json(cache_file, orient='records', date_format='iso')
            
            self.logger.debug(f"Dados salvos no cache para {indicator}: {len(df)} registros")
            
        except Exception as e:
            self.logger.warning(f"Erro ao salvar cache para {indicator}: {e}")
    
    def _load_fallback_data(self, indicator: str, start_date: datetime, 
                           end_date: datetime) -> pd.DataFrame:
        """
        Carrega dados de fallback (dados históricos).
        
        Args:
            indicator: Código do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados de fallback
        """
        try:
            fallback_file = os.path.join(self.data_dir, f"{indicator}.json")
            
            if os.path.exists(fallback_file):
                df = pd.read_json(fallback_file)
                df['data'] = pd.to_datetime(df['data'])
                
                # Filtrar por intervalo
                mask = (df['data'] >= start_date) & (df['data'] <= end_date)
                filtered_df = df[mask].reset_index(drop=True)
                
                if not filtered_df.empty:
                    self.logger.info(f"Dados de fallback carregados para {indicator}")
                    return filtered_df
            
        except Exception as e:
            self.logger.warning(f"Erro ao carregar fallback para {indicator}: {e}")
        
        # Retornar DataFrame vazio se não há fallback
        return pd.DataFrame(columns=['data', 'valor'])
    
    def get_indicator_metadata(self, indicator: str) -> Dict[str, Any]:
        """
        Obtém metadados de um indicador.
        
        Args:
            indicator: Código do indicador
            
        Returns:
            Dicionário com metadados
        """
        if indicator in self._metadata_cache:
            return self._metadata_cache[indicator]
        
        if indicator not in self.INDICATOR_MAPPING:
            raise ValueError(f"Indicador '{indicator}' não suportado")
        
        source = self.INDICATOR_MAPPING[indicator]
        connector = self.connectors.get(source)
        
        if connector is None:
            metadata = {
                'indicator': indicator,
                'source': source,
                'error': 'Conector não disponível'
            }
        else:
            try:
                metadata = connector.get_indicator_metadata(indicator)
            except Exception as e:
                metadata = {
                    'indicator': indicator,
                    'source': source,
                    'error': str(e)
                }
        
        # Cache metadata
        self._metadata_cache[indicator] = metadata
        return metadata
    
    def validate_data_quality(self, indicator: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida a qualidade dos dados de um indicador.
        
        Args:
            indicator: Código do indicador
            df: DataFrame com dados
            
        Returns:
            Dicionário com métricas de qualidade
        """
        if indicator not in self.INDICATOR_MAPPING:
            return {'valid': False, 'error': 'Indicador não suportado'}
        
        source = self.INDICATOR_MAPPING[indicator]
        connector = self.connectors.get(source)
        
        if connector is None or not hasattr(connector, 'validate_data_quality'):
            # Validação básica
            return self._basic_data_validation(df, indicator)
        
        try:
            return connector.validate_data_quality(df, indicator)
        except Exception as e:
            self.logger.error(f"Erro na validação de {indicator}: {e}")
            return {'valid': False, 'error': str(e)}
    
    def _basic_data_validation(self, df: pd.DataFrame, indicator: str) -> Dict[str, Any]:
        """
        Validação básica de qualidade de dados.
        
        Args:
            df: DataFrame com dados
            indicator: Código do indicador
            
        Returns:
            Dicionário com métricas básicas
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
        
        # Verificar colunas obrigatórias
        required_columns = ['data', 'valor']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Colunas obrigatórias ausentes: {missing_columns}")
        
        # Verificar valores nulos
        if 'valor' in df.columns:
            null_count = df['valor'].isnull().sum()
            if null_count > 0:
                warnings.append(f"{null_count} valores nulos encontrados")
        
        # Verificar tipos de dados
        if 'data' in df.columns:
            try:
                pd.to_datetime(df['data'])
            except:
                errors.append("Coluna 'data' contém valores inválidos")
        
        metrics = {
            'total_records': len(df),
            'null_count': null_count if 'valor' in df.columns else 0
        }
        
        if 'data' in df.columns and not df.empty:
            metrics['date_range'] = {
                'start': df['data'].min(),
                'end': df['data'].max()
            }
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'metrics': metrics
        }
    
    def update_all_indicators(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Atualiza todos os indicadores disponíveis.
        
        Args:
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            Relatório de atualização
        """
        self.logger.info("Iniciando atualização de todos os indicadores")
        
        indicators = list(self.INDICATOR_MAPPING.keys())
        results = self.get_multiple_indicators(indicators, start_date, end_date, force_update=True)
        
        # Gerar relatório
        report = {
            'timestamp': datetime.now().isoformat(),
            'period': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat()
            },
            'indicators': {},
            'summary': {
                'total': len(indicators),
                'success': 0,
                'failed': 0,
                'total_records': 0
            }
        }
        
        for indicator, df in results.items():
            if not df.empty:
                report['indicators'][indicator] = {
                    'status': 'success',
                    'records': len(df),
                    'date_range': {
                        'start': df['data'].min().isoformat(),
                        'end': df['data'].max().isoformat()
                    }
                }
                report['summary']['success'] += 1
                report['summary']['total_records'] += len(df)
            else:
                report['indicators'][indicator] = {
                    'status': 'failed',
                    'records': 0,
                    'error': 'Nenhum dado coletado'
                }
                report['summary']['failed'] += 1
        
        self.logger.info(f"Atualização concluída: {report['summary']['success']}/{report['summary']['total']} indicadores")
        return report
    
    def clear_cache(self) -> None:
        """
        Limpa todo o cache de dados.
        """
        try:
            for filename in os.listdir(self.data_dir):
                if filename.endswith('.json'):
                    file_path = os.path.join(self.data_dir, filename)
                    os.remove(file_path)
            
            # Limpar cache dos conectores
            for connector in self.connectors.values():
                if connector is not None and hasattr(connector, 'clear_cache'):
                    connector.clear_cache()
            
            self.logger.info("Cache limpo com sucesso")
            
        except Exception as e:
            self.logger.error(f"Erro ao limpar cache: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        Obtém status do sistema de coleta de dados.
        
        Returns:
            Dicionário com status de todos os componentes
        """
        status = {
            'timestamp': datetime.now().isoformat(),
            'connectors': {},
            'data_directory': self.data_dir,
            'parallel_enabled': self.enable_parallel
        }
        
        # Testar conectividade de cada conector
        for source, connector in self.connectors.items():
            if connector is None:
                status['connectors'][source] = {
                    'available': False,
                    'error': 'Conector não inicializado'
                }
            else:
                try:
                    # Testar conectividade
                    is_connected = connector.test_connection() if hasattr(connector, 'test_connection') else True
                    
                    status['connectors'][source] = {
                        'available': True,
                        'connected': is_connected,
                        'indicators': connector.get_available_indicators()
                    }
                    
                    # Adicionar estatísticas de cache se disponível
                    if hasattr(connector, 'get_cache_stats'):
                        status['connectors'][source]['cache_stats'] = connector.get_cache_stats()
                        
                except Exception as e:
                    status['connectors'][source] = {
                        'available': True,
                        'connected': False,
                        'error': str(e)
                    }
        
        return status

