"""
Gerenciador de Dados Econômicos
Módulo central para gerenciamento e acesso aos dados econômicos.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass

from common.logger import get_logger
from common.config_manager import config_manager
from common.validators import EconomicDataValidator, BusinessRuleValidator
from data_services.cache_service import CacheService

logger = get_logger("data_manager")


@dataclass
class IndicatorMetadata:
    """Metadados de um indicador econômico."""
    name: str
    unit: str
    description: str
    source: str
    frequency: str
    color: str
    last_update: Optional[datetime] = None
    total_records: int = 0


class EconomicDataManager:
    """
    Gerenciador central de dados econômicos.
    
    Esta classe implementa o padrão Repository para acesso aos dados,
    fornecendo uma interface unificada para operações de leitura,
    validação e transformação de dados econômicos.
    """
    
    def __init__(self):
        """Inicializa o gerenciador de dados."""
        self.cache_service = CacheService()
        self.data_directory = self._get_data_directory()
        self.indicators_config = self._load_indicators_config()
        self._data_cache = {}
        
        logger.info("Gerenciador de dados econômicos inicializado")
    
    def _get_data_directory(self) -> Path:
        """
        Determina o diretório de dados.
        
        Returns:
            Path: Caminho para o diretório de dados
        """
        project_root = Path(__file__).parent.parent.parent
        return project_root / "data"
    
    def _load_indicators_config(self) -> Dict[str, Dict[str, Any]]:
        """
        Carrega configurações dos indicadores.
        
        Returns:
            Dict[str, Dict[str, Any]]: Configurações dos indicadores
        """
        return config_manager.get_section("indicators")
    
    def get_available_indicators(self) -> List[str]:
        """
        Retorna lista de indicadores disponíveis.
        
        Returns:
            List[str]: Lista de códigos dos indicadores
        """
        try:
            available = []
            
            for indicator_code in self.indicators_config.keys():
                data_file = self.data_directory / f"{indicator_code}.json"
                if data_file.exists():
                    available.append(indicator_code)
            
            logger.info(f"Indicadores disponíveis: {available}")
            return available
            
        except Exception as e:
            logger.error(f"Erro ao listar indicadores disponíveis: {e}")
            return []
    
    def get_indicator_metadata(self, indicator_code: str) -> Optional[IndicatorMetadata]:
        """
        Retorna metadados de um indicador.
        
        Args:
            indicator_code (str): Código do indicador
            
        Returns:
            Optional[IndicatorMetadata]: Metadados do indicador
        """
        try:
            if indicator_code not in self.indicators_config:
                logger.warning(f"Indicador não configurado: {indicator_code}")
                return None
            
            config = self.indicators_config[indicator_code]
            
            # Carregar dados para obter estatísticas
            df = self.load_indicator_data(indicator_code)
            
            metadata = IndicatorMetadata(
                name=config.get("nome", indicator_code),
                unit=config.get("unidade", ""),
                description=config.get("descricao", ""),
                source=config.get("fonte", "Não especificada"),
                frequency=config.get("frequencia", "Mensal"),
                color=config.get("cor", "#000000"),
                last_update=df['data'].max() if df is not None and not df.empty else None,
                total_records=len(df) if df is not None else 0
            )
            
            return metadata
            
        except Exception as e:
            logger.error(f"Erro ao obter metadados do indicador {indicator_code}: {e}")
            return None
    
    def load_indicator_data(self, indicator_code: str, validate: bool = True) -> Optional[pd.DataFrame]:
        """
        Carrega dados de um indicador específico.
        
        Args:
            indicator_code (str): Código do indicador
            validate (bool): Se deve validar os dados
            
        Returns:
            Optional[pd.DataFrame]: DataFrame com os dados ou None se erro
        """
        try:
            # Verificar cache primeiro
            cache_key = f"indicator_data_{indicator_code}"
            cached_data = self.cache_service.get(cache_key)
            
            if cached_data is not None:
                logger.debug(f"Dados do indicador {indicator_code} obtidos do cache")
                return cached_data
            
            # Carregar do arquivo
            data_file = self.data_directory / f"{indicator_code}.json"
            
            if not data_file.exists():
                logger.warning(f"Arquivo de dados não encontrado: {data_file}")
                return None
            
            with open(data_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Converter para DataFrame
            df = pd.DataFrame(data)
            
            if df.empty:
                logger.warning(f"Dados vazios para indicador: {indicator_code}")
                return None
            
            # Garantir colunas padrão
            if 'data' not in df.columns or 'valor' not in df.columns:
                logger.error(f"Colunas obrigatórias ausentes no indicador {indicator_code}")
                return None
            
            # Converter tipos
            df['data'] = pd.to_datetime(df['data'])
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            
            # Ordenar por data
            df = df.sort_values('data').reset_index(drop=True)
            
            # Validar dados se solicitado
            if validate:
                is_valid, errors = EconomicDataValidator.validate_dataframe(df, indicator_code)
                
                if not is_valid:
                    logger.warning(f"Dados do indicador {indicator_code} contêm erros: {errors}")
                    # Sanitizar dados
                    df = EconomicDataValidator.sanitize_dataframe(df, indicator_code)
            
            # Armazenar no cache
            cache_ttl = config_manager.get("cache.ttl_dados", 1800)
            self.cache_service.set(cache_key, df, ttl=cache_ttl)
            
            logger.info(f"Dados do indicador {indicator_code} carregados: {len(df)} registros")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados do indicador {indicator_code}: {e}")
            return None
    
    def get_indicator_summary(self, indicator_code: str) -> Optional[Dict[str, Any]]:
        """
        Retorna resumo estatístico de um indicador.
        
        Args:
            indicator_code (str): Código do indicador
            
        Returns:
            Optional[Dict[str, Any]]: Resumo estatístico
        """
        try:
            df = self.load_indicator_data(indicator_code)
            
            if df is None or df.empty:
                return None
            
            values = df['valor'].dropna()
            
            summary = {
                'total_records': len(df),
                'valid_records': len(values),
                'date_range': {
                    'start': df['data'].min().strftime('%Y-%m-%d'),
                    'end': df['data'].max().strftime('%Y-%m-%d')
                },
                'statistics': {
                    'mean': float(values.mean()),
                    'median': float(values.median()),
                    'std': float(values.std()),
                    'min': float(values.min()),
                    'max': float(values.max()),
                    'q25': float(values.quantile(0.25)),
                    'q75': float(values.quantile(0.75))
                },
                'latest_value': {
                    'date': df.iloc[-1]['data'].strftime('%Y-%m-%d'),
                    'value': float(df.iloc[-1]['valor'])
                }
            }
            
            # Adicionar tendência recente (últimos 12 meses)
            recent_data = df[df['data'] >= (df['data'].max() - timedelta(days=365))]
            if len(recent_data) >= 2:
                trend = np.polyfit(range(len(recent_data)), recent_data['valor'], 1)[0]
                summary['recent_trend'] = {
                    'slope': float(trend),
                    'direction': 'crescente' if trend > 0 else 'decrescente' if trend < 0 else 'estável'
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Erro ao gerar resumo do indicador {indicator_code}: {e}")
            return None
    
    def get_multiple_indicators(self, indicator_codes: List[str], 
                              start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None) -> Dict[str, pd.DataFrame]:
        """
        Carrega dados de múltiplos indicadores.
        
        Args:
            indicator_codes (List[str]): Lista de códigos dos indicadores
            start_date (Optional[datetime]): Data inicial do filtro
            end_date (Optional[datetime]): Data final do filtro
            
        Returns:
            Dict[str, pd.DataFrame]: Dicionário com dados dos indicadores
        """
        results = {}
        
        for code in indicator_codes:
            try:
                df = self.load_indicator_data(code)
                
                if df is not None and not df.empty:
                    # Aplicar filtros de data se especificados
                    if start_date:
                        df = df[df['data'] >= start_date]
                    if end_date:
                        df = df[df['data'] <= end_date]
                    
                    results[code] = df
                    
            except Exception as e:
                logger.error(f"Erro ao carregar indicador {code}: {e}")
                continue
        
        logger.info(f"Carregados {len(results)} de {len(indicator_codes)} indicadores solicitados")
        return results
    
    def get_correlation_matrix(self, indicator_codes: List[str]) -> Optional[pd.DataFrame]:
        """
        Calcula matriz de correlação entre indicadores.
        
        Args:
            indicator_codes (List[str]): Lista de códigos dos indicadores
            
        Returns:
            Optional[pd.DataFrame]: Matriz de correlação
        """
        try:
            # Carregar dados de todos os indicadores
            data_dict = self.get_multiple_indicators(indicator_codes)
            
            if len(data_dict) < 2:
                logger.warning("Necessários pelo menos 2 indicadores para correlação")
                return None
            
            # Criar DataFrame combinado
            combined_data = pd.DataFrame()
            
            for code, df in data_dict.items():
                if not df.empty:
                    df_temp = df[['data', 'valor']].copy()
                    df_temp = df_temp.rename(columns={'valor': code})
                    
                    if combined_data.empty:
                        combined_data = df_temp
                    else:
                        combined_data = pd.merge(combined_data, df_temp, on='data', how='outer')
            
            # Calcular correlação
            correlation_matrix = combined_data.drop('data', axis=1).corr()
            
            logger.info(f"Matriz de correlação calculada para {len(indicator_codes)} indicadores")
            return correlation_matrix
            
        except Exception as e:
            logger.error(f"Erro ao calcular matriz de correlação: {e}")
            return None
    
    def get_data_quality_report(self) -> Dict[str, Any]:
        """
        Gera relatório de qualidade dos dados.
        
        Returns:
            Dict[str, Any]: Relatório de qualidade
        """
        try:
            available_indicators = self.get_available_indicators()
            report = {
                'timestamp': datetime.now().isoformat(),
                'total_indicators': len(available_indicators),
                'indicators': {}
            }
            
            for indicator_code in available_indicators:
                df = self.load_indicator_data(indicator_code, validate=False)
                
                if df is not None:
                    # Executar validação
                    is_valid, errors = EconomicDataValidator.validate_dataframe(df, indicator_code)
                    
                    # Verificar atualização
                    last_update = df['data'].max()
                    is_fresh, freshness_warnings = BusinessRuleValidator.validate_data_freshness(
                        last_update, indicator_code
                    )
                    
                    report['indicators'][indicator_code] = {
                        'is_valid': is_valid,
                        'validation_errors': errors,
                        'is_fresh': is_fresh,
                        'freshness_warnings': freshness_warnings,
                        'total_records': len(df),
                        'null_values': df['valor'].isnull().sum(),
                        'last_update': last_update.isoformat() if last_update else None
                    }
            
            # Estatísticas gerais
            valid_indicators = sum(1 for ind in report['indicators'].values() if ind['is_valid'])
            fresh_indicators = sum(1 for ind in report['indicators'].values() if ind['is_fresh'])
            
            report['summary'] = {
                'valid_indicators': valid_indicators,
                'fresh_indicators': fresh_indicators,
                'quality_score': (valid_indicators + fresh_indicators) / (2 * len(available_indicators)) if available_indicators else 0
            }
            
            logger.info(f"Relatório de qualidade gerado: {valid_indicators}/{len(available_indicators)} indicadores válidos")
            return report
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatório de qualidade: {e}")
            return {}
    
    def refresh_cache(self) -> None:
        """Limpa o cache de dados forçando recarregamento."""
        try:
            self.cache_service.clear_pattern("indicator_data_*")
            self._data_cache.clear()
            logger.info("Cache de dados limpo com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao limpar cache: {e}")


# Instância global do gerenciador de dados
data_manager = EconomicDataManager()

