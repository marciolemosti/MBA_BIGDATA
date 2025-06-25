"""
Conector para APIs do IBGE (Instituto Brasileiro de Geografia e Estatística).

Este módulo implementa a coleta de dados econômicos do IBGE, incluindo:
- IPCA (Índice Nacional de Preços ao Consumidor Amplo)
- PIB (Produto Interno Bruto)
- Taxa de Desemprego (PNAD Contínua)

Autor: Márcio Lemos
Data: 2025-06-23
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from . import APIConnectorBase


class IBGEConnector(APIConnectorBase):
    """
    Conector para APIs do IBGE.
    
    Implementa coleta de dados dos principais indicadores econômicos
    disponibilizados pelo IBGE através de suas APIs públicas.
    """
    
    # URLs das APIs do IBGE
    SIDRA_BASE_URL = "https://apisidra.ibge.gov.br/values"
    
    # Códigos das tabelas SIDRA para cada indicador
    INDICATOR_TABLES = {
        'ipca': {
            'table': '1737',  # IPCA - Variação mensal
            'variable': '63',  # Variação mensal (%)
            'classification': '315|7169',  # Brasil
            'period': 'all'
        },
        'pib': {
            'table': '1621',  # PIB - Valores correntes
            'variable': '584',  # PIB (Milhões de Reais)
            'classification': '11255|90707',  # Brasil
            'period': 'all'
        },
        'desemprego': {
            'table': '4099',  # PNAD Contínua - Taxa de desocupação
            'variable': '4099',  # Taxa de desocupação (%)
            'classification': '1|1',  # Brasil
            'period': 'all'
        }
    }
    
    def __init__(self):
        """
        Inicializa o conector do IBGE.
        """
        super().__init__(
            base_url=self.SIDRA_BASE_URL,
            timeout=30,
            max_retries=3
        )
        
        # Cache mais longo para dados do IBGE (dados históricos mudam pouco)
        self.default_cache_duration = 7200  # 2 horas
        
        self.logger.info("Conector IBGE inicializado com sucesso")
    
    def get_available_indicators(self) -> List[str]:
        """
        Retorna lista de indicadores disponíveis no IBGE.
        
        Returns:
            Lista de códigos de indicadores disponíveis
        """
        return list(self.INDICATOR_TABLES.keys())
    
    def _build_sidra_params(self, indicator: str, start_date: Optional[datetime] = None,
                           end_date: Optional[datetime] = None) -> Dict[str, str]:
        """
        Constrói parâmetros para consulta à API SIDRA.
        
        Args:
            indicator: Código do indicador
            start_date: Data inicial (opcional)
            end_date: Data final (opcional)
            
        Returns:
            Dicionário com parâmetros da consulta
        """
        if indicator not in self.INDICATOR_TABLES:
            raise ValueError(f"Indicador '{indicator}' não suportado. "
                           f"Disponíveis: {list(self.INDICATOR_TABLES.keys())}")
        
        config = self.INDICATOR_TABLES[indicator]
        
        params = {
            't': config['table'],
            'v': config['variable'],
            'p': config['period'],
            'c': config['classification'],
            'f': 'json'
        }
        
        # Ajustar período se datas forem especificadas
        if start_date and end_date:
            # SIDRA usa formato YYYYMM para períodos mensais
            if indicator == 'pib':
                # PIB é trimestral - usar formato YYYYQQ
                start_period = f"{start_date.year}{(start_date.month-1)//3 + 1:02d}"
                end_period = f"{end_date.year}{(end_date.month-1)//3 + 1:02d}"
            else:
                # IPCA e Desemprego são mensais
                start_period = f"{start_date.year}{start_date.month:02d}"
                end_period = f"{end_date.year}{end_date.month:02d}"
            
            params['p'] = f"{start_period}-{end_period}"
        
        return params
    
    def _parse_sidra_response(self, data: List[Dict], indicator: str) -> pd.DataFrame:
        """
        Converte resposta da API SIDRA em DataFrame padronizado.
        
        Args:
            data: Dados da resposta JSON
            indicator: Código do indicador
            
        Returns:
            DataFrame com colunas 'data' e 'valor'
        """
        if not data or len(data) < 2:
            self.logger.warning(f"Resposta vazia ou inválida para {indicator}")
            return pd.DataFrame(columns=['data', 'valor'])
        
        # Primeira linha contém metadados, dados começam na segunda linha
        records = data[1:]
        
        df_data = []
        for record in records:
            try:
                # Extrair período e valor
                periodo = record.get('D2C', record.get('D1C', ''))
                valor_str = record.get('V', '').replace(',', '.')
                
                if not periodo or not valor_str or valor_str in ['...', '-', '']:
                    continue
                
                # Converter período para data
                data_obj = self._parse_period(periodo, indicator)
                if data_obj is None:
                    continue
                
                # Converter valor para float
                try:
                    valor = float(valor_str)
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
        
        self.logger.info(f"Processados {len(df)} registros para {indicator}")
        return df
    
    def _parse_period(self, periodo: str, indicator: str) -> Optional[datetime]:
        """
        Converte período do SIDRA para objeto datetime.
        
        Args:
            periodo: String do período (ex: "202312", "202304")
            indicator: Código do indicador
            
        Returns:
            Objeto datetime ou None se inválido
        """
        try:
            if len(periodo) == 6:  # YYYYMM
                year = int(periodo[:4])
                month = int(periodo[4:6])
                return datetime(year, month, 1)
            elif len(periodo) == 5:  # YYYYQ (trimestral)
                year = int(periodo[:4])
                quarter = int(periodo[4:5])
                month = (quarter - 1) * 3 + 1
                return datetime(year, month, 1)
            else:
                self.logger.warning(f"Formato de período não reconhecido: {periodo}")
                return None
        except (ValueError, IndexError) as e:
            self.logger.warning(f"Erro ao converter período {periodo}: {e}")
            return None
    
    def get_data(self, indicator: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Obtém dados de um indicador específico do IBGE.
        
        Args:
            indicator: Código do indicador ('ipca', 'pib', 'desemprego')
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com colunas 'data' e 'valor'
            
        Raises:
            ValueError: Se o indicador não for suportado
            requests.RequestException: Em caso de erro na API
        """
        self.logger.info(f"Coletando dados do IBGE para {indicator}")
        
        # Validar parâmetros
        self._validate_date_range(start_date, end_date)
        
        if indicator not in self.INDICATOR_TABLES:
            raise ValueError(f"Indicador '{indicator}' não suportado pelo IBGE. "
                           f"Disponíveis: {list(self.INDICATOR_TABLES.keys())}")
        
        try:
            # Construir parâmetros da consulta
            params = self._build_sidra_params(indicator, start_date, end_date)
            
            # Fazer requisição
            response_data = self._make_request(
                endpoint="",  # SIDRA usa parâmetros na URL base
                params=params,
                cache_ttl=self.default_cache_duration
            )
            
            # Processar resposta
            df = self._parse_sidra_response(response_data, indicator)
            
            # Filtrar por intervalo de datas se necessário
            if not df.empty:
                mask = (df['data'] >= start_date) & (df['data'] <= end_date)
                df = df[mask].reset_index(drop=True)
            
            self.logger.info(f"Coletados {len(df)} registros do IBGE para {indicator}")
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao coletar dados do IBGE para {indicator}: {e}")
            
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
    
    def get_latest_data(self, indicator: str, months: int = 12) -> pd.DataFrame:
        """
        Obtém os dados mais recentes de um indicador.
        
        Args:
            indicator: Código do indicador
            months: Número de meses para buscar (padrão: 12)
            
        Returns:
            DataFrame com os dados mais recentes
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=months * 30)
        
        return self.get_data(indicator, start_date, end_date)
    
    def get_indicator_metadata(self, indicator: str) -> Dict[str, Any]:
        """
        Obtém metadados de um indicador.
        
        Args:
            indicator: Código do indicador
            
        Returns:
            Dicionário com metadados do indicador
        """
        if indicator not in self.INDICATOR_TABLES:
            raise ValueError(f"Indicador '{indicator}' não suportado")
        
        config = self.INDICATOR_TABLES[indicator]
        
        metadata = {
            'indicator': indicator,
            'source': 'IBGE',
            'table': config['table'],
            'variable': config['variable'],
            'frequency': 'monthly' if indicator in ['ipca', 'desemprego'] else 'quarterly',
            'unit': self._get_indicator_unit(indicator),
            'description': self._get_indicator_description(indicator)
        }
        
        return metadata
    
    def _get_indicator_unit(self, indicator: str) -> str:
        """
        Retorna a unidade de medida do indicador.
        
        Args:
            indicator: Código do indicador
            
        Returns:
            String com a unidade de medida
        """
        units = {
            'ipca': '% a.m.',
            'pib': 'Milhões de R$',
            'desemprego': '%'
        }
        return units.get(indicator, 'N/A')
    
    def _get_indicator_description(self, indicator: str) -> str:
        """
        Retorna a descrição do indicador.
        
        Args:
            indicator: Código do indicador
            
        Returns:
            String com a descrição do indicador
        """
        descriptions = {
            'ipca': 'Índice Nacional de Preços ao Consumidor Amplo - Variação Mensal',
            'pib': 'Produto Interno Bruto - Valores Correntes',
            'desemprego': 'Taxa de Desocupação - PNAD Contínua'
        }
        return descriptions.get(indicator, 'N/A')

