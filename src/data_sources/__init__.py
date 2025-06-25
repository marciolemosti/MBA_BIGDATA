"""
Módulo base para conectores de APIs de dados econômicos.

Este módulo fornece a classe base e utilitários comuns para todos os conectores
de APIs utilizados no sistema de coleta de dados econômicos.

Autor: Márcio Lemos
Data: 2025-06-23
"""

import time
import logging
import requests
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class APIConnectorBase(ABC):
    """
    Classe base abstrata para conectores de APIs de dados econômicos.
    
    Esta classe fornece funcionalidades comuns como:
    - Tratamento de rate limiting
    - Sistema de retry automático
    - Cache inteligente
    - Logging estruturado
    - Validação de dados
    """
    
    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3):
        """
        Inicializa o conector base.
        
        Args:
            base_url: URL base da API
            timeout: Timeout para requisições em segundos
            max_retries: Número máximo de tentativas em caso de falha
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Configurar sessão HTTP com retry automático
        self.session = self._setup_session()
        
        # Cache interno para reduzir chamadas desnecessárias
        self._cache = {}
        self._cache_ttl = {}
        self.default_cache_duration = 3600  # 1 hora em segundos
        
        self.logger.info(f"Conector {self.__class__.__name__} inicializado")
    
    def _setup_session(self) -> requests.Session:
        """
        Configura a sessão HTTP com retry automático e timeouts.
        
        Returns:
            Sessão HTTP configurada
        """
        session = requests.Session()
        
        # Configurar estratégia de retry
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,  # Backoff exponencial
            status_forcelist=[429, 500, 502, 503, 504],  # Status codes para retry
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Headers padrão
        session.headers.update({
            'User-Agent': 'MBA-BigData-Dashboard/1.0 (Economic Data Collector)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
        
        return session
    
    def _get_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """
        Gera chave única para cache baseada no endpoint e parâmetros.
        
        Args:
            endpoint: Endpoint da API
            params: Parâmetros da requisição
            
        Returns:
            Chave única para cache
        """
        # Ordenar parâmetros para garantir consistência
        sorted_params = sorted(params.items()) if params else []
        params_str = "&".join([f"{k}={v}" for k, v in sorted_params])
        return f"{endpoint}?{params_str}"
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """
        Verifica se o cache ainda é válido.
        
        Args:
            cache_key: Chave do cache
            
        Returns:
            True se o cache é válido, False caso contrário
        """
        if cache_key not in self._cache_ttl:
            return False
        
        return datetime.now() < self._cache_ttl[cache_key]
    
    def _set_cache(self, cache_key: str, data: Any, ttl_seconds: Optional[int] = None) -> None:
        """
        Armazena dados no cache com TTL.
        
        Args:
            cache_key: Chave do cache
            data: Dados para armazenar
            ttl_seconds: Tempo de vida em segundos (usa padrão se None)
        """
        ttl = ttl_seconds or self.default_cache_duration
        self._cache[cache_key] = data
        self._cache_ttl[cache_key] = datetime.now() + timedelta(seconds=ttl)
        
        self.logger.debug(f"Cache atualizado para {cache_key}, TTL: {ttl}s")
    
    def _get_cache(self, cache_key: str) -> Optional[Any]:
        """
        Recupera dados do cache se válidos.
        
        Args:
            cache_key: Chave do cache
            
        Returns:
            Dados do cache ou None se inválido/inexistente
        """
        if self._is_cache_valid(cache_key):
            self.logger.debug(f"Cache hit para {cache_key}")
            return self._cache[cache_key]
        
        # Limpar cache expirado
        if cache_key in self._cache:
            del self._cache[cache_key]
            del self._cache_ttl[cache_key]
            self.logger.debug(f"Cache expirado removido para {cache_key}")
        
        return None
    
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None,
                     cache_ttl: Optional[int] = None) -> Dict[str, Any]:
        """
        Executa requisição HTTP com cache e tratamento de erros.
        
        Args:
            endpoint: Endpoint da API (relativo à base_url)
            params: Parâmetros da requisição
            cache_ttl: Tempo de vida do cache em segundos
            
        Returns:
            Dados da resposta JSON
            
        Raises:
            requests.RequestException: Em caso de erro na requisição
            ValueError: Em caso de resposta inválida
        """
        params = params or {}
        cache_key = self._get_cache_key(endpoint, params)
        
        # Verificar cache primeiro
        cached_data = self._get_cache(cache_key)
        if cached_data is not None:
            return cached_data
        
        # Construir URL completa
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            self.logger.info(f"Fazendo requisição para {url}")
            self.logger.debug(f"Parâmetros: {params}")
            
            # Executar requisição
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            # Verificar se a resposta é JSON válido
            try:
                data = response.json()
            except ValueError as e:
                self.logger.error(f"Resposta não é JSON válido: {e}")
                raise ValueError(f"API retornou resposta inválida: {response.text[:200]}")
            
            # Armazenar no cache
            self._set_cache(cache_key, data, cache_ttl)
            
            self.logger.info(f"Requisição bem-sucedida para {url}")
            return data
            
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout na requisição para {url}")
            raise
        except requests.exceptions.ConnectionError:
            self.logger.error(f"Erro de conexão para {url}")
            raise
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Erro HTTP {e.response.status_code} para {url}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado na requisição para {url}: {e}")
            raise
    
    def _validate_date_range(self, start_date: datetime, end_date: datetime) -> None:
        """
        Valida intervalo de datas.
        
        Args:
            start_date: Data inicial
            end_date: Data final
            
        Raises:
            ValueError: Se o intervalo for inválido
        """
        if start_date > end_date:
            raise ValueError("Data inicial não pode ser posterior à data final")
        
        if end_date > datetime.now():
            self.logger.warning("Data final é futura, ajustando para hoje")
            end_date = datetime.now()
        
        # Verificar se o intervalo não é muito grande (proteção contra sobrecarga)
        max_days = 3650  # ~10 anos
        if (end_date - start_date).days > max_days:
            raise ValueError(f"Intervalo muito grande. Máximo permitido: {max_days} dias")
    
    def _format_date(self, date: Union[datetime, str], format_str: str = "%Y-%m-%d") -> str:
        """
        Formata data para string no formato especificado.
        
        Args:
            date: Data para formatar
            format_str: Formato de saída
            
        Returns:
            Data formatada como string
        """
        if isinstance(date, str):
            return date
        return date.strftime(format_str)
    
    def _parse_date(self, date_str: str, format_str: str = "%Y-%m-%d") -> datetime:
        """
        Converte string de data para objeto datetime.
        
        Args:
            date_str: String da data
            format_str: Formato da string
            
        Returns:
            Objeto datetime
        """
        return datetime.strptime(date_str, format_str)
    
    def clear_cache(self) -> None:
        """
        Limpa todo o cache armazenado.
        """
        self._cache.clear()
        self._cache_ttl.clear()
        self.logger.info("Cache limpo")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do cache.
        
        Returns:
            Dicionário com estatísticas do cache
        """
        total_entries = len(self._cache)
        valid_entries = sum(1 for key in self._cache.keys() if self._is_cache_valid(key))
        
        return {
            'total_entries': total_entries,
            'valid_entries': valid_entries,
            'expired_entries': total_entries - valid_entries,
            'cache_keys': list(self._cache.keys())
        }
    
    @abstractmethod
    def get_data(self, indicator: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Método abstrato para obter dados de um indicador específico.
        
        Args:
            indicator: Nome do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com os dados do indicador
        """
        pass
    
    @abstractmethod
    def get_available_indicators(self) -> List[str]:
        """
        Método abstrato para obter lista de indicadores disponíveis.
        
        Returns:
            Lista de indicadores disponíveis
        """
        pass
    
    def test_connection(self) -> bool:
        """
        Testa a conectividade com a API.
        
        Returns:
            True se a conexão for bem-sucedida, False caso contrário
        """
        try:
            # Tentar uma requisição simples para testar conectividade
            response = self.session.get(self.base_url, timeout=10)
            return response.status_code < 500
        except Exception as e:
            self.logger.error(f"Teste de conexão falhou: {e}")
            return False

