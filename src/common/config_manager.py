"""
Sistema de Gerenciamento de Configurações
Módulo responsável pela centralização e gestão de configurações do sistema.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigurationManager:
    """
    Gerenciador centralizado de configurações do sistema.
    
    Esta classe implementa o padrão Singleton para garantir uma única
    instância de configuração em toda a aplicação, seguindo as melhores
    práticas de arquitetura de software empresarial.
    """
    
    _instance = None
    _config_data = None
    
    def __new__(cls):
        """Implementação do padrão Singleton."""
        if cls._instance is None:
            cls._instance = super(ConfigurationManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Inicializa o gerenciador de configurações."""
        if self._config_data is None:
            self._load_configuration()
    
    def _load_configuration(self) -> None:
        """
        Carrega as configurações do arquivo YAML.
        
        Raises:
            FileNotFoundError: Quando o arquivo de configuração não é encontrado
            yaml.YAMLError: Quando há erro na estrutura do arquivo YAML
        """
        try:
            config_path = self._get_config_path()
            
            with open(config_path, 'r', encoding='utf-8') as config_file:
                self._config_data = yaml.safe_load(config_file)
                
            logging.info(f"Configurações carregadas com sucesso: {config_path}")
            
        except FileNotFoundError:
            logging.error("Arquivo de configuração não encontrado")
            self._config_data = self._get_default_config()
            
        except yaml.YAMLError as e:
            logging.error(f"Erro ao processar arquivo YAML: {e}")
            self._config_data = self._get_default_config()
    
    def _get_config_path(self) -> Path:
        """
        Determina o caminho do arquivo de configuração.
        
        Returns:
            Path: Caminho para o arquivo de configuração
        """
        project_root = Path(__file__).parent.parent.parent
        return project_root / "config" / "application.yaml"
    
    def _get_default_config(self) -> Dict[str, Any]:
        """
        Retorna configurações padrão do sistema.
        
        Returns:
            Dict[str, Any]: Dicionário com configurações padrão
        """
        return {
            "application": {
                "name": "Dashboard Econômico",
                "version": "1.0.0",
                "author": "Márcio Lemos",
                "description": "Sistema de Análise de Indicadores Econômicos"
            },
            "database": {
                "cache_ttl": 3600,
                "data_refresh_interval": 1800
            },
            "analytics": {
                "forecast_horizon_months": 24,
                "confidence_interval": 0.95,
                "seasonal_periods": 12
            },
            "interface": {
                "theme": "professional",
                "layout": "wide",
                "sidebar_state": "expanded"
            }
        }
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Recupera valor de configuração usando notação de ponto.
        
        Args:
            key_path (str): Caminho da chave (ex: 'database.cache_ttl')
            default (Any): Valor padrão se a chave não existir
            
        Returns:
            Any: Valor da configuração ou valor padrão
        """
        keys = key_path.split('.')
        value = self._config_data
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Recupera uma seção completa de configuração.
        
        Args:
            section (str): Nome da seção
            
        Returns:
            Dict[str, Any]: Dicionário com a seção de configuração
        """
        return self._config_data.get(section, {})
    
    def update(self, key_path: str, value: Any) -> None:
        """
        Atualiza valor de configuração em tempo de execução.
        
        Args:
            key_path (str): Caminho da chave
            value (Any): Novo valor
        """
        keys = key_path.split('.')
        config_section = self._config_data
        
        for key in keys[:-1]:
            if key not in config_section:
                config_section[key] = {}
            config_section = config_section[key]
        
        config_section[keys[-1]] = value
        logging.info(f"Configuração atualizada: {key_path} = {value}")
    
    def reload(self) -> None:
        """Recarrega as configurações do arquivo."""
        self._config_data = None
        self._load_configuration()
    
    @property
    def all_configs(self) -> Dict[str, Any]:
        """
        Retorna todas as configurações carregadas.
        
        Returns:
            Dict[str, Any]: Dicionário completo de configurações
        """
        return self._config_data.copy()


# Instância global do gerenciador de configurações
config_manager = ConfigurationManager()

