"""
Sistema de Logging Empresarial
Módulo responsável pela configuração e gestão de logs do sistema.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


class EnterpriseLogger:
    """
    Sistema de logging empresarial com rotação de arquivos e múltiplos handlers.
    
    Implementa as melhores práticas de logging para aplicações corporativas,
    incluindo formatação estruturada, rotação de arquivos e diferentes níveis
    de log para desenvolvimento e produção.
    """
    
    def __init__(self, name: str = "dashboard_economico"):
        """
        Inicializa o sistema de logging.
        
        Args:
            name (str): Nome do logger principal
        """
        self.logger_name = name
        self.logger = logging.getLogger(name)
        self._setup_logger()
    
    def _setup_logger(self) -> None:
        """Configura o logger com handlers e formatadores apropriados."""
        # Evita configuração duplicada
        if self.logger.handlers:
            return
        
        self.logger.setLevel(logging.DEBUG)
        
        # Cria diretório de logs se não existir
        log_dir = self._get_log_directory()
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configurar formatadores
        detailed_formatter = self._get_detailed_formatter()
        simple_formatter = self._get_simple_formatter()
        
        # Handler para arquivo de logs detalhados
        file_handler = self._create_file_handler(log_dir, detailed_formatter)
        self.logger.addHandler(file_handler)
        
        # Handler para arquivo de erros
        error_handler = self._create_error_handler(log_dir, detailed_formatter)
        self.logger.addHandler(error_handler)
        
        # Handler para console (apenas em desenvolvimento)
        if self._is_development_mode():
            console_handler = self._create_console_handler(simple_formatter)
            self.logger.addHandler(console_handler)
    
    def _get_log_directory(self) -> Path:
        """
        Determina o diretório para armazenamento de logs.
        
        Returns:
            Path: Caminho para o diretório de logs
        """
        project_root = Path(__file__).parent.parent.parent
        return project_root / "logs"
    
    def _get_detailed_formatter(self) -> logging.Formatter:
        """
        Cria formatador detalhado para arquivos de log.
        
        Returns:
            logging.Formatter: Formatador configurado
        """
        format_string = (
            "%(asctime)s | %(levelname)-8s | %(name)s | "
            "%(module)s.%(funcName)s:%(lineno)d | %(message)s"
        )
        return logging.Formatter(
            format_string,
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    
    def _get_simple_formatter(self) -> logging.Formatter:
        """
        Cria formatador simples para console.
        
        Returns:
            logging.Formatter: Formatador configurado
        """
        return logging.Formatter(
            "%(levelname)s: %(message)s"
        )
    
    def _create_file_handler(self, log_dir: Path, formatter: logging.Formatter) -> logging.Handler:
        """
        Cria handler para arquivo de logs com rotação.
        
        Args:
            log_dir (Path): Diretório de logs
            formatter (logging.Formatter): Formatador a ser usado
            
        Returns:
            logging.Handler: Handler configurado
        """
        log_file = log_dir / "application.log"
        
        handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        
        return handler
    
    def _create_error_handler(self, log_dir: Path, formatter: logging.Formatter) -> logging.Handler:
        """
        Cria handler específico para logs de erro.
        
        Args:
            log_dir (Path): Diretório de logs
            formatter (logging.Formatter): Formatador a ser usado
            
        Returns:
            logging.Handler: Handler configurado
        """
        error_file = log_dir / "errors.log"
        
        handler = logging.handlers.RotatingFileHandler(
            filename=error_file,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=3,
            encoding='utf-8'
        )
        
        handler.setLevel(logging.ERROR)
        handler.setFormatter(formatter)
        
        return handler
    
    def _create_console_handler(self, formatter: logging.Formatter) -> logging.Handler:
        """
        Cria handler para saída no console.
        
        Args:
            formatter (logging.Formatter): Formatador a ser usado
            
        Returns:
            logging.Handler: Handler configurado
        """
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        
        return handler
    
    def _is_development_mode(self) -> bool:
        """
        Verifica se a aplicação está em modo de desenvolvimento.
        
        Returns:
            bool: True se estiver em desenvolvimento
        """
        return os.getenv("ENVIRONMENT", "development").lower() == "development"
    
    def get_logger(self, module_name: Optional[str] = None) -> logging.Logger:
        """
        Retorna logger configurado para um módulo específico.
        
        Args:
            module_name (str, optional): Nome do módulo
            
        Returns:
            logging.Logger: Logger configurado
        """
        if module_name:
            return logging.getLogger(f"{self.logger_name}.{module_name}")
        return self.logger
    
    def log_performance(self, operation: str, duration: float, **kwargs) -> None:
        """
        Registra métricas de performance.
        
        Args:
            operation (str): Nome da operação
            duration (float): Duração em segundos
            **kwargs: Parâmetros adicionais
        """
        extra_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        message = f"PERFORMANCE | {operation} | {duration:.3f}s"
        
        if extra_info:
            message += f" | {extra_info}"
        
        self.logger.info(message)
    
    def log_business_event(self, event: str, details: dict) -> None:
        """
        Registra eventos de negócio importantes.
        
        Args:
            event (str): Nome do evento
            details (dict): Detalhes do evento
        """
        details_str = " | ".join([f"{k}={v}" for k, v in details.items()])
        message = f"BUSINESS_EVENT | {event} | {details_str}"
        
        self.logger.info(message)


# Instância global do sistema de logging
enterprise_logger = EnterpriseLogger()

# Função de conveniência para obter logger
def get_logger(module_name: Optional[str] = None) -> logging.Logger:
    """
    Função de conveniência para obter logger configurado.
    
    Args:
        module_name (str, optional): Nome do módulo
        
    Returns:
        logging.Logger: Logger configurado
    """
    return enterprise_logger.get_logger(module_name)

