"""
Módulo de Conexão com PostgreSQL
Gerencia conexões e operações com o banco de dados PostgreSQL.

Este módulo fornece uma interface robusta para interagir com o banco de dados,
incluindo pool de conexões, transações e operações CRUD otimizadas.

Autor: Márcio Lemos
Data: 2025-06-23
"""

import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor, execute_values
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json


class DatabaseConfig:
    """
    Configuração do banco de dados.
    
    Centraliza todas as configurações de conexão e permite
    diferentes ambientes (desenvolvimento, produção, etc.).
    """
    
    def __init__(self, environment: str = "development"):
        """
        Inicializa configuração do banco.
        
        Args:
            environment: Ambiente (development, production, test)
        """
        self.environment = environment
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Configurações por ambiente
        self.configs = {
            "development": {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "database": os.getenv("DB_NAME", "mba_bigdata"),
                "user": os.getenv("DB_USER", "mba_user"),
                "password": os.getenv("DB_PASSWORD", "mba_password_2025"),
                "app_user": os.getenv("DB_APP_USER", "mba_app_user"),
                "app_password": os.getenv("DB_APP_PASSWORD", "app_password_2025")
            },
            "production": {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "database": os.getenv("DB_NAME", "mba_bigdata"),
                "user": os.getenv("DB_USER", "mba_user"),
                "password": os.getenv("DB_PASSWORD", "mba_password_2025"),
                "app_user": os.getenv("DB_APP_USER", "mba_app_user"),
                "app_password": os.getenv("DB_APP_PASSWORD", "app_password_2025")
            },
            "test": {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "database": os.getenv("DB_NAME", "mba_bigdata_test"),
                "user": os.getenv("DB_USER", "mba_user"),
                "password": os.getenv("DB_PASSWORD", "mba_password_2025"),
                "app_user": os.getenv("DB_APP_USER", "mba_app_user"),
                "app_password": os.getenv("DB_APP_PASSWORD", "app_password_2025")
            }
        }
        
        self.config = self.configs.get(environment, self.configs["development"])
        
        # Pool de conexões
        self.pool_config = {
            "minconn": int(os.getenv("DB_POOL_MIN", "2")),
            "maxconn": int(os.getenv("DB_POOL_MAX", "10"))
        }
    
    def get_connection_string(self, use_app_user: bool = True) -> str:
        """
        Gera string de conexão PostgreSQL.
        
        Args:
            use_app_user: Se deve usar usuário da aplicação
            
        Returns:
            String de conexão
        """
        if use_app_user:
            user = self.config["app_user"]
            password = self.config["app_password"]
        else:
            user = self.config["user"]
            password = self.config["password"]
        
        return (
            f"postgresql://{user}:{password}@"
            f"{self.config['host']}:{self.config['port']}/{self.config['database']}"
        )
    
    def get_psycopg2_params(self, use_app_user: bool = True) -> Dict[str, Any]:
        """
        Gera parâmetros para psycopg2.
        
        Args:
            use_app_user: Se deve usar usuário da aplicação
            
        Returns:
            Dicionário com parâmetros de conexão
        """
        if use_app_user:
            user = self.config["app_user"]
            password = self.config["app_password"]
        else:
            user = self.config["user"]
            password = self.config["password"]
        
        return {
            "host": self.config["host"],
            "port": self.config["port"],
            "database": self.config["database"],
            "user": user,
            "password": password,
            "connect_timeout": 30,
            "application_name": "MBA_BigData_Dashboard"
        }


class DatabaseManager:
    """
    Gerenciador principal do banco de dados.
    
    Fornece interface de alto nível para todas as operações
    de banco de dados, incluindo pool de conexões e transações.
    """
    
    def __init__(self, environment: str = "development"):
        """
        Inicializa o gerenciador de banco de dados.
        
        Args:
            environment: Ambiente de execução
        """
        self.config = DatabaseConfig(environment)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Pool de conexões psycopg2
        self._connection_pool = None
        
        # Engine SQLAlchemy
        self._engine = None
        self._session_factory = None
        
        # Inicializar conexões
        self._initialize_connections()
        
        self.logger.info(f"DatabaseManager inicializado para ambiente: {environment}")
    
    def _initialize_connections(self):
        """Inicializa pools de conexão e engines."""
        try:
            # Pool psycopg2 para operações de baixo nível
            self._connection_pool = psycopg2.pool.ThreadedConnectionPool(
                **self.config.pool_config,
                **self.config.get_psycopg2_params()
            )
            
            # Engine SQLAlchemy para operações de alto nível
            self._engine = create_engine(
                self.config.get_connection_string(),
                pool_size=self.config.pool_config["maxconn"],
                max_overflow=5,
                pool_timeout=30,
                pool_recycle=3600,
                echo=False  # Set to True for SQL debugging
            )
            
            # Session factory
            self._session_factory = sessionmaker(bind=self._engine)
            
            self.logger.info("Conexões com banco de dados inicializadas")
            
        except Exception as e:
            self.logger.error(f"Erro ao inicializar conexões: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """
        Context manager para obter conexão do pool.
        
        Yields:
            Conexão psycopg2
        """
        conn = None
        try:
            conn = self._connection_pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Erro na conexão: {e}")
            raise
        finally:
            if conn:
                self._connection_pool.putconn(conn)
    
    @contextmanager
    def get_cursor(self, dict_cursor: bool = True):
        """
        Context manager para obter cursor.
        
        Args:
            dict_cursor: Se deve usar RealDictCursor
            
        Yields:
            Cursor psycopg2
        """
        with self.get_connection() as conn:
            cursor_class = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_class)
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Erro no cursor: {e}")
                raise
            finally:
                cursor.close()
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict]:
        """
        Executa query SELECT e retorna resultados.
        
        Args:
            query: Query SQL
            params: Parâmetros da query
            
        Returns:
            Lista de dicionários com resultados
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def execute_command(self, command: str, params: Optional[Tuple] = None) -> int:
        """
        Executa comando SQL (INSERT, UPDATE, DELETE).
        
        Args:
            command: Comando SQL
            params: Parâmetros do comando
            
        Returns:
            Número de linhas afetadas
        """
        with self.get_cursor() as cursor:
            cursor.execute(command, params)
            return cursor.rowcount
    
    def execute_many(self, command: str, params_list: List[Tuple]) -> int:
        """
        Executa comando SQL múltiplas vezes.
        
        Args:
            command: Comando SQL
            params_list: Lista de parâmetros
            
        Returns:
            Número total de linhas afetadas
        """
        with self.get_cursor() as cursor:
            cursor.executemany(command, params_list)
            return cursor.rowcount
    
    def bulk_insert(self, table: str, data: List[Dict], 
                   schema: str = "economic_data") -> int:
        """
        Inserção em lote otimizada.
        
        Args:
            table: Nome da tabela
            data: Lista de dicionários com dados
            schema: Schema da tabela
            
        Returns:
            Número de linhas inseridas
        """
        if not data:
            return 0
        
        # Preparar dados
        columns = list(data[0].keys())
        values = [[row[col] for col in columns] for row in data]
        
        # Construir query
        table_name = f"{schema}.{table}"
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        with self.get_cursor(dict_cursor=False) as cursor:
            execute_values(cursor, query, values, page_size=1000)
            return cursor.rowcount
    
    def get_dataframe(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """
        Executa query e retorna DataFrame pandas.
        
        Args:
            query: Query SQL
            params: Parâmetros da query
            
        Returns:
            DataFrame com resultados
        """
        return pd.read_sql_query(query, self._engine, params=params)
    
    def save_dataframe(self, df: pd.DataFrame, table: str, 
                      schema: str = "economic_data", 
                      if_exists: str = "append") -> None:
        """
        Salva DataFrame no banco de dados.
        
        Args:
            df: DataFrame para salvar
            table: Nome da tabela
            schema: Schema da tabela
            if_exists: Ação se tabela existir ('append', 'replace', 'fail')
        """
        df.to_sql(
            table, 
            self._engine, 
            schema=schema,
            if_exists=if_exists, 
            index=False,
            method='multi',
            chunksize=1000
        )
    
    def test_connection(self) -> bool:
        """
        Testa conectividade com o banco.
        
        Returns:
            True se conexão bem-sucedida
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            self.logger.error(f"Teste de conexão falhou: {e}")
            return False
    
    def get_system_stats(self) -> Dict[str, Any]:
        """
        Obtém estatísticas do sistema.
        
        Returns:
            Dicionário com estatísticas
        """
        try:
            query = "SELECT system_metadata.get_system_stats() as stats"
            result = self.execute_query(query)
            
            if result:
                return result[0]['stats']
            else:
                return {}
                
        except Exception as e:
            self.logger.error(f"Erro ao obter estatísticas: {e}")
            return {"error": str(e)}
    
    def cleanup_old_data(self, retention_days: int = 2555) -> int:
        """
        Remove dados antigos baseado na política de retenção.
        
        Args:
            retention_days: Dias de retenção
            
        Returns:
            Número de registros removidos
        """
        try:
            query = "SELECT system_metadata.cleanup_old_data(%s) as deleted_count"
            result = self.execute_query(query, (retention_days,))
            
            if result:
                return result[0]['deleted_count']
            else:
                return 0
                
        except Exception as e:
            self.logger.error(f"Erro na limpeza de dados: {e}")
            return 0
    
    def close_connections(self):
        """Fecha todas as conexões."""
        try:
            if self._connection_pool:
                self._connection_pool.closeall()
            
            if self._engine:
                self._engine.dispose()
            
            self.logger.info("Conexões fechadas")
            
        except Exception as e:
            self.logger.error(f"Erro ao fechar conexões: {e}")


class EconomicDataRepository:
    """
    Repositório para dados econômicos.
    
    Fornece métodos específicos para operações com dados econômicos,
    incluindo inserção, consulta e validação.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o repositório.
        
        Args:
            db_manager: Instância do DatabaseManager
        """
        self.db = db_manager
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_indicator_id(self, indicator_code: str) -> Optional[int]:
        """
        Obtém ID de um indicador pelo código.
        
        Args:
            indicator_code: Código do indicador
            
        Returns:
            ID do indicador ou None se não encontrado
        """
        query = """
        SELECT id FROM economic_data.indicators 
        WHERE code = %s AND is_active = true
        """
        
        result = self.db.execute_query(query, (indicator_code,))
        return result[0]['id'] if result else None
    
    def save_indicator_data(self, indicator_code: str, df: pd.DataFrame) -> int:
        """
        Salva dados de um indicador.
        
        Args:
            indicator_code: Código do indicador
            df: DataFrame com colunas 'data' e 'valor'
            
        Returns:
            Número de registros salvos
        """
        indicator_id = self.get_indicator_id(indicator_code)
        if not indicator_id:
            raise ValueError(f"Indicador '{indicator_code}' não encontrado")
        
        if df.empty:
            return 0
        
        # Preparar dados para inserção
        records = []
        for _, row in df.iterrows():
            records.append({
                'indicator_id': indicator_id,
                'reference_date': row['data'],
                'value': float(row['valor']),
                'collection_timestamp': datetime.now(),
                'data_quality_score': 1.0,  # Será calculado posteriormente
                'is_validated': False
            })
        
        # Inserção com ON CONFLICT para evitar duplicatas
        insert_query = """
        INSERT INTO economic_data.economic_data 
        (indicator_id, reference_date, value, collection_timestamp, data_quality_score, is_validated)
        VALUES (%(indicator_id)s, %(reference_date)s, %(value)s, %(collection_timestamp)s, %(data_quality_score)s, %(is_validated)s)
        ON CONFLICT (indicator_id, reference_date) 
        DO UPDATE SET 
            value = EXCLUDED.value,
            collection_timestamp = EXCLUDED.collection_timestamp,
            data_quality_score = EXCLUDED.data_quality_score
        """
        
        with self.db.get_cursor(dict_cursor=False) as cursor:
            cursor.executemany(insert_query, records)
            return cursor.rowcount
    
    def get_indicator_data(self, indicator_code: str, 
                          start_date: Optional[datetime] = None,
                          end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Obtém dados de um indicador.
        
        Args:
            indicator_code: Código do indicador
            start_date: Data inicial (opcional)
            end_date: Data final (opcional)
            
        Returns:
            DataFrame com dados do indicador
        """
        query = """
        SELECT 
            ed.reference_date as data,
            ed.value as valor,
            ed.data_quality_score,
            ed.collection_timestamp
        FROM economic_data.economic_data ed
        JOIN economic_data.indicators i ON ed.indicator_id = i.id
        WHERE i.code = %s AND i.is_active = true
        """
        
        params = [indicator_code]
        
        if start_date:
            query += " AND ed.reference_date >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND ed.reference_date <= %s"
            params.append(end_date)
        
        query += " ORDER BY ed.reference_date"
        
        return self.db.get_dataframe(query, params)
    
    def get_latest_data(self) -> pd.DataFrame:
        """
        Obtém dados mais recentes de todos os indicadores.
        
        Returns:
            DataFrame com dados mais recentes
        """
        query = """
        SELECT 
            code,
            name,
            unit,
            reference_date as data,
            value as valor,
            data_quality_score,
            collection_timestamp
        FROM economic_data.latest_indicators
        ORDER BY code
        """
        
        return self.db.get_dataframe(query)
    
    def update_data_quality_score(self, indicator_code: str, 
                                 quality_scores: Dict[str, float]) -> int:
        """
        Atualiza scores de qualidade dos dados.
        
        Args:
            indicator_code: Código do indicador
            quality_scores: Dicionário {data: score}
            
        Returns:
            Número de registros atualizados
        """
        indicator_id = self.get_indicator_id(indicator_code)
        if not indicator_id:
            return 0
        
        update_query = """
        UPDATE economic_data.economic_data 
        SET data_quality_score = %s, is_validated = true
        WHERE indicator_id = %s AND reference_date = %s
        """
        
        params_list = [
            (score, indicator_id, date_str) 
            for date_str, score in quality_scores.items()
        ]
        
        return self.db.execute_many(update_query, params_list)
    
    def log_data_collection_run(self, run_info: Dict[str, Any]) -> int:
        """
        Registra execução de coleta de dados.
        
        Args:
            run_info: Informações da execução
            
        Returns:
            ID da execução registrada
        """
        insert_query = """
        INSERT INTO system_metadata.data_collection_runs 
        (start_time, end_time, status, total_indicators, successful_indicators, 
         failed_indicators, total_records_collected, error_message, execution_metadata)
        VALUES (%(start_time)s, %(end_time)s, %(status)s, %(total_indicators)s, 
                %(successful_indicators)s, %(failed_indicators)s, %(total_records_collected)s, 
                %(error_message)s, %(execution_metadata)s)
        RETURNING id
        """
        
        with self.db.get_cursor() as cursor:
            cursor.execute(insert_query, run_info)
            return cursor.fetchone()['id']

