#!/usr/bin/env python3
"""
Script de Atualização de Dados - Versão Refatorada
Responsável pela coleta e atualização dos dados econômicos usando APIs reais.

Esta versão utiliza os novos conectores de APIs implementados e corrige
problemas identificados na versão anterior.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
Data: 2025-06-23
"""

import sys
import json
import os
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.logger import get_logger
from data_sources.data_manager import DataManager

logger = get_logger("data_updater")


class JSONEncoder(json.JSONEncoder):
    """
    Encoder JSON customizado para tratar tipos numpy e pandas.
    """
    
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        
        return super().default(obj)


class DataUpdater:
    """
    Atualizador de dados econômicos - Versão Refatorada.
    
    Esta classe coordena a coleta de dados de múltiplas fontes
    e atualiza os arquivos de dados do sistema.
    """
    
    def __init__(self):
        """Inicializa o atualizador de dados."""
        self.project_root = Path(__file__).parent.parent
        self.data_dir = self.project_root / "data"
        self.reports_dir = self.project_root / "reports"
        
        # Criar diretórios se não existirem
        self.data_dir.mkdir(exist_ok=True)
        self.reports_dir.mkdir(exist_ok=True)
        
        # Inicializar gerenciador de dados
        self.data_manager = DataManager(str(self.data_dir))
        
        # Verificar se deve forçar atualização
        self.force_update = os.getenv('FORCE_UPDATE', 'false').lower() == 'true'
        
        logger.info("Atualizador de dados inicializado")
        logger.info(f"Diretório de dados: {self.data_dir}")
        logger.info(f"Forçar atualização: {self.force_update}")
    
    def update_all_data(self) -> Dict[str, Any]:
        """
        Atualiza todos os indicadores econômicos.
        
        Returns:
            Dict com relatório de atualização
        """
        start_time = datetime.now()
        
        logger.info("=== Iniciando atualização de dados ===")
        
        try:
            # Definir período de coleta
            end_date = datetime.now()
            start_date = datetime(2020, 1, 1)  # Dados desde 2020
            
            logger.info(f"Período de coleta: {start_date.date()} a {end_date.date()}")
            
            # Obter lista de indicadores
            indicators = list(self.data_manager.INDICATOR_MAPPING.keys())
            logger.info(f"Indicadores a atualizar: {indicators}")
            
            # Coletar dados
            logger.info("Coletando dados de todas as fontes...")
            data_results = self.data_manager.get_multiple_indicators(
                indicators, start_date, end_date, force_update=self.force_update
            )
            
            # Processar e salvar cada indicador
            update_results = {}
            total_records = 0
            successful_updates = 0
            
            for indicator, df in data_results.items():
                try:
                    result = self._process_indicator_data(indicator, df)
                    update_results[indicator] = result
                    
                    if result['status'] == 'success':
                        successful_updates += 1
                        total_records += result['records']
                        
                except Exception as e:
                    logger.error(f"Erro ao processar {indicator}: {e}")
                    update_results[indicator] = {
                        'status': 'error',
                        'error': str(e),
                        'records': 0
                    }
            
            # Gerar relatório de atualização
            update_report = {
                'timestamp': start_time.isoformat(),
                'execution_info': {
                    'start_time': start_time.isoformat(),
                    'end_time': datetime.now().isoformat(),
                    'duration_seconds': (datetime.now() - start_time).total_seconds(),
                    'force_update': self.force_update
                },
                'data_period': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat()
                },
                'summary': {
                    'total_indicators': len(indicators),
                    'successful_updates': successful_updates,
                    'failed_updates': len(indicators) - successful_updates,
                    'total_records': total_records,
                    'success_rate': successful_updates / len(indicators) if indicators else 0
                },
                'indicators': update_results,
                'system_info': self._get_system_info()
            }
            
            # Salvar relatório
            self._save_update_report(update_report)
            
            # Log do resumo
            logger.info(f"Atualização concluída: {successful_updates}/{len(indicators)} indicadores")
            logger.info(f"Total de registros processados: {total_records}")
            
            return update_report
            
        except Exception as e:
            logger.error(f"Erro crítico durante atualização: {e}")
            
            error_report = {
                'timestamp': start_time.isoformat(),
                'status': 'error',
                'error': str(e),
                'summary': {
                    'successful_updates': 0,
                    'failed_updates': 0,
                    'total_records': 0
                }
            }
            
            return error_report
    
    def _process_indicator_data(self, indicator: str, df) -> Dict[str, Any]:
        """
        Processa e salva dados de um indicador.
        
        Args:
            indicator: Nome do indicador
            df: DataFrame com os dados
            
        Returns:
            Dict com resultado do processamento
        """
        try:
            if df.empty:
                logger.warning(f"Nenhum dado coletado para {indicator}")
                return {
                    'status': 'warning',
                    'message': 'Nenhum dado coletado',
                    'records': 0
                }
            
            # Validar estrutura dos dados
            if 'data' not in df.columns or 'valor' not in df.columns:
                raise ValueError(f"Estrutura de dados inválida para {indicator}")
            
            # Limpar e preparar dados
            df_clean = df.copy()
            
            # Remover valores nulos
            initial_count = len(df_clean)
            df_clean = df_clean.dropna(subset=['valor'])
            null_removed = initial_count - len(df_clean)
            
            if null_removed > 0:
                logger.info(f"{indicator}: Removidos {null_removed} valores nulos")
            
            # Remover duplicatas
            df_clean = df_clean.drop_duplicates(subset=['data'], keep='last')
            
            # Ordenar por data
            df_clean = df_clean.sort_values('data').reset_index(drop=True)
            
            # Converter tipos para garantir serialização
            df_clean['data'] = df_clean['data'].dt.strftime('%Y-%m-%d')
            df_clean['valor'] = df_clean['valor'].astype(float)
            
            # Salvar arquivo JSON
            output_file = self.data_dir / f"{indicator}.json"
            
            # Converter para formato de lista de dicionários
            data_list = df_clean.to_dict('records')
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data_list, f, ensure_ascii=False, indent=2, cls=JSONEncoder)
            
            logger.info(f"{indicator}: Salvos {len(df_clean)} registros em {output_file}")
            
            # Calcular estatísticas
            stats = {
                'records': len(df_clean),
                'date_range': {
                    'start': df_clean['data'].iloc[0] if not df_clean.empty else None,
                    'end': df_clean['data'].iloc[-1] if not df_clean.empty else None
                },
                'value_stats': {
                    'min': float(df_clean['valor'].min()),
                    'max': float(df_clean['valor'].max()),
                    'mean': float(df_clean['valor'].mean()),
                    'std': float(df_clean['valor'].std())
                } if not df_clean.empty else {},
                'data_quality': {
                    'null_removed': null_removed,
                    'duplicates_removed': initial_count - null_removed - len(df_clean)
                }
            }
            
            return {
                'status': 'success',
                'records': len(df_clean),
                'file': str(output_file),
                'statistics': stats
            }
            
        except Exception as e:
            logger.error(f"Erro ao processar {indicator}: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'records': 0
            }
    
    def _get_system_info(self) -> Dict[str, Any]:
        """
        Obtém informações do sistema.
        
        Returns:
            Dict com informações do sistema
        """
        try:
            system_status = self.data_manager.get_system_status()
            
            return {
                'data_manager_status': system_status,
                'environment': {
                    'python_version': sys.version,
                    'working_directory': str(Path.cwd()),
                    'data_directory': str(self.data_dir),
                    'force_update': self.force_update
                }
            }
            
        except Exception as e:
            logger.warning(f"Erro ao obter informações do sistema: {e}")
            return {
                'error': str(e),
                'environment': {
                    'python_version': sys.version,
                    'working_directory': str(Path.cwd())
                }
            }
    
    def _save_update_report(self, report: Dict[str, Any]) -> None:
        """
        Salva relatório de atualização.
        
        Args:
            report: Relatório de atualização
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = self.reports_dir / f"update_report_{timestamp}.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, cls=JSONEncoder)
            
            logger.info(f"Relatório de atualização salvo: {report_file}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar relatório de atualização: {e}")
    
    def update_single_indicator(self, indicator: str) -> Dict[str, Any]:
        """
        Atualiza um indicador específico.
        
        Args:
            indicator: Nome do indicador
            
        Returns:
            Dict com resultado da atualização
        """
        logger.info(f"Atualizando indicador específico: {indicator}")
        
        try:
            # Verificar se o indicador é válido
            if indicator not in self.data_manager.INDICATOR_MAPPING:
                raise ValueError(f"Indicador '{indicator}' não suportado")
            
            # Definir período
            end_date = datetime.now()
            start_date = datetime(2020, 1, 1)
            
            # Coletar dados
            df = self.data_manager.get_data(indicator, start_date, end_date, force_update=True)
            
            # Processar e salvar
            result = self._process_indicator_data(indicator, df)
            
            logger.info(f"Atualização de {indicator} concluída: {result['status']}")
            return result
            
        except Exception as e:
            logger.error(f"Erro ao atualizar {indicator}: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'records': 0
            }


def main():
    """Função principal do script."""
    try:
        logger.info("=== Script de Atualização de Dados (Versão Refatorada) ===")
        
        updater = DataUpdater()
        
        # Verificar se foi especificado um indicador específico
        if len(sys.argv) > 1:
            indicator = sys.argv[1]
            logger.info(f"Atualizando indicador específico: {indicator}")
            result = updater.update_single_indicator(indicator)
            
            if result['status'] == 'success':
                logger.info(f"Indicador {indicator} atualizado com sucesso")
                sys.exit(0)
            else:
                logger.error(f"Falha ao atualizar {indicator}")
                sys.exit(1)
        else:
            # Atualizar todos os indicadores
            report = updater.update_all_data()
            
            summary = report.get('summary', {})
            success_rate = summary.get('success_rate', 0)
            
            if success_rate >= 0.8:  # 80% ou mais de sucesso
                logger.info(f"Atualização bem-sucedida: {success_rate:.1%} de taxa de sucesso")
                sys.exit(0)
            else:
                logger.warning(f"Atualização com problemas: {success_rate:.1%} de taxa de sucesso")
                sys.exit(1)
        
    except Exception as e:
        logger.error(f"Erro crítico na atualização: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

