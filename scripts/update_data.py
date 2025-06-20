#!/usr/bin/env python3
"""
Script de Atualização Automática de Dados Econômicos
Responsável pela coleta e atualização dos indicadores econômicos brasileiros.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
"""

import os
import sys
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.logger import get_logger
from common.config_manager import config_manager
from analytics.data_manager import data_manager

logger = get_logger("data_updater")


class EconomicDataUpdater:
    """
    Atualizador de dados econômicos com integração a APIs oficiais.
    
    Esta classe implementa a coleta automatizada de dados de indicadores
    econômicos brasileiros a partir de fontes oficiais como IBGE e BCB.
    """
    
    def __init__(self):
        """Inicializa o atualizador de dados."""
        self.data_dir = Path(__file__).parent.parent / "data"
        self.reports_dir = Path(__file__).parent.parent / "reports"
        
        # Criar diretórios se não existirem
        self.data_dir.mkdir(exist_ok=True)
        self.reports_dir.mkdir(exist_ok=True)
        
        # Configurações
        self.force_update = os.getenv("FORCE_UPDATE", "false").lower() == "true"
        self.timeout = 30
        
        # URLs das APIs (simuladas para demonstração)
        self.api_endpoints = {
            "ipca": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados",
            "selic": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados",
            "cambio_ptax_venda": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados",
            "deficit_primario": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.5793/dados",
            "arrecadacao_iof": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1207/dados"
        }
        
        logger.info("Atualizador de dados econômicos inicializado")
    
    def update_all_indicators(self) -> Dict[str, Any]:
        """
        Atualiza todos os indicadores econômicos.
        
        Returns:
            Dict[str, Any]: Relatório de atualização
        """
        start_time = datetime.now()
        report = {
            "timestamp": start_time.isoformat(),
            "force_update": self.force_update,
            "indicators": {},
            "summary": {
                "total_indicators": 0,
                "updated_indicators": 0,
                "failed_indicators": 0,
                "errors": []
            }
        }
        
        logger.info("Iniciando atualização de todos os indicadores")
        
        # Obter lista de indicadores configurados
        indicators_config = config_manager.get_section("indicators")
        
        for indicator_code in indicators_config.keys():
            try:
                logger.info(f"Atualizando indicador: {indicator_code}")
                
                result = self.update_indicator(indicator_code)
                report["indicators"][indicator_code] = result
                report["summary"]["total_indicators"] += 1
                
                if result["success"]:
                    report["summary"]["updated_indicators"] += 1
                else:
                    report["summary"]["failed_indicators"] += 1
                    report["summary"]["errors"].append(f"{indicator_code}: {result.get('error', 'Erro desconhecido')}")
                
            except Exception as e:
                error_msg = f"Erro ao atualizar {indicator_code}: {str(e)}"
                logger.error(error_msg)
                
                report["indicators"][indicator_code] = {
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                report["summary"]["failed_indicators"] += 1
                report["summary"]["errors"].append(error_msg)
        
        # Finalizar relatório
        end_time = datetime.now()
        report["duration_seconds"] = (end_time - start_time).total_seconds()
        report["completed_at"] = end_time.isoformat()
        
        # Salvar relatório
        self._save_update_report(report)
        
        logger.info(f"Atualização concluída: {report['summary']['updated_indicators']}/{report['summary']['total_indicators']} indicadores atualizados")
        
        return report
    
    def update_indicator(self, indicator_code: str) -> Dict[str, Any]:
        """
        Atualiza um indicador específico.
        
        Args:
            indicator_code (str): Código do indicador
            
        Returns:
            Dict[str, Any]: Resultado da atualização
        """
        try:
            # Verificar se precisa atualizar
            if not self.force_update and not self._needs_update(indicator_code):
                return {
                    "success": True,
                    "action": "skipped",
                    "reason": "Dados já atualizados",
                    "timestamp": datetime.now().isoformat()
                }
            
            # Buscar novos dados
            new_data = self._fetch_indicator_data(indicator_code)
            
            if not new_data:
                return {
                    "success": False,
                    "error": "Nenhum dado retornado pela API",
                    "timestamp": datetime.now().isoformat()
                }
            
            # Carregar dados existentes
            existing_data = self._load_existing_data(indicator_code)
            
            # Mesclar dados
            merged_data = self._merge_data(existing_data, new_data)
            
            # Validar dados
            if not self._validate_data(merged_data, indicator_code):
                return {
                    "success": False,
                    "error": "Dados não passaram na validação",
                    "timestamp": datetime.now().isoformat()
                }
            
            # Salvar dados atualizados
            self._save_indicator_data(indicator_code, merged_data)
            
            return {
                "success": True,
                "action": "updated",
                "records_added": len(new_data),
                "total_records": len(merged_data),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Erro ao atualizar indicador {indicator_code}: {e}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def _needs_update(self, indicator_code: str) -> bool:
        """
        Verifica se um indicador precisa ser atualizado.
        
        Args:
            indicator_code (str): Código do indicador
            
        Returns:
            bool: True se precisa atualizar
        """
        try:
            data_file = self.data_dir / f"{indicator_code}.json"
            
            if not data_file.exists():
                return True
            
            # Verificar data da última modificação
            last_modified = datetime.fromtimestamp(data_file.stat().st_mtime)
            hours_since_update = (datetime.now() - last_modified).total_seconds() / 3600
            
            # Atualizar se passou mais de 12 horas
            return hours_since_update > 12
            
        except Exception:
            return True
    
    def _fetch_indicator_data(self, indicator_code: str) -> Optional[List[Dict[str, Any]]]:
        """
        Busca dados de um indicador na API.
        
        Args:
            indicator_code (str): Código do indicador
            
        Returns:
            Optional[List[Dict[str, Any]]]: Dados do indicador ou None
        """
        try:
            # Para demonstração, vamos simular dados ou usar dados existentes
            # Em produção, aqui seria feita a chamada real para as APIs
            
            logger.info(f"Simulando busca de dados para {indicator_code}")
            
            # Carregar dados existentes e simular alguns novos
            existing_data = self._load_existing_data(indicator_code)
            
            if not existing_data:
                return None
            
            # Simular adição de 1-3 novos pontos de dados
            last_date = pd.to_datetime(existing_data[-1]["data"])
            new_data = []
            
            for i in range(1, 4):  # Adicionar até 3 novos pontos
                new_date = last_date + timedelta(days=30 * i)
                
                # Simular valor baseado no último valor com pequena variação
                last_value = existing_data[-1]["valor"]
                variation = (hash(f"{indicator_code}_{new_date}") % 200 - 100) / 1000  # -0.1 a +0.1
                new_value = last_value * (1 + variation)
                
                new_data.append({
                    "data": new_date.strftime("%Y-%m-%d"),
                    "valor": round(new_value, 4)
                })
            
            logger.info(f"Simulados {len(new_data)} novos pontos para {indicator_code}")
            return new_data
            
        except Exception as e:
            logger.error(f"Erro ao buscar dados do indicador {indicator_code}: {e}")
            return None
    
    def _load_existing_data(self, indicator_code: str) -> List[Dict[str, Any]]:
        """
        Carrega dados existentes de um indicador.
        
        Args:
            indicator_code (str): Código do indicador
            
        Returns:
            List[Dict[str, Any]]: Dados existentes
        """
        try:
            data_file = self.data_dir / f"{indicator_code}.json"
            
            if not data_file.exists():
                return []
            
            with open(data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
                
        except Exception as e:
            logger.warning(f"Erro ao carregar dados existentes de {indicator_code}: {e}")
            return []
    
    def _merge_data(self, existing_data: List[Dict[str, Any]], 
                   new_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Mescla dados existentes com novos dados.
        
        Args:
            existing_data (List[Dict[str, Any]]): Dados existentes
            new_data (List[Dict[str, Any]]): Novos dados
            
        Returns:
            List[Dict[str, Any]]: Dados mesclados
        """
        try:
            # Converter para DataFrame para facilitar a mesclagem
            df_existing = pd.DataFrame(existing_data) if existing_data else pd.DataFrame(columns=['data', 'valor'])
            df_new = pd.DataFrame(new_data) if new_data else pd.DataFrame(columns=['data', 'valor'])
            
            if df_new.empty:
                return existing_data
            
            # Converter datas
            if not df_existing.empty:
                df_existing['data'] = pd.to_datetime(df_existing['data'])
            df_new['data'] = pd.to_datetime(df_new['data'])
            
            # Combinar e remover duplicatas
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['data'], keep='last')
            df_combined = df_combined.sort_values('data').reset_index(drop=True)
            
            # Converter de volta para formato JSON
            df_combined['data'] = df_combined['data'].dt.strftime('%Y-%m-%d')
            
            return df_combined.to_dict('records')
            
        except Exception as e:
            logger.error(f"Erro ao mesclar dados: {e}")
            return existing_data
    
    def _validate_data(self, data: List[Dict[str, Any]], indicator_code: str) -> bool:
        """
        Valida os dados de um indicador.
        
        Args:
            data (List[Dict[str, Any]]): Dados a serem validados
            indicator_code (str): Código do indicador
            
        Returns:
            bool: True se os dados são válidos
        """
        try:
            if not data:
                return False
            
            # Verificações básicas
            for record in data:
                if 'data' not in record or 'valor' not in record:
                    return False
                
                # Verificar se a data é válida
                try:
                    pd.to_datetime(record['data'])
                except:
                    return False
                
                # Verificar se o valor é numérico
                try:
                    float(record['valor'])
                except:
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Erro na validação de dados: {e}")
            return False
    
    def _save_indicator_data(self, indicator_code: str, data: List[Dict[str, Any]]) -> None:
        """
        Salva dados de um indicador.
        
        Args:
            indicator_code (str): Código do indicador
            data (List[Dict[str, Any]]): Dados a serem salvos
        """
        try:
            data_file = self.data_dir / f"{indicator_code}.json"
            
            with open(data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Dados salvos para {indicator_code}: {len(data)} registros")
            
        except Exception as e:
            logger.error(f"Erro ao salvar dados de {indicator_code}: {e}")
            raise
    
    def _save_update_report(self, report: Dict[str, Any]) -> None:
        """
        Salva relatório de atualização.
        
        Args:
            report (Dict[str, Any]): Relatório de atualização
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = self.reports_dir / f"update_report_{timestamp}.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Relatório de atualização salvo: {report_file}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar relatório: {e}")


def main():
    """Função principal do script."""
    try:
        logger.info("=== Iniciando Atualização Automática de Dados ===")
        
        updater = EconomicDataUpdater()
        report = updater.update_all_indicators()
        
        # Log do resumo
        summary = report["summary"]
        logger.info(f"Atualização concluída em {report['duration_seconds']:.2f} segundos")
        logger.info(f"Indicadores atualizados: {summary['updated_indicators']}/{summary['total_indicators']}")
        
        if summary["errors"]:
            logger.warning(f"Erros encontrados: {len(summary['errors'])}")
            for error in summary["errors"]:
                logger.warning(f"  - {error}")
        
        # Código de saída
        exit_code = 0 if summary["failed_indicators"] == 0 else 1
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Erro crítico na atualização: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

