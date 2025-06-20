#!/usr/bin/env python3
"""
Script de Validação de Qualidade de Dados
Responsável pela validação e geração de relatórios de qualidade dos dados econômicos.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
"""

import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.logger import get_logger
from analytics.data_manager import data_manager

logger = get_logger("data_quality_validator")


class DataQualityValidator:
    """
    Validador de qualidade de dados econômicos.
    
    Esta classe implementa verificações abrangentes de qualidade,
    integridade e consistência dos dados econômicos.
    """
    
    def __init__(self):
        """Inicializa o validador de qualidade."""
        self.reports_dir = Path(__file__).parent.parent / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        
        logger.info("Validador de qualidade de dados inicializado")
    
    def validate_all_data(self) -> Dict[str, Any]:
        """
        Executa validação completa de qualidade dos dados.
        
        Returns:
            Dict[str, Any]: Relatório de qualidade
        """
        start_time = datetime.now()
        
        logger.info("Iniciando validação de qualidade dos dados")
        
        # Gerar relatório de qualidade usando o data_manager
        quality_report = data_manager.get_data_quality_report()
        
        # Adicionar informações adicionais
        quality_report.update({
            "validation_start": start_time.isoformat(),
            "validation_end": datetime.now().isoformat(),
            "validation_duration": (datetime.now() - start_time).total_seconds()
        })
        
        # Salvar relatório
        self._save_quality_report(quality_report)
        
        # Log do resumo
        summary = quality_report.get("summary", {})
        logger.info(f"Validação concluída: {summary.get('quality_score', 0):.2%} de qualidade geral")
        
        return quality_report
    
    def _save_quality_report(self, report: Dict[str, Any]) -> None:
        """
        Salva relatório de qualidade.
        
        Args:
            report (Dict[str, Any]): Relatório de qualidade
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = self.reports_dir / f"quality_report_{timestamp}.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Relatório de qualidade salvo: {report_file}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar relatório de qualidade: {e}")


def main():
    """Função principal do script."""
    try:
        logger.info("=== Iniciando Validação de Qualidade de Dados ===")
        
        validator = DataQualityValidator()
        report = validator.validate_all_data()
        
        # Verificar se há problemas críticos
        summary = report.get("summary", {})
        quality_score = summary.get("quality_score", 0)
        
        if quality_score < 0.8:  # Menos de 80% de qualidade
            logger.warning(f"Qualidade dos dados abaixo do esperado: {quality_score:.2%}")
            sys.exit(1)
        else:
            logger.info(f"Qualidade dos dados satisfatória: {quality_score:.2%}")
            sys.exit(0)
        
    except Exception as e:
        logger.error(f"Erro crítico na validação: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

