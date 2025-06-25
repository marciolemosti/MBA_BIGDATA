#!/usr/bin/env python3
"""
Script de Geração de Relatório de Atualização
Responsável pela geração de relatórios detalhados das atualizações de dados.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
"""

import sys
import json
import glob
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.logger import get_logger

logger = get_logger("report_generator")


class UpdateReportGenerator:
    """
    Gerador de relatórios de atualização.
    
    Esta classe consolida informações de múltiplas execuções
    e gera relatórios executivos para monitoramento.
    """
    
    def __init__(self):
        """Inicializa o gerador de relatórios."""
        self.reports_dir = Path(__file__).parent.parent / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        
        logger.info("Gerador de relatórios inicializado")
    
    def generate_consolidated_report(self) -> Dict[str, Any]:
        """
        Gera relatório consolidado das últimas atualizações.
        
        Returns:
            Dict[str, Any]: Relatório consolidado
        """
        try:
            # Buscar relatórios recentes
            update_reports = self._load_recent_update_reports()
            quality_reports = self._load_recent_quality_reports()
            
            # Gerar relatório consolidado
            consolidated_report = {
                "generated_at": datetime.now().isoformat(),
                "period": "last_24_hours",
                "update_executions": len(update_reports),
                "quality_checks": len(quality_reports),
                "summary": self._generate_summary(update_reports, quality_reports),
                "trends": self._analyze_trends(update_reports),
                "recommendations": self._generate_recommendations(update_reports, quality_reports)
            }
            
            # Salvar relatório
            self._save_consolidated_report(consolidated_report)
            
            logger.info("Relatório consolidado gerado com sucesso")
            return consolidated_report
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatório consolidado: {e}")
            return {}
    
    def _load_recent_update_reports(self) -> List[Dict[str, Any]]:
        """Carrega relatórios de atualização recentes."""
        try:
            reports = []
            pattern = str(self.reports_dir / "update_report_*.json")
            
            for file_path in sorted(glob.glob(pattern), reverse=True)[:10]:  # Últimos 10
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        report = json.load(f)
                        reports.append(report)
                except Exception as e:
                    logger.warning(f"Erro ao carregar relatório {file_path}: {e}")
            
            return reports
            
        except Exception as e:
            logger.error(f"Erro ao carregar relatórios de atualização: {e}")
            return []
    
    def _load_recent_quality_reports(self) -> List[Dict[str, Any]]:
        """Carrega relatórios de qualidade recentes."""
        try:
            reports = []
            pattern = str(self.reports_dir / "quality_report_*.json")
            
            for file_path in sorted(glob.glob(pattern), reverse=True)[:10]:  # Últimos 10
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        report = json.load(f)
                        reports.append(report)
                except Exception as e:
                    logger.warning(f"Erro ao carregar relatório {file_path}: {e}")
            
            return reports
            
        except Exception as e:
            logger.error(f"Erro ao carregar relatórios de qualidade: {e}")
            return []
    
    def _generate_summary(self, update_reports: List[Dict[str, Any]], 
                         quality_reports: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Gera resumo executivo."""
        try:
            if not update_reports:
                return {"status": "no_data"}
            
            # Estatísticas de atualização
            total_updates = len(update_reports)
            successful_updates = sum(1 for r in update_reports 
                                   if r.get("summary", {}).get("failed_indicators", 0) == 0)
            
            # Estatísticas de qualidade
            avg_quality_score = 0
            if quality_reports:
                quality_scores = [r.get("summary", {}).get("quality_score", 0) 
                                for r in quality_reports]
                avg_quality_score = sum(quality_scores) / len(quality_scores)
            
            return {
                "status": "healthy" if successful_updates / total_updates > 0.8 else "warning",
                "update_success_rate": successful_updates / total_updates,
                "average_quality_score": avg_quality_score,
                "last_update": update_reports[0].get("timestamp") if update_reports else None,
                "total_executions": total_updates
            }
            
        except Exception as e:
            logger.error(f"Erro ao gerar resumo: {e}")
            return {"status": "error", "error": str(e)}
    
    def _analyze_trends(self, update_reports: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analisa tendências nas atualizações."""
        try:
            if len(update_reports) < 2:
                return {"status": "insufficient_data"}
            
            # Analisar duração das atualizações
            durations = [r.get("duration_seconds", 0) for r in update_reports]
            avg_duration = sum(durations) / len(durations)
            
            # Analisar taxa de sucesso ao longo do tempo
            success_rates = []
            for report in update_reports:
                summary = report.get("summary", {})
                total = summary.get("total_indicators", 1)
                failed = summary.get("failed_indicators", 0)
                success_rate = (total - failed) / total
                success_rates.append(success_rate)
            
            return {
                "average_duration_seconds": avg_duration,
                "duration_trend": "stable",  # Simplificado
                "success_rate_trend": "stable",  # Simplificado
                "performance_status": "good" if avg_duration < 60 else "slow"
            }
            
        except Exception as e:
            logger.error(f"Erro ao analisar tendências: {e}")
            return {"status": "error"}
    
    def _generate_recommendations(self, update_reports: List[Dict[str, Any]], 
                                quality_reports: List[Dict[str, Any]]) -> List[str]:
        """Gera recomendações baseadas nos relatórios."""
        recommendations = []
        
        try:
            if not update_reports:
                recommendations.append("Configurar monitoramento de atualizações")
                return recommendations
            
            # Analisar última atualização
            last_report = update_reports[0]
            summary = last_report.get("summary", {})
            
            if summary.get("failed_indicators", 0) > 0:
                recommendations.append("Investigar falhas na atualização de indicadores")
            
            if last_report.get("duration_seconds", 0) > 120:
                recommendations.append("Otimizar performance das atualizações")
            
            # Analisar qualidade
            if quality_reports:
                last_quality = quality_reports[0]
                quality_score = last_quality.get("summary", {}).get("quality_score", 1)
                
                if quality_score < 0.8:
                    recommendations.append("Melhorar qualidade dos dados")
                
                if quality_score < 0.6:
                    recommendations.append("Revisar fontes de dados urgentemente")
            
            if not recommendations:
                recommendations.append("Sistema funcionando adequadamente")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Erro ao gerar recomendações: {e}")
            return ["Erro ao analisar sistema"]
    
    def _save_consolidated_report(self, report: Dict[str, Any]) -> None:
        """Salva relatório consolidado."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = self.reports_dir / f"consolidated_report_{timestamp}.json"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Relatório consolidado salvo: {report_file}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar relatório consolidado: {e}")


def main():
    """Função principal do script."""
    try:
        logger.info("=== Gerando Relatório de Atualização ===")
        
        generator = UpdateReportGenerator()
        report = generator.generate_consolidated_report()
        
        # Log do status
        summary = report.get("summary", {})
        status = summary.get("status", "unknown")
        
        logger.info(f"Relatório gerado - Status: {status}")
        
        if status == "error":
            sys.exit(1)
        else:
            sys.exit(0)
        
    except Exception as e:
        logger.error(f"Erro crítico na geração do relatório: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

