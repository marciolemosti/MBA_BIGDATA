#!/usr/bin/env python3
"""
Script de Validação de Qualidade de Dados - Versão Refatorada
Responsável pela validação e geração de relatórios de qualidade dos dados econômicos.

Esta versão corrige o erro de serialização JSON e implementa validações mais robustas.

Autor: Márcio Lemos
Projeto: Dashboard de Indicadores Econômicos Brasileiros
MBA: Gestão Analítica em BI e Big Data
Data: 2025-06-23
"""

import sys
import json
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Union

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.logger import get_logger
from data_sources.data_manager import DataManager

logger = get_logger("data_quality_validator")


class JSONEncoder(json.JSONEncoder):
    """
    Encoder JSON customizado para tratar tipos numpy e pandas.
    
    Esta classe resolve o problema de serialização de tipos int64 e outros
    tipos numpy que não são nativamente serializáveis em JSON.
    """
    
    def default(self, obj):
        """
        Converte objetos não serializáveis para tipos Python nativos.
        
        Args:
            obj: Objeto a ser serializado
            
        Returns:
            Objeto serializado ou chamada para o encoder padrão
        """
        # Tratar tipos numpy
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        
        # Tratar tipos pandas
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, pd.Timedelta):
            return str(obj)
        
        # Tratar datetime
        elif isinstance(obj, datetime):
            return obj.isoformat()
        
        # Chamar encoder padrão para outros tipos
        return super().default(obj)


class DataQualityValidator:
    """
    Validador de qualidade de dados econômicos - Versão Refatorada.
    
    Esta classe implementa verificações abrangentes de qualidade,
    integridade e consistência dos dados econômicos, com correções
    para problemas de serialização e validações mais robustas.
    """
    
    def __init__(self):
        """Inicializa o validador de qualidade."""
        self.reports_dir = Path(__file__).parent.parent / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        
        # Inicializar gerenciador de dados
        self.data_manager = DataManager()
        
        # Configurações de qualidade
        self.quality_thresholds = {
            'min_records': 10,  # Mínimo de registros por indicador
            'max_null_percentage': 0.05,  # Máximo 5% de valores nulos
            'max_outlier_percentage': 0.10,  # Máximo 10% de outliers
            'min_data_freshness_days': 30  # Dados não podem ser mais antigos que 30 dias
        }
        
        logger.info("Validador de qualidade de dados inicializado")
    
    def validate_all_data(self) -> Dict[str, Any]:
        """
        Executa validação completa de qualidade dos dados.
        
        Returns:
            Dict[str, Any]: Relatório de qualidade com tipos serializáveis
        """
        start_time = datetime.now()
        
        logger.info("Iniciando validação de qualidade dos dados")
        
        try:
            # Obter todos os indicadores disponíveis
            indicators = list(self.data_manager.INDICATOR_MAPPING.keys())
            
            # Período para validação (últimos 2 anos)
            end_date = datetime.now()
            start_date = datetime(end_date.year - 2, 1, 1)
            
            # Coletar dados de todos os indicadores
            logger.info(f"Coletando dados para {len(indicators)} indicadores")
            data_results = self.data_manager.get_multiple_indicators(
                indicators, start_date, end_date
            )
            
            # Validar cada indicador
            validation_results = {}
            total_score = 0
            valid_indicators = 0
            
            for indicator, df in data_results.items():
                logger.info(f"Validando {indicator}...")
                
                try:
                    validation_result = self._validate_indicator(indicator, df)
                    validation_results[indicator] = validation_result
                    
                    if validation_result['valid']:
                        total_score += validation_result['quality_score']
                        valid_indicators += 1
                        
                except Exception as e:
                    logger.error(f"Erro ao validar {indicator}: {e}")
                    validation_results[indicator] = {
                        'valid': False,
                        'error': str(e),
                        'quality_score': 0.0
                    }
            
            # Calcular score geral
            overall_quality = total_score / len(indicators) if indicators else 0.0
            
            # Gerar relatório final
            quality_report = {
                "validation_info": {
                    "timestamp": start_time.isoformat(),
                    "start_time": start_time.isoformat(),
                    "end_time": datetime.now().isoformat(),
                    "duration_seconds": float((datetime.now() - start_time).total_seconds()),
                    "validator_version": "2.0.0"
                },
                "data_period": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                },
                "summary": {
                    "total_indicators": int(len(indicators)),
                    "valid_indicators": int(valid_indicators),
                    "failed_indicators": int(len(indicators) - valid_indicators),
                    "overall_quality_score": float(overall_quality),
                    "quality_percentage": float(overall_quality * 100),
                    "status": "PASS" if overall_quality >= 0.8 else "FAIL"
                },
                "indicators": validation_results,
                "quality_thresholds": self.quality_thresholds,
                "recommendations": self._generate_recommendations(validation_results)
            }
            
            # Salvar relatório
            self._save_quality_report(quality_report)
            
            # Log do resumo
            logger.info(f"Validação concluída: {overall_quality:.2%} de qualidade geral")
            logger.info(f"Indicadores válidos: {valid_indicators}/{len(indicators)}")
            
            return quality_report
            
        except Exception as e:
            logger.error(f"Erro durante validação: {e}")
            
            # Retornar relatório de erro
            error_report = {
                "validation_info": {
                    "timestamp": start_time.isoformat(),
                    "error": str(e),
                    "status": "ERROR"
                },
                "summary": {
                    "overall_quality_score": 0.0,
                    "status": "ERROR"
                }
            }
            
            return error_report
    
    def _validate_indicator(self, indicator: str, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida um indicador específico.
        
        Args:
            indicator: Nome do indicador
            df: DataFrame com os dados
            
        Returns:
            Dict com resultado da validação
        """
        validation_result = {
            'indicator': indicator,
            'valid': True,
            'errors': [],
            'warnings': [],
            'metrics': {},
            'quality_score': 1.0
        }
        
        try:
            # Verificação básica: DataFrame não vazio
            if df.empty:
                validation_result['valid'] = False
                validation_result['errors'].append("DataFrame vazio")
                validation_result['quality_score'] = 0.0
                return validation_result
            
            # Verificação de colunas obrigatórias
            required_columns = ['data', 'valor']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                validation_result['valid'] = False
                validation_result['errors'].append(f"Colunas obrigatórias ausentes: {missing_columns}")
                validation_result['quality_score'] = 0.0
                return validation_result
            
            # Métricas básicas
            total_records = len(df)
            null_count = int(df['valor'].isnull().sum())
            null_percentage = float(null_count / total_records) if total_records > 0 else 0.0
            
            # Verificar quantidade mínima de registros
            if total_records < self.quality_thresholds['min_records']:
                validation_result['warnings'].append(
                    f"Poucos registros: {total_records} (mínimo: {self.quality_thresholds['min_records']})"
                )
                validation_result['quality_score'] *= 0.8
            
            # Verificar valores nulos
            if null_percentage > self.quality_thresholds['max_null_percentage']:
                validation_result['errors'].append(
                    f"Muitos valores nulos: {null_percentage:.2%} (máximo: {self.quality_thresholds['max_null_percentage']:.2%})"
                )
                validation_result['quality_score'] *= 0.7
            
            # Verificar outliers (apenas se há dados suficientes)
            outlier_count = 0
            outlier_percentage = 0.0
            
            if total_records > 10 and null_count < total_records:
                valid_values = df['valor'].dropna()
                if len(valid_values) > 0:
                    q1 = float(valid_values.quantile(0.25))
                    q3 = float(valid_values.quantile(0.75))
                    iqr = q3 - q1
                    
                    if iqr > 0:
                        lower_bound = q1 - 1.5 * iqr
                        upper_bound = q3 + 1.5 * iqr
                        
                        outliers = (valid_values < lower_bound) | (valid_values > upper_bound)
                        outlier_count = int(outliers.sum())
                        outlier_percentage = float(outlier_count / len(valid_values))
                        
                        if outlier_percentage > self.quality_thresholds['max_outlier_percentage']:
                            validation_result['warnings'].append(
                                f"Muitos outliers: {outlier_percentage:.2%} (máximo: {self.quality_thresholds['max_outlier_percentage']:.2%})"
                            )
                            validation_result['quality_score'] *= 0.9
            
            # Verificar atualidade dos dados
            if 'data' in df.columns and not df.empty:
                try:
                    latest_date = pd.to_datetime(df['data']).max()
                    days_old = (datetime.now() - latest_date).days
                    
                    if days_old > self.quality_thresholds['min_data_freshness_days']:
                        validation_result['warnings'].append(
                            f"Dados desatualizados: {days_old} dias (máximo: {self.quality_thresholds['min_data_freshness_days']})"
                        )
                        validation_result['quality_score'] *= 0.9
                        
                except Exception as e:
                    validation_result['warnings'].append(f"Erro ao verificar atualidade: {e}")
            
            # Verificar continuidade temporal
            gaps = self._check_temporal_gaps(df)
            if gaps > 0:
                validation_result['warnings'].append(f"Gaps temporais encontrados: {gaps}")
                validation_result['quality_score'] *= 0.95
            
            # Compilar métricas (garantindo tipos serializáveis)
            validation_result['metrics'] = {
                'total_records': int(total_records),
                'null_count': int(null_count),
                'null_percentage': float(null_percentage),
                'outlier_count': int(outlier_count),
                'outlier_percentage': float(outlier_percentage),
                'temporal_gaps': int(gaps)
            }
            
            # Adicionar estatísticas dos valores se disponível
            if not df.empty and 'valor' in df.columns:
                valid_values = df['valor'].dropna()
                if len(valid_values) > 0:
                    validation_result['metrics']['value_stats'] = {
                        'min': float(valid_values.min()),
                        'max': float(valid_values.max()),
                        'mean': float(valid_values.mean()),
                        'std': float(valid_values.std()),
                        'median': float(valid_values.median())
                    }
                    
                    # Adicionar range de datas
                    if 'data' in df.columns:
                        dates = pd.to_datetime(df['data'])
                        validation_result['metrics']['date_range'] = {
                            'start': dates.min().isoformat(),
                            'end': dates.max().isoformat(),
                            'span_days': int((dates.max() - dates.min()).days)
                        }
            
            # Determinar se é válido baseado em erros
            validation_result['valid'] = len(validation_result['errors']) == 0
            
            # Garantir que quality_score está entre 0 e 1
            validation_result['quality_score'] = max(0.0, min(1.0, validation_result['quality_score']))
            
        except Exception as e:
            logger.error(f"Erro na validação de {indicator}: {e}")
            validation_result['valid'] = False
            validation_result['errors'].append(f"Erro interno: {str(e)}")
            validation_result['quality_score'] = 0.0
        
        return validation_result
    
    def _check_temporal_gaps(self, df: pd.DataFrame) -> int:
        """
        Verifica gaps temporais nos dados.
        
        Args:
            df: DataFrame com os dados
            
        Returns:
            Número de gaps encontrados
        """
        try:
            if df.empty or 'data' not in df.columns:
                return 0
            
            dates = pd.to_datetime(df['data']).sort_values()
            gaps = 0
            
            for i in range(1, len(dates)):
                gap_days = (dates.iloc[i] - dates.iloc[i-1]).days
                if gap_days > 35:  # Gap maior que 1 mês
                    gaps += 1
            
            return gaps
            
        except Exception:
            return 0
    
    def _generate_recommendations(self, validation_results: Dict[str, Any]) -> List[str]:
        """
        Gera recomendações baseadas nos resultados da validação.
        
        Args:
            validation_results: Resultados da validação
            
        Returns:
            Lista de recomendações
        """
        recommendations = []
        
        # Contar problemas
        failed_indicators = [k for k, v in validation_results.items() if not v.get('valid', False)]
        low_quality_indicators = [k for k, v in validation_results.items() 
                                if v.get('quality_score', 0) < 0.8]
        
        if failed_indicators:
            recommendations.append(
                f"Corrigir indicadores com falhas: {', '.join(failed_indicators)}"
            )
        
        if low_quality_indicators:
            recommendations.append(
                f"Melhorar qualidade dos indicadores: {', '.join(low_quality_indicators)}"
            )
        
        # Recomendações específicas baseadas em padrões
        null_issues = [k for k, v in validation_results.items() 
                      if any('nulos' in error for error in v.get('errors', []))]
        if null_issues:
            recommendations.append(
                "Implementar tratamento de valores nulos nos conectores de dados"
            )
        
        outlier_issues = [k for k, v in validation_results.items() 
                         if any('outliers' in warning for warning in v.get('warnings', []))]
        if outlier_issues:
            recommendations.append(
                "Revisar algoritmos de detecção de outliers e implementar filtros"
            )
        
        freshness_issues = [k for k, v in validation_results.items() 
                           if any('desatualizados' in warning for warning in v.get('warnings', []))]
        if freshness_issues:
            recommendations.append(
                "Aumentar frequência de atualização dos dados ou verificar APIs"
            )
        
        if not recommendations:
            recommendations.append("Qualidade dos dados está satisfatória")
        
        return recommendations
    
    def _save_quality_report(self, report: Dict[str, Any]) -> None:
        """
        Salva relatório de qualidade com encoder JSON customizado.
        
        Args:
            report: Relatório de qualidade
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = self.reports_dir / f"quality_report_{timestamp}.json"
            
            # Usar encoder customizado para tratar tipos numpy
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, cls=JSONEncoder)
            
            logger.info(f"Relatório de qualidade salvo: {report_file}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar relatório de qualidade: {e}")
            
            # Tentar salvar versão simplificada em caso de erro
            try:
                simple_report = {
                    "timestamp": datetime.now().isoformat(),
                    "error": str(e),
                    "summary": report.get("summary", {}),
                    "status": "ERROR_SAVING"
                }
                
                error_file = self.reports_dir / f"quality_report_error_{timestamp}.json"
                with open(error_file, 'w', encoding='utf-8') as f:
                    json.dump(simple_report, f, ensure_ascii=False, indent=2, cls=JSONEncoder)
                
                logger.info(f"Relatório de erro salvo: {error_file}")
                
            except Exception as e2:
                logger.error(f"Erro crítico ao salvar relatório: {e2}")


def main():
    """Função principal do script."""
    try:
        logger.info("=== Iniciando Validação de Qualidade de Dados (Versão Refatorada) ===")
        
        validator = DataQualityValidator()
        report = validator.validate_all_data()
        
        # Verificar se há problemas críticos
        summary = report.get("summary", {})
        quality_score = summary.get("overall_quality_score", 0)
        status = summary.get("status", "UNKNOWN")
        
        if status == "ERROR":
            logger.error("Erro crítico durante validação")
            sys.exit(1)
        elif quality_score < 0.8:  # Menos de 80% de qualidade
            logger.warning(f"Qualidade dos dados abaixo do esperado: {quality_score:.2%}")
            logger.info("Verifique o relatório de qualidade para detalhes")
            sys.exit(1)
        else:
            logger.info(f"Qualidade dos dados satisfatória: {quality_score:.2%}")
            sys.exit(0)
        
    except Exception as e:
        logger.error(f"Erro crítico na validação: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

