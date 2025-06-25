    
    def _generate_quality_csv(self, quality_report):
        """Gera CSV do relatório de qualidade."""
        try:
            import io
            
            output = io.StringIO()
            
            # Cabeçalho
            output.write("Relatório de Qualidade de Dados - Dashboard Econômico Brasileiro\n")
            output.write(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            output.write("Autor: Márcio Lemos - MBA em Gestão Analítica em BI e Big Data\n\n")
            
            # Resumo geral
            summary = quality_report.get("summary", {})
            output.write("RESUMO GERAL\n")
            output.write(f"Indicadores Válidos,{summary.get('valid_indicators', 0)}\n")
            output.write(f"Indicadores Atualizados,{summary.get('fresh_indicators', 0)}\n")
            output.write(f"Score de Qualidade,{summary.get('quality_score', 0):.2%}\n\n")
            
            # Detalhes por indicador
            output.write("DETALHES POR INDICADOR\n")
            output.write("Indicador,Nome,Status,Registros Totais,Valores Nulos,Última Atualização,Erros,Avisos\n")
            
            indicators_data = quality_report.get("indicators", {})
            for indicator_code, indicator_quality in indicators_data.items():
                config = self.indicators_config.get(indicator_code, {})
                nome = config.get('nome', indicator_code)
                status = 'Válido' if indicator_quality.get('is_valid', False) else 'Inválido'
                total_records = indicator_quality.get('total_records', 0)
                null_values = indicator_quality.get('null_values', 0)
                last_update = indicator_quality.get('last_update', 'N/A')
                errors = '; '.join(indicator_quality.get('validation_errors', []))
                warnings = '; '.join(indicator_quality.get('freshness_warnings', []))
                
                output.write(f"{indicator_code},{nome},{status},{total_records},{null_values},{last_update},{errors},{warnings}\n")
            
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Erro ao gerar CSV de qualidade: {e}")
            return "Erro ao gerar relatório CSV"
    
    def _generate_historical_csv(self):
        """Gera CSV dos dados históricos."""
        try:
            import io
            
            output = io.StringIO()
            
            # Cabeçalho
            output.write("Dados Históricos - Dashboard Econômico Brasileiro\n")
            output.write(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            output.write("Autor: Márcio Lemos - MBA em Gestão Analítica em BI e Big Data\n\n")
            
            # Cabeçalho dos dados
            output.write("Data,Indicador,Nome,Valor,Unidade,Fonte\n")
            
            # Dados de cada indicador selecionado
            for indicator_code in st.session_state.selected_indicators:
                df = data_manager.load_indicator_data(indicator_code)
                config = self.indicators_config.get(indicator_code, {})
                
                if df is not None and not df.empty:
                    nome = config.get('nome', indicator_code)
                    unidade = config.get('unidade', '')
                    fonte = config.get('fonte', '')
                    
                    for _, row in df.iterrows():
                        data_str = row['data'].strftime('%Y-%m-%d') if hasattr(row['data'], 'strftime') else str(row['data'])
                        output.write(f"{data_str},{indicator_code},{nome},{row['valor']},{unidade},{fonte}\n")
            
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Erro ao gerar CSV histórico: {e}")
            return "Erro ao gerar dados históricos CSV"
    
    def _generate_forecasts_csv(self):
        """Gera CSV das previsões."""
        try:
            import io
            
            output = io.StringIO()
            
            # Cabeçalho
            output.write("Previsões Econômicas - Dashboard Econômico Brasileiro\n")
            output.write(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            output.write("Autor: Márcio Lemos - MBA em Gestão Analítica em BI e Big Data\n")
            output.write(f"Horizonte: {st.session_state.forecast_horizon} meses\n\n")
            
            # Cabeçalho dos dados
            output.write("Data,Indicador,Nome,Valor Previsto,Limite Inferior,Limite Superior,Modelo,Confiança\n")
            
            # Gerar previsões para cada indicador selecionado
            for indicator_code in st.session_state.selected_indicators:
                df = data_manager.load_indicator_data(indicator_code)
                config = self.indicators_config.get(indicator_code, {})
                
                if df is not None and not df.empty and len(df) >= 24:
                    nome = config.get('nome', indicator_code)
                    
                    # Gerar previsão
                    forecast_result = forecast_engine.generate_forecast(df, indicator_code)
                    
                    if forecast_result is not None:
                        for i, date in enumerate(forecast_result.forecast_dates):
                            data_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
                            valor_previsto = forecast_result.forecast_values[i]
                            limite_inf = forecast_result.lower_bound[i]
                            limite_sup = forecast_result.upper_bound[i]
                            modelo = forecast_result.model_type
                            confianca = f"{forecast_result.confidence_level:.0%}"
                            
                            output.write(f"{data_str},{indicator_code},{nome},{valor_previsto:.4f},{limite_inf:.4f},{limite_sup:.4f},{modelo},{confianca}\n")
            
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Erro ao gerar CSV de previsões: {e}")
            return "Erro ao gerar previsões CSV"

