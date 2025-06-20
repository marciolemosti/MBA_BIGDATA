"""
Dashboard Principal - Sistema de Indicadores Econômicos
Aplicação principal do dashboard interativo.

Autor: Márcio Lemos
Projeto: MBA em Gestão Analítica em BI e Big Data
Ano: 2025
"""

import os
import sys
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from pathlib import Path

# Configurar caminhos
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Configuração da página
st.set_page_config(
    page_title="Dashboard Econômico Brasileiro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Importações do projeto
try:
    from common.config_manager import config_manager
    from common.logger import get_logger
    from analytics.data_manager import data_manager
    from analytics.forecast_models import forecast_engine
    from data_services.cache_service import cache_service
except ImportError as e:
    st.error(f"Erro ao importar módulos: {e}")
    st.stop()

logger = get_logger("dashboard_main")


class EconomicDashboard:
    """
    Dashboard principal para análise de indicadores econômicos.
    
    Esta classe implementa a interface principal do sistema,
    fornecendo visualizações interativas e análises em tempo real.
    """
    
    def __init__(self):
        """Inicializa o dashboard."""
        self.config = config_manager
        self.indicators_config = self.config.get_section("indicators")
        
        # Inicializar estado da sessão
        if 'selected_indicators' not in st.session_state:
            st.session_state.selected_indicators = list(self.indicators_config.keys())[:3]
        
        if 'forecast_horizon' not in st.session_state:
            st.session_state.forecast_horizon = 24
    
    def render(self):
        """Renderiza o dashboard completo."""
        try:
            self._render_header()
            self._render_sidebar()
            self._render_main_content()
            self._render_footer()
            
        except Exception as e:
            logger.error(f"Erro ao renderizar dashboard: {e}")
            st.error("Erro interno do sistema. Verifique os logs para mais detalhes.")
    
    def _render_header(self):
        """Renderiza o cabeçalho do dashboard."""
        st.title("📊 Dashboard Econômico Brasileiro")
        st.markdown("### Sistema de Análise de Indicadores Econômicos")
        
        # Informações do projeto
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.markdown("**Projeto**: MBA em Gestão Analítica em BI e Big Data")
        
        with col2:
            st.markdown("**Autor**: Márcio Lemos")
        
        with col3:
            st.markdown(f"**Atualizado**: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
        
        st.divider()
    
    def _render_sidebar(self):
        """Renderiza a barra lateral com controles."""
        with st.sidebar:
            st.header("⚙️ Configurações")
            
            # Seleção de indicadores
            st.subheader("Indicadores")
            available_indicators = list(self.indicators_config.keys())
            
            selected = st.multiselect(
                "Selecione os indicadores:",
                options=available_indicators,
                default=st.session_state.selected_indicators,
                format_func=lambda x: self.indicators_config[x]["nome"]
            )
            
            if selected:
                st.session_state.selected_indicators = selected
            
            # Configurações de previsão
            st.subheader("Previsões")
            horizon = st.slider(
                "Horizonte (meses):",
                min_value=6,
                max_value=36,
                value=st.session_state.forecast_horizon,
                step=6
            )
            st.session_state.forecast_horizon = horizon
            
            # Filtros de data
            st.subheader("Período")
            date_range = st.selectbox(
                "Período de análise:",
                options=["Últimos 5 anos", "Últimos 3 anos", "Últimos 2 anos", "Último ano"],
                index=0
            )
            
            # Botões de ação
            st.subheader("Ações")
            if st.button("🔄 Atualizar Dados"):
                self._refresh_data()
            
            if st.button("📊 Gerar Relatório"):
                self._generate_report()
            
            # Informações do sistema
            st.subheader("Sistema")
            self._render_system_info()
    
    def _render_main_content(self):
        """Renderiza o conteúdo principal."""
        if not st.session_state.selected_indicators:
            st.warning("Selecione pelo menos um indicador na barra lateral.")
            return
        
        # Tabs principais
        tab1, tab2, tab3, tab4 = st.tabs([
            "📈 Visualizações", 
            "🔮 Previsões", 
            "📊 Análise Comparativa", 
            "📋 Relatórios"
        ])
        
        with tab1:
            self._render_visualizations_tab()
        
        with tab2:
            self._render_forecasts_tab()
        
        with tab3:
            self._render_comparative_tab()
        
        with tab4:
            self._render_reports_tab()
    
    def _render_visualizations_tab(self):
        """Renderiza a aba de visualizações."""
        st.subheader("Séries Temporais dos Indicadores")
        
        # Carregar dados dos indicadores selecionados
        indicators_data = {}
        
        for indicator_code in st.session_state.selected_indicators:
            df = data_manager.load_indicator_data(indicator_code)
            if df is not None and not df.empty:
                indicators_data[indicator_code] = df
        
        if not indicators_data:
            st.error("Nenhum dado disponível para os indicadores selecionados.")
            return
        
        # Criar gráficos
        for indicator_code, df in indicators_data.items():
            config = self.indicators_config[indicator_code]
            
            # Gráfico individual
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df['data'],
                y=df['valor'],
                mode='lines+markers',
                name=config['nome'],
                line=dict(color=config['cor'], width=2),
                marker=dict(size=4)
            ))
            
            fig.update_layout(
                title=f"{config['nome']} - {config['descricao']}",
                xaxis_title="Data",
                yaxis_title=f"{config['nome']} ({config['unidade']})",
                height=400,
                showlegend=True,
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Estatísticas resumidas
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Último Valor", f"{df.iloc[-1]['valor']:.2f} {config['unidade']}")
            
            with col2:
                variation = df.iloc[-1]['valor'] - df.iloc[-2]['valor'] if len(df) > 1 else 0
                st.metric("Variação", f"{variation:+.2f}", delta=f"{variation:+.2f}")
            
            with col3:
                st.metric("Média Histórica", f"{df['valor'].mean():.2f}")
            
            with col4:
                st.metric("Volatilidade", f"{df['valor'].std():.2f}")
            
            st.divider()
    
    def _render_forecasts_tab(self):
        """Renderiza a aba de previsões."""
        st.subheader("Previsões Econômicas")
        
        if not st.session_state.selected_indicators:
            st.warning("Selecione indicadores para gerar previsões.")
            return
        
        # Gerar previsões para cada indicador
        for indicator_code in st.session_state.selected_indicators:
            config = self.indicators_config[indicator_code]
            
            st.markdown(f"#### {config['nome']}")
            
            # Carregar dados históricos
            df = data_manager.load_indicator_data(indicator_code)
            
            if df is None or df.empty:
                st.error(f"Dados não disponíveis para {config['nome']}")
                continue
            
            # Gerar previsão
            with st.spinner(f"Gerando previsão para {config['nome']}..."):
                forecast_result = forecast_engine.generate_forecast(
                    df, indicator_code, model_type="auto"
                )
            
            if forecast_result is None:
                st.error(f"Erro ao gerar previsão para {config['nome']}")
                continue
            
            # Visualizar previsão
            fig = go.Figure()
            
            # Dados históricos
            fig.add_trace(go.Scatter(
                x=df['data'],
                y=df['valor'],
                mode='lines+markers',
                name='Histórico',
                line=dict(color=config['cor'], width=2)
            ))
            
            # Previsão
            fig.add_trace(go.Scatter(
                x=forecast_result.forecast_dates,
                y=forecast_result.forecast_values,
                mode='lines+markers',
                name='Previsão',
                line=dict(color=config['cor'], width=2, dash='dash')
            ))
            
            # Intervalo de confiança
            fig.add_trace(go.Scatter(
                x=forecast_result.forecast_dates + forecast_result.forecast_dates[::-1],
                y=forecast_result.upper_bound + forecast_result.lower_bound[::-1],
                fill='toself',
                fillcolor=f"rgba{tuple(list(px.colors.hex_to_rgb(config['cor'])) + [0.2])}",
                line=dict(color='rgba(255,255,255,0)'),
                name=f'IC {forecast_result.confidence_level:.0%}',
                showlegend=True
            ))
            
            fig.update_layout(
                title=f"Previsão: {config['nome']} - {st.session_state.forecast_horizon} meses",
                xaxis_title="Data",
                yaxis_title=f"{config['nome']} ({config['unidade']})",
                height=500,
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Métricas da previsão
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Modelo Usado", forecast_result.model_type.title())
            
            with col2:
                mae = forecast_result.model_performance.get('mae', 0)
                st.metric("MAE", f"{mae:.3f}")
            
            with col3:
                r2 = forecast_result.model_performance.get('r_squared', 0)
                st.metric("R²", f"{r2:.3f}")
            
            with col4:
                mape = forecast_result.model_performance.get('mape', 0)
                st.metric("MAPE", f"{mape:.1f}%")
            
            st.divider()
    
    def _render_comparative_tab(self):
        """Renderiza a aba de análise comparativa."""
        st.subheader("Análise Comparativa")
        
        if len(st.session_state.selected_indicators) < 2:
            st.warning("Selecione pelo menos 2 indicadores para análise comparativa.")
            return
        
        # Matriz de correlação
        st.markdown("#### Matriz de Correlação")
        
        correlation_matrix = data_manager.get_correlation_matrix(st.session_state.selected_indicators)
        
        if correlation_matrix is not None:
            fig = px.imshow(
                correlation_matrix,
                text_auto=True,
                aspect="auto",
                color_continuous_scale="RdBu_r",
                title="Correlação entre Indicadores"
            )
            
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.error("Erro ao calcular matriz de correlação.")
        
        # Gráfico comparativo normalizado
        st.markdown("#### Comparação Normalizada")
        
        indicators_data = data_manager.get_multiple_indicators(st.session_state.selected_indicators)
        
        if indicators_data:
            fig = go.Figure()
            
            for indicator_code, df in indicators_data.items():
                if not df.empty:
                    config = self.indicators_config[indicator_code]
                    
                    # Normalizar valores (z-score)
                    normalized_values = (df['valor'] - df['valor'].mean()) / df['valor'].std()
                    
                    fig.add_trace(go.Scatter(
                        x=df['data'],
                        y=normalized_values,
                        mode='lines',
                        name=config['nome'],
                        line=dict(color=config['cor'], width=2)
                    ))
            
            fig.update_layout(
                title="Indicadores Normalizados (Z-Score)",
                xaxis_title="Data",
                yaxis_title="Valor Normalizado",
                height=500,
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    def _render_reports_tab(self):
        """Renderiza a aba de relatórios."""
        st.subheader("Relatórios e Análises")
        
        # Relatório de qualidade dos dados
        st.markdown("#### Qualidade dos Dados")
        
        quality_report = data_manager.get_data_quality_report()
        
        if quality_report:
            summary = quality_report.get("summary", {})
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Indicadores Válidos", summary.get("valid_indicators", 0))
            
            with col2:
                st.metric("Indicadores Atualizados", summary.get("fresh_indicators", 0))
            
            with col3:
                quality_score = summary.get("quality_score", 0)
                st.metric("Score de Qualidade", f"{quality_score:.1%}")
            
            # Botão para download do relatório de qualidade em CSV
            if st.button("📥 Download Relatório de Qualidade (CSV)"):
                csv_data = self._generate_quality_csv(quality_report)
                st.download_button(
                    label="Baixar CSV",
                    data=csv_data,
                    file_name=f"relatorio_qualidade_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            
            # Detalhes por indicador
            st.markdown("#### Detalhes por Indicador")
            
            for indicator_code in st.session_state.selected_indicators:
                if indicator_code in quality_report.get("indicators", {}):
                    indicator_quality = quality_report["indicators"][indicator_code]
                    config = self.indicators_config[indicator_code]
                    
                    with st.expander(f"{config['nome']} - Status: {'✅' if indicator_quality['is_valid'] else '❌'}"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**Registros Totais:**", indicator_quality.get("total_records", 0))
                            st.write("**Valores Nulos:**", indicator_quality.get("null_values", 0))
                            st.write("**Última Atualização:**", indicator_quality.get("last_update", "N/A"))
                        
                        with col2:
                            if indicator_quality.get("validation_errors"):
                                st.write("**Erros de Validação:**")
                                for error in indicator_quality["validation_errors"]:
                                    st.write(f"- {error}")
                            
                            if indicator_quality.get("freshness_warnings"):
                                st.write("**Avisos de Atualização:**")
                                for warning in indicator_quality["freshness_warnings"]:
                                    st.write(f"- {warning}")
        
        # Seção de download de dados históricos
        st.markdown("#### Download de Dados")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📥 Download Dados Históricos (CSV)"):
                csv_data = self._generate_historical_csv()
                st.download_button(
                    label="Baixar Dados Históricos",
                    data=csv_data,
                    file_name=f"dados_historicos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        with col2:
            if st.button("📥 Download Previsões (CSV)"):
                csv_data = self._generate_forecasts_csv()
                st.download_button(
                    label="Baixar Previsões",
                    data=csv_data,
                    file_name=f"previsoes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    def _render_footer(self):
        """Renderiza o rodapé."""
        st.divider()
        
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            st.markdown(
                """
                <div style='text-align: center; color: #666;'>
                    <p><strong>Dashboard Econômico Brasileiro</strong></p>
                    <p>MBA em Gestão Analítica em BI e Big Data | Márcio Lemos | 2025</p>
                </div>
                """,
                unsafe_allow_html=True
            )
    
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
    
    def _render_system_info(self):
        """Renderiza informações do sistema."""
        # Cache stats
        cache_stats = cache_service.get_stats()
        
        st.write("**Cache:**")
        st.write(f"- Entradas: {cache_stats.get('total_entries', 0)}")
        st.write(f"- Hit Rate: {cache_stats.get('hit_rate_percent', 0):.1f}%")
        
        # Indicadores disponíveis
        available = data_manager.get_available_indicators()
        st.write(f"**Indicadores:** {len(available)} disponíveis")
    
    def _refresh_data(self):
        """Atualiza os dados do sistema."""
        with st.spinner("Atualizando dados..."):
            cache_service.clear()
            data_manager.refresh_cache()
        
        st.success("Dados atualizados com sucesso!")
        st.rerun()
    
    def _generate_report(self):
        """Gera relatório do sistema."""
        st.info("Funcionalidade de relatório será implementada em versão futura.")


def main():
    """Função principal da aplicação."""
    try:
        dashboard = EconomicDashboard()
        dashboard.render()
        
    except Exception as e:
        st.error(f"Erro crítico na aplicação: {e}")
        logger.error(f"Erro crítico: {e}")


if __name__ == "__main__":
    main()

