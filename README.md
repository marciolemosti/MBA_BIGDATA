# Dashboard Econômico Brasileiro
## Trabalho de Conclusão - MBA em Gestão Analítica em BI e Big Data

### Autor: Márcio Lemos
### Ano: 2025

---

## Visão Geral

Este projeto apresenta um sistema completo de análise e visualização de indicadores econômicos brasileiros, desenvolvido como trabalho de conclusão do MBA em Gestão Analítica em BI e Big Data. A solução implementa técnicas avançadas de Business Intelligence, análise de dados e modelagem preditiva.

## Objetivos

### Objetivo Principal
Desenvolver uma plataforma analítica integrada para monitoramento em tempo real e análise preditiva de indicadores econômicos brasileiros, aplicando metodologias de BI e Big Data para suporte à tomada de decisões estratégicas.

### Objetivos Específicos
- Implementar sistema automatizado de coleta e processamento de dados econômicos
- Desenvolver modelos preditivos para projeções de médio prazo (24 meses)
- Criar interface interativa para análise exploratória de dados
- Estabelecer pipeline de atualização automática via GitHub Actions
- Aplicar metodologias de governança e qualidade de dados

## Arquitetura da Solução

### Estrutura do Projeto
```
MBA_Final/
├── src/
│   ├── analytics/           # Motor de análises e previsões
│   ├── data_services/       # Serviços de dados e cache
│   ├── dashboard/           # Interface do usuário
│   └── common/              # Utilitários compartilhados
├── config/                  # Configurações do sistema
├── data/                    # Dados dos indicadores
├── scripts/                 # Scripts de automação
├── docs/                    # Documentação técnica
└── .github/workflows/       # Automação CI/CD
```

### Tecnologias Utilizadas
- **Python 3.11**: Linguagem principal
- **Streamlit**: Framework para dashboards
- **Pandas & NumPy**: Manipulação de dados
- **Plotly**: Visualizações interativas
- **Prophet**: Modelagem de séries temporais
- **GitHub Actions**: Automação e CI/CD

## Indicadores Econômicos

1. **IPCA** - Índice de Preços ao Consumidor Amplo
2. **Taxa Selic** - Taxa básica de juros
3. **Câmbio USD/BRL** - Taxa de câmbio
4. **Déficit Primário** - Situação fiscal do governo
5. **Arrecadação IOF** - Imposto sobre Operações Financeiras
6. **PIB** - Produto Interno Bruto
7. **Taxa de Desemprego** - Mercado de trabalho

## Funcionalidades Principais

### Análise de Dados
- Visualizações interativas de séries temporais
- Análise de correlação entre indicadores
- Detecção automática de outliers e anomalias
- Relatórios de qualidade de dados

### Modelagem Preditiva
- Previsões para horizonte de 24 meses
- Múltiplos algoritmos (Prophet, decomposição sazonal, tendência linear)
- Intervalos de confiança configuráveis
- Métricas de performance dos modelos

### Automação
- Atualização automática de dados (8h e 18h diariamente)
- Validação contínua de qualidade
- Geração automática de relatórios
- Pipeline de CI/CD completo

## Instalação e Execução

### Pré-requisitos
- Python 3.11 ou superior
- Git

### Instalação
```bash
# Clonar o repositório
git clone <url-do-repositorio>
cd MBA_Final

# Instalar dependências
pip install -r requirements.txt
```

### Execução Local
```bash
# Executar dashboard
streamlit run src/dashboard/main.py

# Atualizar dados manualmente
python scripts/update_data.py

# Validar qualidade dos dados
python scripts/validate_data_quality.py
```

## Automação GitHub Actions

O projeto inclui workflows automatizados que executam:
- **Atualização de dados**: Diariamente às 8h e 18h
- **Validação de qualidade**: Após cada atualização
- **Geração de relatórios**: Consolidação de métricas
- **Limpeza e manutenção**: Otimização do repositório

## Metodologia Acadêmica

### Fundamentação Teórica
- Aplicação de conceitos de Business Intelligence
- Metodologias de análise de séries temporais
- Técnicas de visualização de dados
- Governança e qualidade de dados

### Contribuições
- Framework integrado para análise econômica
- Pipeline automatizado de dados
- Metodologia de validação contínua
- Interface responsiva para análise exploratória

## Resultados Esperados

- **Performance**: Redução de 80% no tempo de análise
- **Precisão**: Acurácia superior a 85% nas previsões
- **Usabilidade**: Interface intuitiva para usuários não-técnicos
- **Confiabilidade**: Atualização automática e validação contínua

## Considerações Finais

Este trabalho demonstra a aplicação prática de conceitos avançados de Business Intelligence e Big Data na análise de indicadores econômicos brasileiros. A solução desenvolvida oferece uma plataforma robusta, escalável e automatizada para suporte à tomada de decisões estratégicas.

---

**Desenvolvido por**: Márcio Lemos  
**Orientação**: Prof. Me. Thiago Bluhm  
**Instituição**: Universidade de Fortaleza  
**Pós Graduação**: MBA em Gestão Analítica em BI e Big Data  
**Ano**: 2025

