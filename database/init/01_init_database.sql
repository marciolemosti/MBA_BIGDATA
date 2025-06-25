-- Script de Inicialização do Banco de Dados
-- MBA Big Data - Dashboard de Indicadores Econômicos Brasileiros
-- 
-- Este script cria a estrutura inicial do banco de dados PostgreSQL
-- para armazenar dados econômicos e metadados do sistema.
--
-- Autor: Márcio Lemos
-- Data: 2025-06-23

-- Configurar encoding e locale
SET client_encoding = 'UTF8';
SET timezone = 'America/Sao_Paulo';

-- Criar extensões necessárias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Criar schema para dados econômicos
CREATE SCHEMA IF NOT EXISTS economic_data;
CREATE SCHEMA IF NOT EXISTS system_metadata;

-- Comentários nos schemas
COMMENT ON SCHEMA economic_data IS 'Schema para armazenamento de dados econômicos brasileiros';
COMMENT ON SCHEMA system_metadata IS 'Schema para metadados do sistema e logs';

-- =====================================================
-- TABELAS DE DADOS ECONÔMICOS
-- =====================================================

-- Tabela de fontes de dados
CREATE TABLE economic_data.data_sources (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    base_url VARCHAR(500),
    api_version VARCHAR(20),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE economic_data.data_sources IS 'Fontes de dados econômicos (IBGE, BCB, Tesouro Nacional, etc.)';

-- Inserir fontes de dados padrão
INSERT INTO economic_data.data_sources (code, name, description, base_url) VALUES
('ibge', 'Instituto Brasileiro de Geografia e Estatística', 'Dados de IPCA, PIB, Desemprego', 'https://apisidra.ibge.gov.br'),
('bcb', 'Banco Central do Brasil', 'Dados de Selic, Câmbio, Indicadores Monetários', 'https://api.bcb.gov.br'),
('tesouro', 'Tesouro Nacional', 'Dados Fiscais, Déficit Primário, Dívida Pública', 'https://apidatalake.tesouro.gov.br'),
('receita', 'Receita Federal', 'Dados de Arrecadação, IOF, Tributos', 'https://dados.gov.br');

-- Tabela de indicadores econômicos
CREATE TABLE economic_data.indicators (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    unit VARCHAR(50),
    frequency VARCHAR(20), -- daily, weekly, monthly, quarterly, yearly
    data_source_id INTEGER REFERENCES economic_data.data_sources(id),
    external_code VARCHAR(100), -- Código na API externa
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE economic_data.indicators IS 'Definição dos indicadores econômicos disponíveis';

-- Inserir indicadores padrão
INSERT INTO economic_data.indicators (code, name, description, unit, frequency, data_source_id, external_code) VALUES
('ipca', 'IPCA - Índice Nacional de Preços ao Consumidor Amplo', 'Variação mensal da inflação', '% a.m.', 'monthly', 1, '1737'),
('pib', 'PIB - Produto Interno Bruto', 'Produto Interno Bruto em valores correntes', 'Milhões de R$', 'quarterly', 1, '1621'),
('desemprego', 'Taxa de Desemprego', 'Taxa de desocupação - PNAD Contínua', '%', 'monthly', 1, '4099'),
('selic', 'Taxa Selic', 'Taxa básica de juros da economia', '% a.a.', 'daily', 2, '11'),
('cambio_ptax_venda', 'Taxa de Câmbio USD/BRL', 'Taxa de câmbio PTAX venda', 'R$/US$', 'daily', 2, '1'),
('deficit_primario', 'Resultado Primário', 'Déficit/Superávit primário do Governo Central', 'Milhões de R$', 'monthly', 3, 'resultado_primario'),
('arrecadacao_iof', 'Arrecadação IOF', 'Arrecadação do Imposto sobre Operações Financeiras', 'Milhões de R$', 'monthly', 4, 'iof');

-- Tabela principal de dados econômicos
CREATE TABLE economic_data.economic_data (
    id BIGSERIAL PRIMARY KEY,
    indicator_id INTEGER NOT NULL REFERENCES economic_data.indicators(id),
    reference_date DATE NOT NULL,
    value DECIMAL(20,6) NOT NULL,
    raw_value TEXT, -- Valor original da API (para auditoria)
    collection_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_quality_score DECIMAL(3,2), -- Score de 0.00 a 1.00
    is_validated BOOLEAN DEFAULT false,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE economic_data.economic_data IS 'Dados econômicos históricos e atuais';

-- Índices para performance
CREATE INDEX idx_economic_data_indicator_date ON economic_data.economic_data(indicator_id, reference_date);
CREATE INDEX idx_economic_data_date ON economic_data.economic_data(reference_date);
CREATE INDEX idx_economic_data_collection_time ON economic_data.economic_data(collection_timestamp);
CREATE UNIQUE INDEX idx_economic_data_unique ON economic_data.economic_data(indicator_id, reference_date);

-- =====================================================
-- TABELAS DE METADADOS E SISTEMA
-- =====================================================

-- Tabela de execuções de coleta de dados
CREATE TABLE system_metadata.data_collection_runs (
    id BIGSERIAL PRIMARY KEY,
    run_uuid UUID DEFAULT uuid_generate_v4(),
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL, -- running, completed, failed, cancelled
    total_indicators INTEGER,
    successful_indicators INTEGER,
    failed_indicators INTEGER,
    total_records_collected INTEGER,
    error_message TEXT,
    execution_metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE system_metadata.data_collection_runs IS 'Log de execuções de coleta de dados';

-- Tabela de logs de qualidade de dados
CREATE TABLE system_metadata.data_quality_logs (
    id BIGSERIAL PRIMARY KEY,
    indicator_id INTEGER REFERENCES economic_data.indicators(id),
    validation_date TIMESTAMP WITH TIME ZONE NOT NULL,
    quality_score DECIMAL(3,2) NOT NULL,
    total_records INTEGER,
    null_count INTEGER,
    outlier_count INTEGER,
    validation_errors JSONB,
    validation_warnings JSONB,
    validation_metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE system_metadata.data_quality_logs IS 'Histórico de validações de qualidade de dados';

-- Tabela de configurações do sistema
CREATE TABLE system_metadata.system_config (
    id SERIAL PRIMARY KEY,
    config_key VARCHAR(100) UNIQUE NOT NULL,
    config_value TEXT NOT NULL,
    description TEXT,
    config_type VARCHAR(20) DEFAULT 'string', -- string, integer, boolean, json
    is_sensitive BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE system_metadata.system_config IS 'Configurações do sistema';

-- Inserir configurações padrão
INSERT INTO system_metadata.system_config (config_key, config_value, description, config_type) VALUES
('data_retention_days', '2555', 'Dias de retenção de dados (7 anos)', 'integer'),
('collection_frequency_hours', '12', 'Frequência de coleta em horas', 'integer'),
('quality_threshold', '0.80', 'Threshold mínimo de qualidade (80%)', 'decimal'),
('enable_parallel_collection', 'true', 'Habilitar coleta paralela', 'boolean'),
('max_retry_attempts', '3', 'Máximo de tentativas em caso de falha', 'integer'),
('cache_ttl_seconds', '3600', 'TTL do cache em segundos', 'integer');

-- =====================================================
-- VIEWS PARA CONSULTAS COMUNS
-- =====================================================

-- View com dados mais recentes de cada indicador
CREATE VIEW economic_data.latest_indicators AS
SELECT 
    i.code,
    i.name,
    i.unit,
    ed.reference_date,
    ed.value,
    ed.collection_timestamp,
    ed.data_quality_score
FROM economic_data.indicators i
JOIN economic_data.economic_data ed ON i.id = ed.indicator_id
WHERE ed.reference_date = (
    SELECT MAX(reference_date) 
    FROM economic_data.economic_data ed2 
    WHERE ed2.indicator_id = i.id
)
AND i.is_active = true;

COMMENT ON VIEW economic_data.latest_indicators IS 'Valores mais recentes de cada indicador ativo';

-- View com estatísticas de qualidade por indicador
CREATE VIEW system_metadata.quality_summary AS
SELECT 
    i.code,
    i.name,
    COUNT(ed.id) as total_records,
    AVG(ed.data_quality_score) as avg_quality_score,
    MIN(ed.reference_date) as oldest_data,
    MAX(ed.reference_date) as newest_data,
    COUNT(CASE WHEN ed.data_quality_score < 0.8 THEN 1 END) as low_quality_records
FROM economic_data.indicators i
LEFT JOIN economic_data.economic_data ed ON i.id = ed.indicator_id
WHERE i.is_active = true
GROUP BY i.id, i.code, i.name;

COMMENT ON VIEW system_metadata.quality_summary IS 'Resumo de qualidade por indicador';

-- =====================================================
-- FUNÇÕES UTILITÁRIAS
-- =====================================================

-- Função para limpar dados antigos
CREATE OR REPLACE FUNCTION system_metadata.cleanup_old_data(retention_days INTEGER DEFAULT 2555)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM economic_data.economic_data 
    WHERE created_at < CURRENT_DATE - INTERVAL '1 day' * retention_days;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    INSERT INTO system_metadata.data_collection_runs 
    (start_time, end_time, status, total_records_collected, execution_metadata)
    VALUES 
    (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'completed', -deleted_count, 
     jsonb_build_object('operation', 'cleanup', 'retention_days', retention_days));
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION system_metadata.cleanup_old_data IS 'Remove dados antigos baseado na política de retenção';

-- Função para obter estatísticas do sistema
CREATE OR REPLACE FUNCTION system_metadata.get_system_stats()
RETURNS JSONB AS $$
DECLARE
    stats JSONB;
BEGIN
    SELECT jsonb_build_object(
        'total_indicators', (SELECT COUNT(*) FROM economic_data.indicators WHERE is_active = true),
        'total_records', (SELECT COUNT(*) FROM economic_data.economic_data),
        'latest_collection', (SELECT MAX(collection_timestamp) FROM economic_data.economic_data),
        'avg_quality_score', (SELECT ROUND(AVG(data_quality_score), 3) FROM economic_data.economic_data WHERE data_quality_score IS NOT NULL),
        'data_sources', (SELECT COUNT(*) FROM economic_data.data_sources WHERE is_active = true),
        'database_size', pg_size_pretty(pg_database_size(current_database()))
    ) INTO stats;
    
    RETURN stats;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION system_metadata.get_system_stats IS 'Retorna estatísticas gerais do sistema';

-- =====================================================
-- TRIGGERS PARA AUDITORIA
-- =====================================================

-- Função para atualizar timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers para atualizar updated_at
CREATE TRIGGER update_data_sources_updated_at 
    BEFORE UPDATE ON economic_data.data_sources 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_indicators_updated_at 
    BEFORE UPDATE ON economic_data.indicators 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_system_config_updated_at 
    BEFORE UPDATE ON system_metadata.system_config 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- PERMISSÕES E SEGURANÇA
-- =====================================================

-- Criar usuário para aplicação (se não existir)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'mba_app_user') THEN
        CREATE ROLE mba_app_user WITH LOGIN PASSWORD 'app_password_2025';
    END IF;
END
$$;

-- Conceder permissões
GRANT USAGE ON SCHEMA economic_data TO mba_app_user;
GRANT USAGE ON SCHEMA system_metadata TO mba_app_user;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA economic_data TO mba_app_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA system_metadata TO mba_app_user;

GRANT USAGE ON ALL SEQUENCES IN SCHEMA economic_data TO mba_app_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA system_metadata TO mba_app_user;

-- Permissões para views
GRANT SELECT ON economic_data.latest_indicators TO mba_app_user;
GRANT SELECT ON system_metadata.quality_summary TO mba_app_user;

-- =====================================================
-- FINALIZAÇÃO
-- =====================================================

-- Inserir log de inicialização
INSERT INTO system_metadata.data_collection_runs 
(start_time, end_time, status, execution_metadata)
VALUES 
(CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'completed', 
 jsonb_build_object('operation', 'database_initialization', 'version', '1.0.0'));

-- Mensagem de sucesso
DO $$
BEGIN
    RAISE NOTICE 'Banco de dados MBA Big Data inicializado com sucesso!';
    RAISE NOTICE 'Schemas criados: economic_data, system_metadata';
    RAISE NOTICE 'Usuário da aplicação: mba_app_user';
    RAISE NOTICE 'Execute SELECT system_metadata.get_system_stats(); para ver estatísticas';
END
$$;

