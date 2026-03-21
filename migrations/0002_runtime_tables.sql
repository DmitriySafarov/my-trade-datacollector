CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    description TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE TABLE IF NOT EXISTS source_watermarks (
    source TEXT PRIMARY KEY,
    last_event_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE TABLE IF NOT EXISTS gap_events (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    gap_started_at TIMESTAMPTZ NOT NULL,
    gap_resolved_at TIMESTAMPTZ,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE TABLE IF NOT EXISTS verification_reports (
    id BIGSERIAL PRIMARY KEY,
    report_name TEXT NOT NULL,
    status TEXT NOT NULL,
    period_start TIMESTAMPTZ,
    period_end TIMESTAMPTZ,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    comments JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

INSERT INTO settings (key, value, description)
VALUES
    ('settings.refresh_seconds', '30'::jsonb, 'Refresh interval for runtime settings'),
    ('batch.default.count', '500'::jsonb, 'Default dual-trigger batch size'),
    ('batch.default.seconds', '2.0'::jsonb, 'Default dual-trigger time limit')
ON CONFLICT (key) DO NOTHING;
