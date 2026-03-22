ALTER TABLE schema_migrations
    ALTER COLUMN applied_at SET DEFAULT now();

ALTER TABLE settings
    ALTER COLUMN updated_at SET DEFAULT now();

ALTER TABLE source_watermarks
    ALTER COLUMN updated_at SET DEFAULT now();

ALTER TABLE gap_events
    ALTER COLUMN created_at SET DEFAULT now();

ALTER TABLE verification_reports
    ALTER COLUMN created_at SET DEFAULT now();
