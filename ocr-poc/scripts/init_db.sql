CREATE TABLE IF NOT EXISTS receipts (
    id SERIAL PRIMARY KEY,
    image BYTEA NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(32) NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    file_size BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS ocr_queue (
    id SERIAL PRIMARY KEY,
    receipt_id INTEGER NOT NULL REFERENCES receipts(id),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    priority VARCHAR(50) DEFAULT 'normal',
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    CONSTRAINT valid_status CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    CONSTRAINT valid_priority CHECK (priority IN ('high', 'normal', 'low'))
);

CREATE TABLE IF NOT EXISTS ocr_results (
    id SERIAL PRIMARY KEY,
    receipt_id INTEGER NOT NULL REFERENCES receipts(id),
    raw_text TEXT,
    merchant_name VARCHAR(255),
    total_amount DECIMAL(10,2),
    receipt_date DATE,
    confidence_score INTEGER,
    extracted_data JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ocr_queue_status ON ocr_queue(status);
CREATE INDEX IF NOT EXISTS idx_ocr_queue_receipt_id ON ocr_queue(receipt_id);
CREATE INDEX IF NOT EXISTS idx_receipts_file_hash ON receipts(file_hash);