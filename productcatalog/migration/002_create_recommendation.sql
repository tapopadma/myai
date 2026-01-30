CREATE TYPE recommendation_status AS ENUM ('CREATED', 'DELIVERED', 'FAILED');

CREATE TABLE IF NOT EXISTS Recommendations (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    status recommendation_status NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

