CREATE TABLE IF NOT EXISTS apps (
    id          VARCHAR(255) NOT NULL PRIMARY KEY,
    key         VARCHAR(255) NOT NULL UNIQUE,
    secret      VARCHAR(255) NOT NULL,
    name        VARCHAR(255) NOT NULL DEFAULT '',
    capacity    BIGINT NOT NULL DEFAULT 0,
    client_messages_enabled     BIGINT NOT NULL DEFAULT 0,
    subscription_count_enabled  BIGINT NOT NULL DEFAULT 0,
    enabled     BIGINT NOT NULL DEFAULT 1,
    webhooks    TEXT NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
