CREATE TABLE IF NOT EXISTS user (
    user_id TEXT NOT NULL UNIQUE,
    account TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    verified_at INTEGER,
    expired_at INTEGER,
    disabled_at INTEGER,
    roles TEXT,
    password TEXT NOT NULL,
    salt TEXT NOT NULL,
    name TEXT NOT NULL,
    info TEXT,
    PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS client (
    client_id TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    client_secret TEXT,
    redirect_uris TEXT NOT NULL,
    scopes TEXT NOT NULL,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    image_url TEXT,
    PRIMARY KEY (client_id)
);

INSERT INTO user (user_id,account,created_at,modified_at,verified_at,roles,password,salt,name,info) VALUES ('admin','admin',0,0,0,'{}','27258772d876ffcef7ca2c75d6f4e6bcd81c203bd3e93c0791c736e5a2df4afa','YsBsou2O','Admin','{}');
INSERT INTO client (client_id,created_at,modified_at,redirect_uris,scopes,user_id,name) VALUES ('public',0,0,'http://localhost:1080/auth/oauth2/redirect','','admin','Public');
INSERT INTO client (client_id,client_secret,created_at,modified_at,redirect_uris,scopes,user_id,name) VALUES ('private','secret',0,0,'http://localhost:1080/auth/oauth2/redirect','','admin','Private');
