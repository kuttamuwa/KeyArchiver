-- 500 char limit will be handled on programming side.


CREATE TABLE IF NOT EXISTS "KEYARCHIVER"
(
    'ID'    INTEGER PRIMARY KEY AUTOINCREMENT,
    'KEY'   VARCHAR(500) NOT NULL,
    'TANIM' VARCHAR(500) NOT NULL,
    'ZAMAN' DATETIME     NOT NULL, -- CREATED TIME
    'GRUP'  INTEGER                -- If text is larger than 500
)
