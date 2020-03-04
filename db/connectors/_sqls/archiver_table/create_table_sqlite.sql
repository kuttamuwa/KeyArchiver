-- 500 char limit will be handled on programming side.


CREATE TABLE IF NOT EXISTS "KEYARCHIVER"
(
    'id' PRIMARY KEY           NOT NULL,
    'KEY'         VARCHAR(500) NOT NULL,
    'DESCRIPTION' VARCHAR(500) NOT NULL,
    'ZAMAN'       DATETIME     NOT NULL, -- CREATED TIME
    'GRUP'        INTEGER                -- If text is larger than 500
)
