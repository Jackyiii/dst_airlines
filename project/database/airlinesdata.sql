CREATE TABLE airline (
    AirlineID VARCHAR(10) PRIMARY KEY,
    AirlineID_ICAO VARCHAR(10) NOT NULL,
    LanguageCode VARCHAR(10) NOT NULL,
    AirlineName VARCHAR(255) NOT NULL,
    FOREIGN KEY (LanguageCode) REFERENCES languages(LanguageCode)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;