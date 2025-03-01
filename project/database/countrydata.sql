CREATE TABLE country (
    CountryCode VARCHAR(10) PRIMARY KEY,
    LanguageCode VARCHAR(10) NOT NULL,
    CountryName VARCHAR(255) NOT NULL,
    FOREIGN KEY (LanguageCode) REFERENCES languages(LanguageCode)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;