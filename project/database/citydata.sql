CREATE TABLE city (
    CityCode VARCHAR(10) PRIMARY KEY,
    CountryCode VARCHAR(10) NOT NULL,
    LanguageCode VARCHAR(10) NOT NULL,
    CityName VARCHAR(255) NOT NULL,
    UtcOffset VARCHAR(10) NOT NULL,
    TimeZoneId VARCHAR(255) NOT NULL,
    AirportCode VARCHAR(10),
    FOREIGN KEY (CountryCode) REFERENCES country(CountryCode),
    FOREIGN KEY (LanguageCode) REFERENCES languages(LanguageCode)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;