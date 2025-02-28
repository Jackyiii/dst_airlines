CREATE TABLE airport (
    AirportCode VARCHAR(10) PRIMARY KEY,
    Latitude DECIMAL(10, 6) NOT NULL,
    Longitude DECIMAL(10, 6) NOT NULL,
    CityCode VARCHAR(10) NOT NULL,
    CountryCode VARCHAR(10) NOT NULL,
    LocationType VARCHAR(50) NOT NULL,
    LanguageCode VARCHAR(10) NOT NULL,
    AirportName VARCHAR(255) NOT NULL,
    UtcOffset VARCHAR(10) NOT NULL,
    TimeZoneId VARCHAR(255) NOT NULL,
    FOREIGN KEY (CityCode) REFERENCES city(CityCode),
    FOREIGN KEY (CountryCode) REFERENCES country(CountryCode),
    FOREIGN KEY (LanguageCode) REFERENCES languages(LanguageCode)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;