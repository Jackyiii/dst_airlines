CREATE TABLE aircraft (
    AircraftCode VARCHAR(10) PRIMARY KEY,
    LanguageCode VARCHAR(10) NOT NULL,
    AircraftName VARCHAR(255) NOT NULL,
    AirlineEquipCode VARCHAR(50) NOT NULL,
    FOREIGN KEY (LanguageCode) REFERENCES languages(LanguageCode)
);

