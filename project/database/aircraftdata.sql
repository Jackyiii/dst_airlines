DROP TABLE IF EXISTS aircraft;
CREATE TABLE aircraft (
    AircraftCode VARCHAR(255),
    LanguageCode VARCHAR(10),
    AircraftName VARCHAR(255),
    AirlineEquipCode VARCHAR(50)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;