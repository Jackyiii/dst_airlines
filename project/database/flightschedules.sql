CREATE TABLE schedules (
    ScheduleID SERIAL PRIMARY KEY,
    Duration INT NOT NULL,
    DepartureAirportCode VARCHAR(10) NOT NULL,
    DepartureTime TIMESTAMP NOT NULL,
    DepartureTerminal VARCHAR(50),
    ArrivalAirportCode VARCHAR(10) NOT NULL,
    ArrivalTime TIMESTAMP NOT NULL,
    ArrivalTerminal VARCHAR(50),
    AirlineID VARCHAR(10) NOT NULL,
    FlightNumber VARCHAR(50) NOT NULL,
    AircraftCode VARCHAR(10) NOT NULL,
    StopQuantity INT NOT NULL,
    DaysOfOperation VARCHAR(50) NOT NULL,
    EffectiveDate DATE NOT NULL,
    ExpirationDate DATE NOT NULL,
    FOREIGN KEY (DepartureAirportCode) REFERENCES airport(AirportCode),
    FOREIGN KEY (ArrivalAirportCode) REFERENCES airport(AirportCode),
    FOREIGN KEY (AirlineID) REFERENCES airline(AirlineID),
    FOREIGN KEY (AircraftCode) REFERENCES aircraft(AircraftCode)
);
