CREATE TABLE status_flight (
    FlightNumber VARCHAR(50) PRIMARY KEY,
    DepartureDate DATE NOT NULL,
    DepartureAirportCode VARCHAR(10) NOT NULL,
    ScheduledDepartureLocalTime TIMESTAMP NOT NULL,
    ActualDepartureLocalTime TIMESTAMP,
    DepartureTerminal VARCHAR(50),
    DepartureGate VARCHAR(50),
    ArrivalAirportCode VARCHAR(10) NOT NULL,
    ScheduledArrivalLocalTime TIMESTAMP NOT NULL,
    ActualArrivalLocalTime TIMESTAMP,
    ArrivalTerminal VARCHAR(50),
    ArrivalGate VARCHAR(50),
    TimeStatusCode VARCHAR(10),
    TimeStatusDefinition VARCHAR(255),
    MarketingAirlineID VARCHAR(10) NOT NULL,
    MarketingFlightNumber VARCHAR(50) NOT NULL,
    AircraftCode VARCHAR(10) NOT NULL,
    AircraftRegistration VARCHAR(50),
    FlightStatusCode VARCHAR(10),
    FlightStatusDefinition VARCHAR(255),
    FOREIGN KEY (DepartureAirportCode) REFERENCES airport(AirportCode),
    FOREIGN KEY (ArrivalAirportCode) REFERENCES airport(AirportCode),
    FOREIGN KEY (MarketingAirlineID) REFERENCES airline(AirlineID),
    FOREIGN KEY (AircraftCode) REFERENCES aircraft(AircraftCode)
);