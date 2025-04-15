from dataclasses import dataclass

@dataclass
class Ride:
    ride_uuid: str
    user_uuid: str
    driver_uuid: str
    distance: float
    price: float

@dataclass
class Driver:
    driver_uuid: str
    name: str
    surname: str
    car_uuid: str
    effective_from: str
    expiry_date: str

@dataclass
class User:
    user_uuid: str
    name: str
    surname: str
    is_driver: bool
