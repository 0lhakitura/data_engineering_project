import uuid
import random
from datetime import datetime, timedelta
from models import Ride, Driver, User

# Create a list of 100 riders (users with is_driver=False)
HARD_CODED_RIDERS = []
for i in range(100):
    user_uuid = str(uuid.uuid4())
    name = f"RiderName{i}"
    surname = f"RiderSurname{i}"
    HARD_CODED_RIDERS.append(User(user_uuid, name, surname, is_driver=False))

# Create a list of 10 drivers.
HARD_CODED_DRIVERS = []
for i in range(10):
    driver_uuid = str(uuid.uuid4())
    name = f"DriverName{i}"
    surname = f"DriverSurname{i}"
    car_uuid = str(uuid.uuid4())
    effective_from = datetime.now().isoformat()
    expiry_date = (datetime.now() + timedelta(days=365)).isoformat()
    HARD_CODED_DRIVERS.append(Driver(driver_uuid, name, surname, car_uuid, effective_from, expiry_date))

# --- Business Logic Functions ---

def generate_fake_driver():
    """Select a random driver from the hardcoded driver list."""
    return random.choice(HARD_CODED_DRIVERS)

def generate_fake_user(is_driver=False):
    """
    If a driver is requested, returns a user representation of a driver.
    Otherwise, returns a random rider from the hardcoded riders.
    """
    if is_driver:
        chosen_driver = random.choice(HARD_CODED_DRIVERS)
        # Return a User object built from the driver info with is_driver=True
        return User(chosen_driver.driver_uuid, chosen_driver.name, chosen_driver.surname, is_driver=True)
    else:
        return random.choice(HARD_CODED_RIDERS)

def generate_fake_ride():
    """
    Creates a fake ride by randomly selecting a rider and a driver from the hardcoded lists.
    Calculates random distance and price.
    """
    ride_uuid = str(uuid.uuid4())
    user = generate_fake_user(is_driver=False)  # Always a rider for a ride
    driver = generate_fake_driver()
    distance = round(random.uniform(1.0, 20.0), 2)
    price = round(distance * random.uniform(1.0, 3.0), 2)
    ride = Ride(ride_uuid, user.user_uuid, driver.driver_uuid, distance, price)
    return ride, driver, user

def get_fake_ride_data():
    """
    Returns a dictionary with ride, driver, and user (rider) data.
    """
    ride, driver, user = generate_fake_ride()
    return {
        "ride": ride.__dict__,
        "driver": driver.__dict__,
        "user": user.__dict__
    }

# --- Data Retrieval Functions ---

def get_all_users():
    """
    Returns a list combining the riders and a user-representation of the drivers.
    """
    riders = [user.__dict__ for user in HARD_CODED_RIDERS]
    # Convert each Driver to a User-like dict.
    drivers_as_users = [{
        "user_uuid": d.driver_uuid,
        "name": d.name,
        "surname": d.surname,
        "is_driver": True
    } for d in HARD_CODED_DRIVERS]
    return riders + drivers_as_users

def get_riders():
    """Returns the list of hardcoded riders."""
    return [user.__dict__ for user in HARD_CODED_RIDERS]

def get_drivers():
    """Returns the list of hardcoded drivers."""
    return [d.__dict__ for d in HARD_CODED_DRIVERS]
