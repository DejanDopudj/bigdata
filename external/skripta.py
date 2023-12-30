import csv
from datetime import datetime, timedelta
import random
import uuid
from concurrent.futures import ThreadPoolExecutor
import threading

file_lock = threading.Lock()

def random_datetime(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

def generate_events_for_user(user_id):
    file_path = 'events.csv'
    session_id = str(uuid.uuid4())
    country_id = str(random.randint(0, 190))
    start_time = datetime.now()

    event_id = 1
    event_date_time = start_time
    user_uuid = user_id#str(uuid.uuid4())
    event_date_time += timedelta(seconds=random.randint(25, 175))
    #sesssion_started_event
    create_event(file_path, session_id, "session_started" ,event_date_time, country_id,'', user_uuid)

    num_clicked_events = random.randint(0, 100)
    #randomly generate 0-100 clicked_on_product events
    for _ in range(num_clicked_events):
        event_date_time += timedelta(seconds=random.randint(25, 175))
        product_id = str(random.randint(0, 1000))
        create_event(file_path, str(uuid.uuid4()), "clicked_on_product", event_date_time, country_id, product_id, user_uuid)

        # Generate bought_product events with 3% chance for each clicked_on_product
        if random.random() <= 0.03:
            event_id += 1
            event_date_time += timedelta(seconds=random.randint(30, 144))
            create_event(file_path, str(uuid.uuid4()), "bought_product", event_date_time, country_id, product_id, user_uuid)

    #session_ended_event
    event_id += 1
    event_date_time += timedelta(seconds=random.randint(1, 60))
    create_event(file_path, session_id, "session_ended", event_date_time, country_id, '', user_uuid)

def create_event(file_path, session_id, name ,event_date_time, country_id, product_id, user_id):
    write_event(file_path, [session_id, name, event_date_time.strftime('%Y-%m-%d %H:%M:%S'), country_id, product_id, user_id])


def write_event(file_path, event_data):
    with file_lock:
        with open(file_path, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(event_data)

file_path = 'events.csv'
num_users = 20

with ThreadPoolExecutor() as executor:
    executor.map(generate_events_for_user, range(1, num_users + 1))
