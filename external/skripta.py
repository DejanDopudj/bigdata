from datetime import datetime, timedelta
import random
import uuid
from concurrent.futures import ThreadPoolExecutor
import threading
from kafka_write import produce_to_kafka
import json


file_lock = threading.Lock()

def random_datetime(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

def generate_events_for_user(user_id):
    session_id = str(uuid.uuid4())
    country_id = str(random.randint(0, 190))
    start_time = datetime.now()
    event_id = 1
    event_date_time = start_time
    user_uuid = user_id#str(uuid.uuid4())
    event_date_time += timedelta(seconds=random.randint(25, 175))
    #sesssion_started_event
    create_event(session_id, "session_started" ,event_date_time, country_id,'','','', user_uuid)

    num_clicked_events = random.randint(0, 100)
    #randomly generate 0-100 clicked_on_product events
    for _ in range(num_clicked_events):
        event_date_time += timedelta(seconds=random.randint(25, 175))
        product_id = random.randint(0, 1000)
        player_name = ""
        if product_id % 3 == 0:
            player_name = "L. James"
        elif product_id % 3 == 1:
            player_name = "K. Durant"
        else:
            player_name = "S. Curry"
            
        product_id = str(product_id)
        create_event(session_id, "clicked_on_product", event_date_time, country_id, product_id, player_name, '', user_uuid)
        #Generate double event in 5% of cases
        if random.random() <= 0.05:
            create_event(session_id, "clicked_on_product", event_date_time, country_id, product_id, player_name,'', user_uuid)

        # Generate bought_product events with 3% chance for each clicked_on_product
        if random.random() <= 0.03:
            event_id += 1
            event_date_time += timedelta(seconds=random.randint(30, 144))
            cost = random.randint(0,50)
            create_event(session_id, "bought_product", event_date_time, country_id, product_id, player_name, cost, user_uuid)

    #session_ended_event
    event_id += 1
    event_date_time += timedelta(seconds=random.randint(1, 60))
    create_event(session_id, "session_ended", event_date_time, country_id,'', '','', user_uuid)

def create_event(session_id, name ,event_date_time, country_id, product_id, player_name, cost, user_id):
    data_list = [session_id, name, event_date_time.strftime('%Y-%m-%d %H:%M:%S'), country_id, product_id, player_name, cost, user_id]
    data_dict = {
        "session_id": data_list[0],
        "name": data_list[1],
        "event_date_time": data_list[2],
        "country_id": data_list[3],
        "product_id": data_list[4],
        "player_name": data_list[5],
        "cost": data_list[6],
        "user_id": data_list[7]
    }
    message_value = json.dumps(data_dict)
    produce_to_kafka("kafka:9092","kafka-topic",message_value)



num_users = 1

with ThreadPoolExecutor() as executor:
    executor.map(generate_events_for_user, range(1, num_users + 1))
