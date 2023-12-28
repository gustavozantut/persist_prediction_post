import json
import requests
import os

import time
from pathlib import Path
import shutil
from json.decoder import JSONDecodeError
import logging
from collections import deque
from datetime import datetime
from kafka import KafkaProducer
from PIL import Image, ImageDraw, ImageFont
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "192.168.0.101:9092,192.168.14.2:9092,192.168.14.2:9093"
start_time = (
    str(datetime.now())
    .replace(" ", "")
    .replace(":", "")
    .replace(".", "")
    .replace("-", "")
)
TOPIC_NAME = "plate_detector"
last_frames = deque(maxlen=15)
server_url = "http://0.0.0.0/plate_pred"
categories = [
    "placa_carro",
    "placa_carro_mercosul",
    "placa_moto",
    "placa_moto_mercosul",
]
detect_dir = Path("/detect")
old_det_dir = detect_dir / "old"

def configure_logging():
    
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create a file handler
    file_handler = logging.FileHandler('app.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

def create_kafka_topic():

    # Create an AdminClient instance
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # Define topic configuration
    topic_config = {
        "topic": TOPIC_NAME,
        "partitions": 1,
        "replication.factor": 3,  # Set the desired replication factor
        "config": {
            "min.insync.replicas": 2  # Set the desired minimum in-sync replicas
        }
    }

    # Create a NewTopic instance
    new_topic = NewTopic(
        topic_config["topic"],
        num_partitions=topic_config["partitions"],
        replication_factor=topic_config["replication.factor"],
        config={
            "min.insync.replicas": str(topic_config["config"]["min.insync.replicas"])
        }
    )
    # Create the topic
    admin_client.create_topics([new_topic])

def clean_json_file():
    """This function renames the actual json_file(json-server's file's source for the posted plates predictions)
    to it's version(datetime stamp) name and creates a new json source file with the prediction format.
    """

    # json structure
    data = {
        "plate_pred": [
            {   
                "id": "",
                "version": "",
                "data_type": "",
                "epoch_time": "",
                "img_width": "",
                "img_height": "",
                "processing_time_ms": "",
                "regions_of_interest": [],
                "results": [],
                "file": "",
                "category": "",
            }
        ]
    }

    # move last json source
    shutil.move(
        "/json_source/db.json",
        f"/json_source/db_{start_time}.json",
    )

    # create new json source
    with open("/json_source/db.json", "w") as f:
        json.dump(data, f)

def get_latest_detectiion_folder_name(detect_dir=detect_dir,old_det_dir=old_det_dir):
    
    latest_detectiion_folder_name=sorted(
        [item
         for item in detect_dir.glob("*")
         if not os.path.samefile(item, old_det_dir)
         and not os.path.commonpath([item, old_det_dir]) == old_det_dir
         ],
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )[0].name
    
    return latest_detectiion_folder_name

def draw_plate_on_img(image_path,frame_name,plate):
    
    image = Image.open(image_path)
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default(size=40)
    image_width, image_height = image.size
    original_center_position = ((image_width - 40) // 2, image_height - 45 - 50)
    shift = 200 if frame_name not in last_frames else -200
    new_position = (original_center_position[0] - shift, original_center_position[1]) 
    text_color = "black"
    bbox = draw.textbbox(new_position, plate, font=font)
    draw.rectangle(bbox, fill="red")
    draw.text(new_position, plate, font=font, fill=text_color)
    image.save(image_path)

def main():
    
    logging.info("Reaching for kafka...")
    
    producer_on = False
    
    while not producer_on:
        
        try:
            
            producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,\
                    acks='all')
            producer_on = True
        
        except:
            
            pass

    while not [
        item
        for item in detect_dir.glob("*")
        if not os.path.samefile(item, old_det_dir)
        and not os.path.commonpath([item, old_det_dir]) == old_det_dir
    ]:
        
        time.sleep(0.5)

    latest_detection = get_latest_detectiion_folder_name()

    processed_plates_dir = Path(
        "/logs") / latest_detection / "processed_plates"
    posted_plates_dir = Path("/logs") / latest_detection / "posted_plates"
    pred_files_dir_placa_carro = processed_plates_dir / categories[0]

    for category in categories:
        
        os.makedirs(posted_plates_dir / category, exist_ok=True)

    while not os.path.exists(pred_files_dir_placa_carro):
        
        time.sleep(0.5)

    while True:
        
        logs_dict = {
            categories[0]: [],
            categories[1]: [],
            categories[2]: [],
            categories[3]: [],
        }

        for category in categories:
            
            # Get a list of all files in the log directory
            logs_dict[category] = sorted(
                [
                    f
                    for f in os.listdir(processed_plates_dir / category)
                    if f.endswith(".log")
                ]
            )
            
        if not any(logs_dict.values()):
            
            time.sleep(0.5)
            continue
        
        for category, log_file_list in logs_dict.items():
            
            for log_file in log_file_list:
                
                log_path = processed_plates_dir / category / log_file

                # Read the contents of the log file as a JSON object
                try:
                    
                    with open(log_path) as f:
                        
                        log_data = json.load(f)
                        
                except JSONDecodeError:

                    continue
                
                except FileNotFoundError:
                    
                    continue

                if not log_data["results"]:
                    
                    os.remove(log_path)

                    continue
                
                # Add the filename to the JSON object
                log_data["file"] = log_file
                log_data["category"] = category
                # Post the JSON object to the server
                response = requests.post(server_url, json=log_data)
                
                while not response.ok:
                    
                    response = requests.post(server_url, json=log_data)
                    # logging.info("response:",response)
                
                logging.info(f"file: {log_file} posted to json server!")
                shutil.move(
                    log_path,
                    posted_plates_dir / category / log_file,
                )
                msg_sent = False
                
                while not msg_sent:
                    
                    try:
                        
                        producer.flush()
                        producer.send(TOPIC_NAME, value=json.dumps(log_data).encode('utf-8')).get()
                        producer.flush()
                        msg_sent = True
                        
                    except:
                        
                        create_kafka_topic()
                        
                        pass
                
                logging.info(f"file: {log_file} sent to kafka!")
                
                plate = log_data["results"][0]["plate"]
                logging.info(f"writting plate: {plate} to frame!")
                frames_dir = Path("/detect") / latest_detection / "frames"
                frame_name = log_file.split("_")[0]
                draw_plate_on_img(image_path=frames_dir / (frame_name + ".png"),frame_name=frame_name, plate=plate)
                last_frames.append(frame_name)
                logging.info(f"written!")
                
                                    
if __name__ == "__main__":
    
    clean_json_file()
    create_kafka_topic()
    configure_logging()
    time.sleep(5)
    main()
