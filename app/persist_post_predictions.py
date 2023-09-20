import json
import requests
import os
import time
from pathlib import Path
import shutil
from json.decoder import JSONDecodeError
import logging
from datetime import datetime


def clean_json_file():
    """This function renames the actual json_file(json-server's file's source for the posted plates predictions)
    to it's version(datetime stamp) name and creates a new json source file with the prediction format.
    """

    # cleaned timestamp
    now = (
        str(datetime.now())
        .replace(" ", "")
        .replace(":", "")
        .replace(".", "")
        .replace("-", "")
    )

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
        f"/json_source/db_{now}.json",
    )

    # create new json source
    with open("/json_source/db.json", "w") as f:
        json.dump(data, f)


def main():
    server_url = "http://json_server/plate_pred"
    categories = [
        "placa_carro",
        "placa_carro_mercosul",
        "placa_moto",
        "placa_moto_mercosul",
    ]

    detect_dir = Path("/detect")
    old_det_dir = detect_dir / "old"
    while not [
        item
        for item in detect_dir.glob("*")
        if not os.path.samefile(item, old_det_dir)
        and not os.path.commonpath([item, old_det_dir]) == old_det_dir
    ]:
        time.sleep(0.5)

    latest_detection = sorted(
        [item
         for item in detect_dir.glob("*")
         if not os.path.samefile(item, old_det_dir)
         and not os.path.commonpath([item, old_det_dir]) == old_det_dir
         ],
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )[0].name

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
        # logging.info(log_files)
        if not any(logs_dict.values()):
            time.sleep(0.5)
            continue
        for category, log_file_list in logs_dict.items():
            for log_file in log_file_list:
                log_path = processed_plates_dir / category / log_file
                logging.info(log_path)

                # Read the contents of the log file as a JSON object
                try:
                    with open(log_path) as f:
                        log_data = json.load(f)
                except JSONDecodeError:
                    # logging.info(
                    #    "Invalid JSON data in log file, continuing...")
                    continue
                except FileNotFoundError:
                    # logging.info("Invalid file, continuing...")
                    continue
                # logging.info(log_data['results'])
                if not log_data["results"]:
                    os.remove(log_path)
                    # logging.info("Empty prediction, continuing...")
                    continue
                # Add the filename to the JSON object
                log_data["file"] = log_file
                log_data["category"] = category

                # Post the JSON object to the server
                response = requests.post(server_url, json=log_data)
                # logging.info("response:",response)
                while not response.ok:
                    
                    response = requests.post(server_url, json=log_data)
                    # logging.info("response:",response)
                    
                # Move the file to the posted directory if the post was successful    
                shutil.move(
                    log_path,
                    posted_plates_dir / category / log_file,
                )


if __name__ == "__main__":
    clean_json_file()
    time.sleep(5)
    main()
