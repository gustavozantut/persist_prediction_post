import json
import requests
import os
import time
from pathlib import Path
import shutil
from json.decoder import JSONDecodeError
import logging

def clean_json_file():
    data = {
        "plate_pred": [
            {
                "id": 0,
                "date": "",
                "file": "",
                "category": "",
                "version": "",
                "data_type": "",
                "epoch_time": "",
                "img_width": "",
                "img_height": "",
                "processing_time_ms": "",
                "regions_of_interest": [],
                "results": []
                }
            ]
        }
    with open('/json_source/db.json', 'w') as f:
        json.dump(data, f)
def main():
    server_url = 'http://json_server/plate_pred'
    category = 'placa_carro'
    detect_dir = Path('/detect')
    old_det_dir = detect_dir / 'old'
    while not [item for item in detect_dir.glob('*') if not os.path.samefile(item, old_det_dir) and not os.path.commonpath([item, old_det_dir]) == old_det_dir]:
        time.sleep(0.5)
    latest_detection = sorted([item for item in detect_dir.glob('*') if not os.path.samefile(item, old_det_dir) and not os.path.commonpath([item, old_det_dir]) == old_det_dir], key=lambda x: x.stat().st_mtime, reverse=True)[0].name
    pred_files_dir = Path('/logs') / latest_detection / 'processed_plates' / category
    logs_dir = Path('/logs') / latest_detection / 'posted_plates' / category
    os.makedirs(logs_dir, exist_ok=True)

    while not os.path.exists(pred_files_dir):
        time.sleep(0.5)
        
    id=1
    while True:
        # Get a list of all files in the log directory
        log_files = [f for f in os.listdir(pred_files_dir) if f.endswith('.log')]
        #logging.info(log_files)
        for log_file in sorted(log_files):
            
            log_path = Path(pred_files_dir) / log_file
            #logging.info(log_path)
            
            # Read the contents of the log file as a JSON object
            try:
                with open(log_path) as f:
                    log_data = json.load(f)
            except JSONDecodeError:
                #logging.info("Invalid JSON data in log file, continuing...")
                continue
            #logging.info(log_data['results'])
            if not log_data['results']:
                os.remove(log_path)
                #logging.info("Empty prediction, continuing...")
                continue
            # Add the filename to the JSON object
            log_data["id"] = id
            log_data["file"] = log_file
            log_data["category"] = category
            
            # Post the JSON object to the server
            response = requests.post(server_url, json=log_data)
            #logging.info("response:",response)
            while not response.ok:
                # Move the file to the posted directory if the post was successful
                response = requests.post(server_url, json=log_data)
                #logging.info("response:",response)
            id += 1  
            shutil.move(log_path, logs_dir / log_file)
        else:
            # Wait for some time before checking for new files again
            time.sleep(0.5)
            new_files = sorted(os.listdir(pred_files_dir))
            if new_files != log_files:
                    #logging.info("New files detected. Processing...")
                    log_files = new_files
if __name__ == '__main__':
    clean_json_file()
    main()