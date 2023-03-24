import json
import requests
import os
import time
from pathlib import Path
import shutil

def main():
    server_url = "json_server/plate_pred"
    category = 'placa_carro'
    detect_dir = Path('/detect')
    latest_detection = sorted(detect_dir.glob('*'), key=lambda x: x.stat().st_mtime, reverse=True)[0].name
    pred_files_dir = Path('/logs') / latest_detection / 'processed_plates' / category
    logs_dir = Path('/logs') / latest_detection / 'posted_plates' / category
    logs_dir.mkdir(exist_ok=True)
    posted_plates_log_file = logs_dir / 'posted_plates.log'
    
    if not posted_plates_log_file.exists():
        posted_plates_log_file.touch()
    
    while not os.path.exists(detect_dir):
        time.sleep(0.5)
    id=1
    while True:
        # Get a list of all files in the log directory
        log_files = os.listdir(pred_files_dir)

        for log_file in sorted(log_files):
            log_path = Path(pred_files_dir) / log_file
            
            # Read the contents of the log file as a JSON object
            with open(log_path) as f:
                log_data = json.load(f)
            if not log_data['results']:
                os.remove(log_path)
                continue
            # Add the filename to the JSON object
            log_data["id"] = id
            log_data["filename"] = log_file
            log_data["category"] = category
            
            # Post the JSON object to the server
            response = requests.post(server_url, json=log_data)

            while not response.ok:
                # Move the file to the posted directory if the post was successful
                response = requests.post(server_url, json=log_data)
                
            shutil.move(log_path, pred_files_dir / 'posted_files' / log_file)
        else:
            # Wait for some time before checking for new files again
            time.sleep(1)
            new_files = sorted(os.listdir(pred_files_dir))
            if new_files != files:
                    #logging.info("New files detected. Processing...")
                    files = new_files
if __name__ == '__main__':
    time.sleep(5)
    main()