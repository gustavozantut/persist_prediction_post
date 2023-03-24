import json
import requests
import time
import inotify.adapters

def main():
    
    # Initialize an empty list for the plate predictions
    plate_pred = []
    id_count = 1
    filename = None

    # Initialize inotify
    i = inotify.adapters.Inotify()

    # Add the log file to watch for modification events
    i.add_watch('/logs/', mask=inotify.constants.IN_MODIFY)

    # Keep looping and processing events
    for event in i.event_gen(yield_nones=False):
        (_, type_names, path, filename) = event

        # If the event type is a modification and the filename matches the log file, process the new lines
        if 'IN_MODIFY' in type_names and 'alpr_' in filename:
            with open(path + filename, 'r') as f_in, open('/tf/GitHub/Toll/runs/detect/yolo_alpr_det/crops/placa_carro/logs/output.json', 'w') as f_out:
                # Process the new lines in the log file
                for line in f_in:
                    # If line starts with ".", it is a file path, so extract the filename
                    if line.startswith('.'):
                        filename = line.replace(".","").replace("/","").strip()

                    # Otherwise, it is a JSON string, so parse it and extract the necessary information
                    else:
                        data = json.loads(line)
                        if data['results']:
                            result = data['results']
                            plate_pred.append({
                                'id':id_count,
                                'date':time.strftime("%Y%m%d%H%M%S"),
                                'file': filename,
                                'version': data['version'],
                                'data_type': data['data_type'],
                                'epoch_time': data['epoch_time'],
                                'img_width': data['img_width'],
                                'img_height': data['img_height'],
                                'processing_time_ms': data['processing_time_ms'],
                                'regions_of_interest': data['regions_of_interest'],
                                'results': result,
                            })
                            id_count+=1
                            # Write the plate predictions to the output file as a JSON object
                            url = 'http://host.docker.internal:8081/plate_pred'
                            headers = {'Content-Type': 'application/json'}
                            response = requests.post(url, headers=headers, json=plate_pred[0])
                            while response.status_code != 201:
                                print(f"Failed to post data to {url}. Status code: {response.status_code}")
                                response
                            # Write the plate predictions to the output file as a JSON object
                            json.dump({'plate_pred': plate_pred}, f_out)
                            f_out.write('\n')

                            # Clear the plate predictions list for the next iteration
                            plate_pred = []
if __name__ == '__main__':
    main()