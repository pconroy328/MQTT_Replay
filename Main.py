
import sys
import json
import paho.mqtt.client as mqtt
from datetime import datetime
import time

file_path = "/home/pconroy/mqttdata.13May24.txt"
output_file_path = "/home/pconroy/output.txt"
new_topic_prefix = None

def parse_mqtt_data_file(new_topic_prefix, replace_dateTime):
    global file_path
    global output_file_path
    print('New topic prefix', new_topic_prefix)

    try:
        with open(file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
            json_buffer = ""  # Buffer to accumulate lines for incomplete JSON
            for line in input_file:
                json_buffer += line.strip()  # Add the current line to the buffer
                #print(json_buffer)
                try:
                    data = json.loads(json_buffer)
                    # Get today's date and time in the required format
                    if replace_dateTime:
                        current_date_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
                        # Update the dateTime field with today's date and time
                        data['dateTime'] = current_date_time

                    if new_topic_prefix is not None:
                        try:
                            print(json_buffer)
                            ##input('press enter')
                            old_topic = data['topic']
                            data['topic'] = new_topic_prefix + old_topic
                        except Exception as e:
                            print(f"JSON message missing a topic value: {e}")
                            data['topic'] = new_topic_prefix

                    # Write the modified JSON back to the output file
                    output_file.write(json.dumps(data) + '\n')
                    json_buffer = ""  # Reset the buffer after successfully parsing JSON
                except json.JSONDecodeError as e:
                    # JSON is incomplete, continue accumulating lines
                    #print(e)
                    continue
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

## -------------------------------------------------------------------------------
def publish_to_mqtt(file_path, mqtt_broker, new_topic_prefix, replace_dateTime):
    # Create an MQTT client instance
    client = mqtt.Client()

    # Connect to the MQTT broker
    client.connect(mqtt_broker)

    current_time = datetime.now()
    last_message_date_time = None

    try:
        # Open the output file
        with open(file_path, 'r') as input_file:
        #with open(output_file_path, 'r') as output_file:
            json_buffer = ""  # Buffer to accumulate lines for incomplete JSON
            for line in input_file:
                json_buffer += line.strip()  # Add the current line to the buffer

                try:
                    data = json.loads(json_buffer)

                    ##
                    ## If we're here - then we have a complete JSON message.
                    ## Time to do some optional subsitutions and then send it
                    ##
                    ## First figure out the delay between the last and this message
                    try:
                        message_date_time = datetime.strptime(data['dateTime'], '%Y-%m-%dT%H:%M:%S%z')
                        if last_message_date_time is None:
                            delay_seconds = 0
                        else:
                            time_difference = message_date_time - last_message_date_time
                            delay_seconds = time_difference.seconds

                        last_message_date_time = message_date_time
                        if delay_seconds > 60:
                            print('bad dateTime - very long delay')
                            delay_seconds = 0
                        elif delay_seconds < 0:
                            print('bad dateTime - negative delay')
                            delay_seconds = 0
                    except Exception as e:
                        print('Exception, message probably missing dateTime')
                        delay_seconds = 0

                    if replace_dateTime:
                        # Get today's date and time in the required format
                        current_date_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
                        # Update the dateTime field with today's date and time
                        data['dateTime'] = current_date_time

                    if new_topic_prefix is not None:
                        try:
                            old_topic = data['topic']
                            data['topic'] = new_topic_prefix + old_topic
                        except Exception as e:
                            print(f"JSON message missing a topic value: {e}")
                            data['topic'] = new_topic_prefix

                    ## Now we delay the right amount of time between the messages
                    time.sleep(delay_seconds)
                    ## Send it!
                    #print(json.dumps(data));
                    #input('press enter')
                    client.publish(data['topic'], json.dumps(data))

                    json_buffer = ""  # Reset the buffer after successfully parsing JSON
                except json.JSONDecodeError as e:
                    # JSON is incomplete, continue accumulating lines
                    #print(e)
                    continue
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Disconnect from the MQTT broker
        client.disconnect()

# Example usage:
    

if __name__ == "__main__":
    if len(sys.argv) < 1:
        print("Usage: python program.py input_file_path output_file_path newprefix")
        sys.exit(1)

    ##input_file_path = sys.argv[1]
    ##output_file_path = sys.argv[2]
    ##try:
    ##    new_topic_prefix = sys.argv[3]
    ##except:
    ##    new_topic_prefix = None
    new_topic_prefix = "FOO/"

    #qparse_mqtt_data_file(new_topic_prefix, False)
    publish_to_mqtt(file_path, "gx100.lan", new_topic_prefix, True)
