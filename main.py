import os
import json
from google.cloud import pubsub_v1
from fastapi import FastAPI

app = FastAPI()

#add project
os.environ["PROJECT_ID"] = ''


# Publishes a message to a Cloud Pub/Sub topic.
@app.get('/')
async def publish(request):
    record = json.loads(json.dumps(request))
    request_data = json.loads(record)
    print(f'Request_data_json: {request_data}')
    PROJECT_ID = os.environ.get('PROJECT_ID')
    print(f'ProjectId {PROJECT_ID}')
    # Instantiates a Pub/Sub client
    publisher = pubsub_v1.PublisherClient()

    topic_name = request_data["topic"]
    message = request_data["message"]

    print(f'Request_data_topic: {topic_name}')
    print(f'Request_data_message: {message}')

    if not topic_name or not message:
        return ('Missing "topic" and/or "message" parameter.', 400)

    print(f'Publishing message to topic {topic_name}')

    # References an existing topic
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)
    print(f'Topic path {topic_path}')

    message_json = json.dumps(message)
    message_bytes = message_json.encode('utf-8')

    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return e, 500
