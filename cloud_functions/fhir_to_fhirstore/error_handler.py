import google.auth.transport.requests
import google.oauth2.id_token  
import os
import requests
import json
from google.cloud import pubsub_v1

def report_failure(failure: dict) -> dict:
 #   url: str = os.environ.get(
 #       'ERROR_HANDLER_URL',
 #       'https://us-central1-aou-curation-omop-dev.cloudfunctions.net/fhir_to_fhirstore_error_handler'
 #   )

#    req = make_authorized_request(
#        method='POST',
#        url=url,
#        json=failure,
#    ).prepare()

#    sess = requests.Session()
#    response = sess.send(req)
#    print(f'Error handler responded with {response.status_code}')

    response = publish(failure)

    return failure


def make_authorized_request(url: str, json: dict, method: str = 'GET'):
    """
    make_authorized_get_request makes a GET request to the specified HTTP endpoint
    in service_url (must be a complete URL) by authenticating with the
    ID token obtained from the google-auth client library.
    """

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)

    headers: dict = {'Authorization': f'Bearer {id_token}'}

    return requests.Request(method=method, url=url, json=json, headers=headers)



# Publishes a message to a Cloud Pub/Sub topic.
def publish(request):

    # Instantiates a Pub/Sub client
    publisher = pubsub_v1.PublisherClient()

    # References an existing topic
    project_id = 'aou-curation-omop-dev'
    topic_name = 'site-fhir-data-error-handler'
    topic_path = publisher.topic_path(project_id, topic_name)

    message_json = json.dumps({
        'data': {'message': request},
    })
    message_bytes = message_json.encode('utf-8')

    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return (e, 500)
