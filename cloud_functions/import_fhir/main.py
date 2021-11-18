
def import_fhir(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    import json
    import os
    import requests
    from google.cloud import tasks_v2

    file = event
    print(f"Processing file: {file['name']}.")

    # Set the queue and cloud function variables.
    project = os.environ.get('GCP_PROJECT')    
    queue_name = os.environ.get('TASK_QUEUE')
    location = os.environ.get('TASK_QUEUE_LOCATION')
    import_function = os.environ.get('FHIR_IMPORT_FUNCTION')
    service_account_email = os.environ.get('SA_EMAIL')

    url = f"https://{location}-{project}.cloudfunctions.net/{import_function}"

    # Create a client.
    client = tasks_v2.CloudTasksClient()

    # Construct the fully qualified queue name.
    parent = client.queue_path(project, location, queue_name)

    # Get the queue object.
    queue = client.get_queue(request=None, name=parent)

    # Create the task queue if it doesn't exist; update if it already exists.
    client.update_queue(request=None, queue=queue)

    # Construct the request body.
    task = {
        "http_request": {  # Specify the type of request.
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,  # The full url path that the task will be sent to.
            "oidc_token": {"service_account_email": service_account_email, "audience": url},
        }
    }

    if isinstance(file, dict):
        # convert dict to JSON string
        file = json.dumps(file)

    if file is not None:
        # The API expects a payload of type bytes
        converted_payload = file.encode()

        # Add the payload to the request body
        task['http_request']['body'] = converted_payload  
    
    # Use the client to build and send the task.
    response = client.create_task(request={"parent": parent, "task": task})

    return "Task added to queue for processing."
