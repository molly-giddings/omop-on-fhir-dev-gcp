import json
import os
from googleapiclient import discovery
from google.cloud import tasks_v2

# Create the FHIR dataset.
# Returns: response
def create_fhir_dataset(hc_client, dataset_id, dataset_parent):
    request = (
        hc_client.projects()
        .locations()
        .datasets()
        .create(parent=dataset_parent, body={}, datasetId=dataset_id)
    )
    response = request.execute()

    return response

# Create the FHIR store.
# Returns: response
def create_fhir_store(hc_client, fhir_version, fhir_store_id, fhir_store_parent):
    # Try to create the fhir store based on the specification.
    body = {"version": fhir_version,
            "enableUpdateCreate": True,
            "disableReferentialIntegrity": True}
    request = (
        hc_client.projects()
        .locations()
        .datasets()
        .fhirStores()
        .create(parent=fhir_store_parent, body=body, fhirStoreId=fhir_store_id)
    )
    response = request.execute()
    
    return response

# Load the FHIR resource to the FHIR store.
# Returns: response
def load_fhir_resource(hc_client, resource_path, resource_body):
    request = (
        hc_client.projects()
        .locations()
        .datasets()
        .fhirStores()
        .import_(name=resource_path, body=resource_body)
    )  
    response = request.execute()

    return response

#def patch_fhir_identification(base_url, resource_path, resource_type, resource_id, id_list):
#    import json
#
#    id_resource_path = f'{base_url}/{resource_path}/fhir/{resource_type}/{resource_id}'
#    headers = {"Content-Type": "application/json-patch+json"}
#
#    for id_list_value in id_list:
#        id_dict = { "value": id_list_value }
#        body = json.dumps([{"op": "add", "path": "/identifier/-", "value": id_dict }])  
#        print(f'PATCH {id_resource_path}')
#
#        response = identify_fhir('PATCH', id_resource_path, headers, body)  
#
#    return response  

# Creates update request to add identifiers to FHIR resource.
# Calls identify_fhir to add request to task queue.
# Returns: response
def update_fhir_json(base_url, resource_path, fhir_json, resource_type, resource_id, resource_identifier, id_list):

    id_resource_path = f'{base_url}/{resource_path}/fhir/{resource_type}/{resource_id}'
    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

    if resource_identifier == 'NOT_FOUND':
        fhir_json['identifier'] = [{ "value": id_list_value } for id_list_value in id_list]
    else:
        for id_list_value in id_list:
            fhir_json['identifier'].append({ "value": id_list_value })

    response = identify_fhir('UPDATE', id_resource_path, headers, fhir_json)

    return response    

# Adds request to task queue to avoid RATE LIMIT errors.
# Returns: response
def identify_fhir(identify_method, id_resource_path, headers, fhir_json):

    #print(f"Queuing {id_resource_path} with {identify_method}.")
    identify_task_dict = { "method": identify_method,
                           "resource_path": id_resource_path,
                           "request_header": headers,
                           "request_json": fhir_json }

    # Set the queue and cloud function variables.
    project = os.environ.get('GCP_PROJECT')    
    queue_name = os.environ.get('IDENTIFY_QUEUE')
    location = os.environ.get('IDENTIFY_QUEUE_LOCATION')
    identify_function = os.environ.get('FHIR_IDENTIFY_FUNCTION')
    service_account_email = os.environ.get('SA_EMAIL')

    # Build the url from the cloud function variables.
    url = f"https://{location}-{project}.cloudfunctions.net/{identify_function}"

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

    if isinstance(identify_task_dict, dict):
        # Convert dict to JSON string.
        identify_task_dict = json.dumps(identify_task_dict)

    if identify_task_dict is not None:
        # The API expects a payload of type bytes.
        converted_payload = identify_task_dict.encode()

        # Add the payload to the request body.
        task['http_request']['body'] = converted_payload  
    
    # Use the client to build and send the task.
    response = client.create_task(request={"parent": parent, "task": task})  

    return response     
