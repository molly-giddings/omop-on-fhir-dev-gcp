
def fhir_to_fhirstore(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    import json
    import os
    from google.cloud import storage
    from googleapiclient import discovery, errors

    # Decode the payload.
    request_json = request.data.decode('utf8').replace("'", '"')
    try:
        request_json = json.loads(request_json)
    except:
        print('Bad request.')
        return 'Bad request'
    file_bucket = request_json['bucket'] # The triggering file bucket.
    file_name = request_json['name'] # The triggering file name.
    
    if not file_name.endswith('.json'):
        return 'Skipping non-json file'
        
    # Instantiates a Cloud Storage client.
    storage_client = storage.Client()

    # Create a bucket object for the triggering file bucket.
    bucket = storage_client.get_bucket(file_bucket)

    # Create a blob object from the filepath for the triggering file name.
    blob = bucket.blob(file_name)

    # Download the file as string and convert to json.
    fhir_file = blob.download_as_string()
    fhir_file = fhir_file.decode('utf-8')
    fhir_file_json = json.loads(fhir_file)    

    # Parse the resource type out of the file and construct the payload.
    file_uri = f'gs://{file_bucket}/{file_name}'
    resource_type = find_by_key(fhir_file_json, 'resourceType') 
    if resource_type.lower() == 'not_found':
        print(f'resourceType not found in {file_name}')
        return 'Skipping - no resource type'
    elif resource_type.lower() == 'bundle':
        resource_body = { "contentStructure": "BUNDLE_PRETTY", "gcsSource": { "uri": file_uri } } 
    else:
        resource_body = { "contentStructure": "RESOURCE_PRETTY", "gcsSource": { "uri": file_uri } }    

    # Read some environment variables; set some others.
    project_id = os.environ.get('GCP_PROJECT','MY_PROJECT')
    location = os.environ.get('FHIR_DATASET_LOCATION','MY_LOCATION')
    dataset_id = os.environ.get('FHIR_DATASET', 'site_fhir_data')
    api_version = os.environ.get('API_VERSION', 'v1beta1')
    service_name = 'healthcare'
    response = 'Import complete'
    
    # Instantiates an authorized API client by discovering the Healthcare API.
    hc_client = discovery.build(service_name, api_version)

    # Try to create the fhir store dataset if it does not exist yet.
    dataset_parent = "projects/{}/locations/{}".format(project_id, location)
    try:
        request = (
            hc_client.projects()
            .locations()
            .datasets()
            .create(parent=dataset_parent, body={}, datasetId=dataset_id)
        )
        response = request.execute()
    except errors.HttpError as exc:
        print(f'Dataset {dataset_id} exists already. {exc.resp["status"]}')
    fhir_store_parent = "projects/{}/locations/{}/datasets/{}".format(
        project_id, location, dataset_id
    )

    fhir_version, fhir_store_id = name_resources(file_name, fhir_file_json)
            
    if fhir_store_id == 'NOT_FOUND':
        fhir_version_list = ['R4', 'STU3', 'DSTU2']
        for fhir_version in fhir_version_list:
            fhir_store_id = f'fhir-dev-{fhir_version}'

            try:
                response = create_fhir_store(hc_client, fhir_version, fhir_store_id, fhir_store_parent)
                print(f'Created FHIR store: {fhir_store_id}')
            except errors.HttpError as exc:
                print(f'FHIR store {fhir_store_id} exists already. {exc.resp["status"]}')

            try:
                # Build the import statement to load the FHIR resource file into the store.
                resource_path = "projects/{}/locations/{}/datasets/{}/fhirStores/{}".format(
                    project_id, location, dataset_id, fhir_store_id
                )

                request = (
                    hc_client.projects()
                    .locations()
                    .datasets()
                    .fhirStores()
                    .import_(name=resource_path, body=resource_body)
                )  
                response = request.execute()
                print(response)

            except errors.HttpError as exc:
                print(f'Failed to post to {fhir_store_id}. {exc.resp["status"]}')

                # If rate limit exceeded or connection error, raise the error so it will be retried
                if exc.resp.status in [429, 500, 503]:
                    raise

    else:
        try:
            response = create_fhir_store(hc_client, fhir_version, fhir_store_id, fhir_store_parent)
            print(f'Created FHIR store: {fhir_store_id}')
        except errors.HttpError as exc:
            print(f'FHIR store {fhir_store_id} exists already. {exc.resp["status"]}')       

        # Build the import statement to load the FHIR resource file into the store.
        resource_path = "projects/{}/locations/{}/datasets/{}/fhirStores/{}".format(
            project_id, location, dataset_id, fhir_store_id
        )

        request = (
            hc_client.projects()
            .locations()
            .datasets()
            .fhirStores()
            .import_(name=resource_path, body=resource_body)
        )  
        response = request.execute()
        print(response)
    
    return response


def find_by_key(data, target):
    for key, value in data.items():
        if isinstance(value, dict):
            find_by_key(value, target)
        elif key == target:
            return value
        
    return 'NOT_FOUND'

    
def name_resources(file_name, file_json):
    # Check for:
    #   FHIRVersion key in json
    #   FHIR specification in file name e.g. resourceid.R4.json
    #   FHIR specification in file path e.g. my_bucket/my_path/fhir_dstu2/blah.json
    fhir_version = find_by_key(file_json, 'FHIRVersion')
    if fhir_version == 'NOT_FOUND':
        if file_name.endswith('.json'):
            file_name = file_name[:-5]
            try_version = file_name.split('.')[-1]
            if try_version in ('R4', 'DSTU2', 'STU3'):
                fhir_version = try_version
                fhir_store_id = f'fhir-dev-{fhir_version}'
            else:
                try_version = file_name.split('/')
                if 'fhir' in try_version:
                    fhir_version = 'R4'
                    fhir_store_id = f'fhir-dev-{fhir_version}' 
                elif 'fhir_dstu2' in try_version:
                    fhir_version = 'DSTU2'
                    fhir_store_id = f'fhir-dev-{fhir_version}' 
                elif 'fhir_stu3' in try_version:
                    fhir_version = 'STU3'
                    fhir_store_id = f'fhir-dev-{fhir_version}' 
                else:
                    fhir_version = 'NOT_FOUND'
                    fhir_store_id = 'NOT_FOUND'
        else:
            fhir_version = 'NOT_FOUND'
            fhir_store_id = 'NOT_FOUND'
    else: 
        fhir_store_id = f'fhir-dev-{fhir_version}' 
    return fhir_version, fhir_store_id   

def create_fhir_store(hc_client, fhir_version, fhir_store_id, fhir_store_parent):
    # Try to create the fhir store based on the specification.
    body = {"version": fhir_version}
    request = (
        hc_client.projects()
        .locations()
        .datasets()
        .fhirStores()
        .create(parent=fhir_store_parent, body=body, fhirStoreId=fhir_store_id)
    )
    response = request.execute()
    
    return response
