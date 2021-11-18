import google.auth.transport.requests
import google.oauth2.id_token    
import json
import os
import requests
import sys
from google.cloud import storage, exceptions, tasks_v2
from googleapiclient import discovery, errors
from fhir_funcs import *
from utils import *
from error_handler import *

def fhir_to_fhirstore(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    # Decode the payload.
    request_json = request.data.decode('utf8')
    failure = {}

    try:
        request_json = json.loads(request_json)
    except:
        return 'Bad request'

    try:
        file_bucket = request_json['bucket'] # The triggering file bucket.
        file_name = request_json['name'] # The triggering file name.
        file_uri = f'gs://{file_bucket}/{file_name}' 
        file_size = request_json['size'] # The size of the file. 

        assert 'id' in request_json, 'KeyError, path not available'
        assert file_name.endswith('.json'), 'Non-json file extension' 
        assert int(file_size) < 5000000, 'File exceeds 5MB'
    except (KeyError, AssertionError) as exc:
        failure = {
            'status': 'failure',
            'reason': 'Assertion or missing key in request',
            'exc': str(exc),
            'status_code': 500,
        }
        if 'id' in request_json and len(request_json['id'].split('/')) > 2:
            failure['path'] = '/'.join(request_json['id'].split('/')[:-1])
        else:
            failure['path'] = 'Path is not available or malformed'

    if failure:
        return report_failure(failure)

    # Parse the ID of the request payload to get identifying information from the file path.
    # These identifiers will be added to each FHIR resource found within the file contents.
    id_list = build_identifiers(request_json['id'])

    # Instantiates a Cloud Storage client.
    storage_client = storage.Client()

    # Create a bucket object for the triggering file bucket.
    bucket = storage_client.get_bucket(file_bucket)

    # Create a blob object from the filepath for the triggering file name.
    blob = bucket.blob(file_name)

    try:    
        fhir_file_json = json.load(blob.open('rt', encoding='utf-8')) 
    except json.decoder.JSONDecodeError as exc:
        failure = {
            'status': 'error',
            'reason': 'Error decoding payload JSON',
            'exc': str(exc),
            'status_code': 400,
        }
    except exceptions.NotFound as exc:
        failure = {
            'status': 'error',
            'reason': 'FHIR file not found',
            'exc': str(exc),
            'status_code': 404,
        }

    if failure:
        failure['path'] = '/'.join(request_json['id'].split('/')[:-1])
        return report_failure(failure)

    # Parse the resource type out of the file and construct the payload.
    # An individual resource or bundle file can be imported with one resource body.
    # Also, parse bundles to build a list of each resource contained within to be used for adding identification information.
    resource_type = find_by_key(fhir_file_json, 'resourceType') 

    # If resource type key not found in the json, skip the file.
    if resource_type.lower() == 'not_found':
        failure = {
            'status': 'error',
            'reason': '"resourceType" field not found',
            'exc': '',
            'status_code': 400,
            'path': '/'.join(request_json['id'].split('/')[:-1]),
        }
    # If the resource type is a bundle,
    elif resource_type.lower() == 'bundle':
        resource_body = { "contentStructure": "BUNDLE_PRETTY", "gcsSource": { "uri": file_uri } } 

    #    bundle_resource_list = []
    #    for entry in fhir_file_json['entry']:
    #        bundle_entry = entry.get('resource')
    #        resource_identifier = find_by_key(entry, 'identifier') 
    #        bundle_group = (bundle_entry.get('resourceType'), bundle_entry.get('id'), resource_identifier, bundle_entry)
    #        bundle_resource_list.append(bundle_group)

    # If the resource type is an individual resource, 
    else:
        resource_body = { "contentStructure": "RESOURCE_PRETTY", "gcsSource": { "uri": file_uri } }  
    #    resource_identifier = find_by_key(fhir_file_json, 'identifier') 
    #    bundle_group = (fhir_file_json['resourceType'], fhir_file_json['id'], resource_identifier, fhir_file_json)
    #    bundle_resource_list =   [ bundle_group ]

    if failure:
        return report_failure(failure)

    # Read some environment variables; set some others.
    project_id = os.environ.get('GCP_PROJECT')
    location = os.environ.get('FHIR_DATASET_LOCATION')
    dataset_id = os.environ.get('FHIR_DATASET')
    api_version = os.environ.get('API_VERSION')
    service_name = 'healthcare'
    response = 'Import complete'
    
    # Instantiates an authorized API client by discovering the Healthcare API.
    hc_client = discovery.build(service_name, api_version)
    base_url = f"https://{service_name}.googleapis.com/{api_version}"

    # Try to create the fhir store dataset if it does not exist yet.
    dataset_parent = "projects/{}/locations/{}".format(project_id, location)    
    try:
        response = create_fhir_dataset(hc_client, dataset_id, dataset_parent)
        print(f'Created FHIR dataset: {dataset_id}')
    except errors.HttpError as exc:
        print(f'FHIR dataset {dataset_id} exists already. {exc.resp["status"]}')

    # Try to infer the fhir version from the file (either contents or path information).
    # If can't be determined from the file, 'NOT_FOUND' is returned.
    # In this case, loop through all the valid FHIR specifications, attempting to import the file to each.
    fhir_version, fhir_store_id, fhir_store_combined_id, fhir_hpo_site = name_resources(file_name, fhir_file_json)
            
    if fhir_store_id == 'NOT_FOUND':
        fhir_version_list = ['R4', 'STU3', 'DSTU2']
    else:
        fhir_version_list = [ fhir_version ]

    for try_fhir_version in fhir_version_list:
        fhir_store_id = f'fhir-{fhir_hpo_site}-{try_fhir_version}'
        fhir_store_combined_id = f'fhir-combined-{try_fhir_version}'
        fhir_store_parent = "projects/{}/locations/{}/datasets/{}".format(project_id, location, dataset_id)  
        try:
            response = create_fhir_store(hc_client, try_fhir_version, fhir_store_id, fhir_store_parent)
            print(f'Created FHIR store: {fhir_store_id}')
        except errors.HttpError as exc:
            print(f'FHIR store {fhir_store_id} exists already. {exc.resp["status"]}')

        try:
            # Build the import statement to load the FHIR resource file into the store.
            resource_path = "projects/{}/locations/{}/datasets/{}/fhirStores/{}".format(
                project_id, location, dataset_id, fhir_store_id
            )
            response = load_fhir_resource(hc_client, resource_path, resource_body)
            print(f'Loaded {resource_path} successfully.')
        #    break
        except errors.HttpError as exc:
            print(f'Failed to post to {fhir_store_id}. {exc.resp["status"]}')

            # If rate limit exceeded or connection error, raise the error so it will be retried.
            # We don't want to raise other failures as we are processing in a loop.
            if exc.resp.status in (429, 500, 503):
                failure = {
                    'status': 'failure',
                    'reason':
                    f'Failed to post to {fhir_store_id}. {exc.resp["status"]}',
                    'exc': str(exc),
                    'status_code': exc.resp.status,
                    'path': '/'.join(request_json['id'].split('/')[:-1]),
                }
                return report_failure(failure)

        # Now for the combined store.
        try:
            response = create_fhir_store(hc_client, try_fhir_version, fhir_store_combined_id, fhir_store_parent)
            print(f'Created FHIR store: {fhir_store_combined_id}')
        except errors.HttpError as exc:
            print(f'FHIR store {fhir_store_combined_id} exists already. {exc.resp["status"]}')

        try:
            # Build the import statement to load the FHIR resource file into the store.
            combined_resource_path = "projects/{}/locations/{}/datasets/{}/fhirStores/{}".format(
                project_id, location, dataset_id, fhir_store_combined_id
            )
            response = load_fhir_resource(hc_client, combined_resource_path, resource_body)
            print(f'Loaded {combined_resource_path} successfully.')
            break
        except errors.HttpError as exc:
            print(f'Failed to post to {fhir_store_combined_id}. {exc.resp["status"]}')

            # If rate limit exceeded or connection error, raise the error so it will be retried.
            # We don't want to raise other failures as we are processing in a loop.
            if exc.resp.status in (429, 500, 503):
                failure = {
                    'status': 'failure',
                    'reason':
                    f'Failed to post to {fhir_store_combined_id}. {exc.resp["status"]}',
                    'exc': str(exc),
                    'status_code': exc.resp.status,
                    'path': '/'.join(request_json['id'].split('/')[:-1]),
                }
                return report_failure(failure)                
    
    # If identifier doesn't exist, add it to the json and execute an update request.
    # Otherwise, append the identifier to the existing dictionary and execute an update request.
    if resource_type.lower() == 'bundle':
        print(f'Starting identification of {len(fhir_file_json["entry"])} resources from file.')
        for entry in fhir_file_json['entry']:
            bundle_entry = entry.get('resource')
            resource_identifier = find_by_key(entry, 'identifier') 
            resource_group_type = bundle_entry.get('resourceType')
            resource_group_id = bundle_entry.get('id')

            try:
                response = update_fhir_json(base_url, resource_path, bundle_entry, resource_group_type, resource_group_id, resource_identifier, id_list)
            except errors.HttpError as exc:
                print(f'Failed to add identifiers to {resource_path}. {exc.resp["status"]}')
                raise
            try:
                response = update_fhir_json(base_url, combined_resource_path, bundle_entry, resource_group_type, resource_group_id, resource_identifier, id_list)
            except errors.HttpError as exc:
                print(f'Failed to add identifiers to {combined_resource_path}. {exc.resp["status"]}')
                raise                
    else:
        print('Starting identification of 1 resources from file.')
        resource_identifier = find_by_key(entry, 'identifier') 
        resource_group_type = fhir_file_json['resourceType']
        resource_group_id = fhir_file_json['id']

        # Update the resource in the individual FHIR store.
        try:
            response = update_fhir_json(base_url, resource_path, fhir_file_json, resource_group_type, resource_group_id, resource_identifier, id_list)
        except errors.HttpError as exc:
            print(f'Failed to add identifiers to {resource_path}. {exc.resp["status"]}')
            raise    

        # Update the resource in the combined FHIR store.
        try:
            response = update_fhir_json(base_url, combined_resource_path, fhir_file_json, resource_group_type, resource_group_id, resource_identifier, id_list)
        except errors.HttpError as exc:
            print(f'Failed to add identifiers to {combined_resource_path}. {exc.resp["status"]}')
            raise                  

    return {'result': 'success'}


