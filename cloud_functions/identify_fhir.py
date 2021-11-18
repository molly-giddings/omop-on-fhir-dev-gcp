
def identify_fhir(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    import json
    import google.auth    
    from google.auth.transport import requests    

    # Decode the payload.
    request_json = request.data.decode('utf8') #request.json

    try:
        request_json = json.loads(request_json)
    except requests.exceptions.RequestException as exc:
        print('Bad request. {exc.resp["status"]}')
        raise

    # Creates a requests Session object with the credentials.
    scoped_credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])     
    session = requests.AuthorizedSession(scoped_credentials)

    # Parse the input parameters from the body of the request payload.
    method = request_json['method']
    id_resource_path = request_json['resource_path']
    headers = request_json['request_header']
    identify_json = request_json['request_json']

    try:
        response = session.put(id_resource_path, headers=headers, json=identify_json)
    except requests.exceptions.RequestException as exc:
        print(f'Failed to add identifier to {id_resource_path}. {exc.resp["status"]}')

        # If rate limit exceeded or connection error, raise the error so it will be retried.
        # We don't want to raise other failures.
        if exc.resp.status in (429, 500, 503):
            raise

    return f'Added identification to {id_resource_path}'  
