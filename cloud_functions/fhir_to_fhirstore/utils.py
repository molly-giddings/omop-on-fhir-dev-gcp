# Build list of identifiers that are to be added to each FHIR resource.
# Returns: list
def build_identifiers(id_path):
    id_list = []
    file_path = id_path.split('/')
    collection = file_path[1]
    if len(file_path) >=8:
        ehr_platform = file_path[5]
        hpo_site = file_path[6].split(' ')[0]
        participant_id = file_path[7]
    else:
        ehr_platform = 'Synthea'
        hpo_site = 'Synthea'
        participant_id = file_path[-1].removesuffix('.json').split('_')[-1]
    id_list.append(f'Collection: {collection}')  
    id_list.append(f'EHR Platform: {ehr_platform}')          
    id_list.append(f'HPO Site: {hpo_site}')
    id_list.append(f'Participant ID: {participant_id}')
        
    return id_list

# Determine if a key exists in a dictionary (possibly nested).
# Returns: string key value or NOT_FOUND
def find_by_key(data, target):
    for key, value in data.items():
        if isinstance(value, dict):
            find_by_key(value, target)
        elif key == target:
            return value
        
    return 'NOT_FOUND'

    
# Try to determine the FHIR version from the file name or path.
# Returns: string FHIR version and string FHIR store ID
def name_resources(file_name, file_json):
    # Check for:
    #   FHIRVersion key in json
    #   FHIR specification in file name e.g. resourceid.R4.json
    #   FHIR specification in file path e.g. aou-curation-omop-dev_transfer_fhir/synthea_mg/fhir_dstu2/blah.json
    fhir_version = find_by_key(file_json, 'FHIRVersion')
    try:
        hpo_site = file_name.split('/')[6].split(' ')[0]
    except:
        hpo_site = 'synthea'

    if fhir_version == 'NOT_FOUND':
        if file_name.endswith('.json'):
            file_name = file_name[:-5]
            try_version = file_name.split('.')[-1]
            if try_version in ('R4', 'DSTU2', 'STU3'):
                fhir_version = try_version
                fhir_store_id = f'fhir-{hpo_site}-{fhir_version}'
                fhir_store_combined_id = f'fhir-combined-{fhir_version}'                  
            else:
                try_version = file_name.split('/')
                if 'fhir' in try_version:
                    fhir_version = 'R4'
                    fhir_store_id = f'fhir-{hpo_site}-{fhir_version}' 
                    fhir_store_combined_id = f'fhir-combined-{fhir_version}'                    
                elif 'fhir_dstu2' in try_version:
                    fhir_version = 'DSTU2'
                    fhir_store_id = f'fhir-{hpo_site}-{fhir_version}' 
                    fhir_store_combined_id = f'fhir-combined-{fhir_version}'                      
                elif 'fhir_stu3' in try_version:
                    fhir_version = 'STU3'
                    fhir_store_id = f'fhir-{hpo_site}-{fhir_version}' 
                    fhir_store_combined_id = f'fhir-combined-{fhir_version}'                      
                else:
                    fhir_version = 'NOT_FOUND'
                    fhir_store_id = 'NOT_FOUND'
                    fhir_store_combined_id = 'NOT_FOUND'
        else:
            fhir_version = 'NOT_FOUND'
            fhir_store_id = 'NOT_FOUND'
            fhir_store_combined_id = 'NOT_FOUND'

    else: 
        fhir_store_id = f'fhir-{hpo_site}-{fhir_version}' 
        fhir_store_combined_id = f'fhir-combined-{fhir_version}'
    return fhir_version, fhir_store_id, fhir_store_combined_id, hpo_site   
