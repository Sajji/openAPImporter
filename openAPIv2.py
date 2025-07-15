import sys
import json
import logging.config
import yaml
import os
import time

from collibra_importer.api_client import Configuration as Collibra_Importer_Api_Client_Config
from collibra_importer.api_client import ApiClient as Collibra_Importer_Api_Client
from collibra_importer.api import import_api

from collibra_core.api_client import Configuration as Collibra_Core_Api_Client_Config
from collibra_core.api_client import ApiClient as Collibra_Core_Api_Client
from collibra_core.api import jobs_api

# Setup logger
def setup_logger():
    with open('logging_config.yaml', 'rt') as f:
        logging_config = yaml.safe_load(f.read())

    logging.config.dictConfig(logging_config)
    logger = logging.getLogger('development')

    return logger

logger = setup_logger()

def read_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error reading file '{file_path}': {e}")
        return None

def extract_title_and_description(json_data):
    try:
        title = json_data['info']['title']
        description = json_data['info'].get('description', 'No description provided')
        return title, description
    except KeyError as e:
        logger.error(f"Key error: {e}")
        return None, None

def read_config_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error reading config file '{file_path}': {e}")
        return None

def send_import_data(import_data, properties):
    temp_filename = 'temp_import_data.json'
    try:
        with open(temp_filename, 'w') as temp_file:
            json.dump(import_data, temp_file)

        collibra_config = Collibra_Importer_Api_Client_Config()
        collibra_config.host = properties['url']
        collibra_config.username = properties['username']
        collibra_config.password = properties['password']

        api_client = Collibra_Importer_Api_Client(collibra_config)
        import_api_instance = import_api.ImportApi(api_client)
        jobs_api_instance = jobs_api.JobsApi(api_client)

        response = import_api_instance.import_json_in_job(file_name=temp_filename, file=temp_filename)
        logger.info(f"Import data sent successfully: {response.id}")

        while response.state not in {"COMPLETED", "CANCELED", "ERROR"}:
            time.sleep(1)
            response = jobs_api_instance.get_job(job_id=response.id)
            logger.info(f"Job state: {response.state}")

    except Exception as e:
        logger.error(f"Error sending import data: {e}")
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def get_references(schema):
    refs = set()
    if isinstance(schema, dict):
        for key, value in schema.items():
            if key == '$ref':
                refs.add(value.split('/')[-1])
            else:
                refs.update(get_references(value))
    elif isinstance(schema, list):
        for item in schema:
            refs.update(get_references(item))
    return refs

def process_schemas(json_data, domain_name, community_name):
    schemas = json_data.get('components', {}).get('schemas', {})

    if not schemas:
        logger.error("No schemas found in the OpenAPI file.")
        return []

    import_data = []
    for schema_name in schemas:
        schema_json = create_schema_asset(schema_name, domain_name, community_name)
        import_data.append(schema_json)
        properties_assets = process_properties(schema_name, schemas[schema_name], community_name, domain_name)
        import_data.extend(properties_assets)

    return import_data

def create_schema_asset(schema_name, domain_name, community_name):
    return {
        "resourceType": "Asset",
        "identifier": {
            "name": schema_name,
            "domain": {
                "name": domain_name,
                "community": {
                    "name": community_name
                }
            }
        },
        "type": {
            "name": "Data Structure"
        },
        "attributes": {
            "Description": [
                {
                    "value": ""
                }
            ]
        }
    }

def process_properties(schema_name, schema_content, community_name, domain_name):
    properties_assets = []
    if 'properties' in schema_content:
        for prop_name, prop_content in schema_content['properties'].items():
            # Pass the description to create_property_asset
            prop_description = prop_content.get('description', '')
            prop_json = create_property_asset(prop_name, schema_name, community_name, domain_name, prop_description)
            if '$ref' in prop_content:
                add_reference_relation(prop_json, prop_content['$ref'], community_name, domain_name)
            elif 'items' in prop_content and '$ref' in prop_content['items']:
                add_reference_relation(prop_json, prop_content['items']['$ref'], community_name, domain_name)
            properties_assets.append(prop_json)
    return properties_assets

# Added description parameter to create_property_asset
def create_property_asset(prop_name, schema_name, community_name, domain_name, description=""):
    return {
        "resourceType": "Asset",
        "identifier": {
            "name": prop_name,
            "domain": {
                "name": domain_name,
                "community": {
                    "name": community_name
                }
            }
        },
        "type": {
            "name": "Data Element"
        },
        "attributes": {
            "Description": [
                {
                    "value": description # Use the provided description
                }
            ]
        },
        "relations": {
            "00000000-0000-0000-0000-000000007017:SOURCE": [
                {
                    "name": schema_name,
                    "domain": {
                        "name": domain_name,
                        "community": {
                            "name": community_name
                        }
                    }
                }
            ]
        }
    }

def add_reference_relation(prop_json, ref, community_name, domain_name):
    ref_schema_name = ref.split('/')[-1]
    if "00000000-0000-0000-0000-000000007017:TARGET" not in prop_json["relations"]:
        prop_json["relations"]["00000000-0000-0000-0000-000000007017:TARGET"] = []
    prop_json["relations"]["00000000-0000-0000-0000-000000007017:TARGET"].append({
        "name": ref_schema_name,
        "domain": {
            "name": domain_name,
            "community": {
                "name": community_name
            }
        }
    })

def process_paths(json_data, title, config_data, community_name, domains):
    paths = json_data.get('paths', {})
    import_data = []

    for path, details in paths.items():
        for method, method_details in details.items():
            endpoint_name = method.upper() + " " + path
            endpoint_description = method_details.get('description', '')

            json_object = create_endpoint_asset(endpoint_name, endpoint_description, title, config_data, community_name, domains)
            import_data.append(json_object)

            responses_assets = process_responses(title, endpoint_name, method_details.get('responses', {}), community_name, domains)
            import_data.extend(responses_assets)

    return import_data

def create_endpoint_asset(endpoint_name, endpoint_description, title, config_data, community_name, domains):
    return {
        "resourceType": "Asset",
        "identifier": {
            "name": endpoint_name,
            "domain": {
                "name": domains.get("data_assets"),
                "community": {
                    "name": community_name
                }
            }
        },
        "type": {
            "name": config_data.get("assets").get("api_endpoint")
        },
        "attributes": {
            "Description": [
                {
                    "value": endpoint_description
                }
            ]
        },
        "relations": {
            "00000000-0000-0000-0000-000000007005:TARGET": [
                {
                    "name": title,
                    "domain": {
                        "name": config_data.get("domains").get("api_assets"),
                        "community": {
                            "name": community_name
                        }
                    }
                }
            ]
        }
    }

def process_responses(title, endpoint_name, responses, community_name, domains):
    responses_assets = []
    for response_code, response_content in responses.items():  # Renamed 'response' to 'response_code' for clarity
        code_name = response_code.upper()
        code_description = response_content.get('description', '')

        json_object = create_response_asset(title, endpoint_name, code_name, code_description, community_name, domains)

        if 'content' in response_content:
            for media_type_content in response_content['content'].values():
                if 'schema' in media_type_content:
                    # Use get_references to find all refs within the schema
                    refs = get_references(media_type_content['schema'])
                    for ref in refs:
                        add_reference_relation(json_object, f"#/components/schemas/{ref}", community_name, domains.get("data_assets"))

        responses_assets.append(json_object)
    return responses_assets

def create_response_asset(title, endpoint_name, code_name, code_description, community_name, domains):
    return {
        "resourceType": "Asset",
        "identifier": {
            "name": '>'.join([title, endpoint_name, code_name]),
            "domain": {
                "name": domains.get("code_values"),
                "community": {
                    "name": community_name
                }
            }
        },
        "displayName": code_name.lower(),
        "type": {
            "name": "Code Value"
        },
        "attributes": {
            "Description": [
                {
                    "value": code_description
                }
            ]
        },
        "relations": {
            "00000000-0000-0000-0000-000000007017:SOURCE": [
                {
                    "name": endpoint_name,
                    "domain": {
                        "name": domains.get("data_assets"),
                        "community": {
                            "name": community_name
                        }
                    }
                }
            ]
        }
    }

def main():
    if len(sys.argv) != 2:
        logger.error("Usage: python openapi.py <path_to_json_file>")
        return

    json_file_path = sys.argv[1]
    logger.info(f"Processing file: {json_file_path}")

    json_data = read_json_file(json_file_path)
    if json_data is None:
        return

    config_data = read_config_file('config.json')
    if config_data is None:
        return

    domains = config_data.get("domains")
    community_name = config_data.get("community_name")
    properties = {
        'url': config_data.get("url", ""),
        'username': config_data.get("username", ""),
        'password': config_data.get("password", "")
    }

    import_data = []

    title, description = extract_title_and_description(json_data)
    if title and description:
        json_object = {
            "resourceType": "Asset",
            "identifier": {
                "name": title,
                "domain": {
                    "name": domains.get("api_assets"),
                    "community": {
                        "name": community_name
                    }
                }
            },
            "type": {
                "name": config_data.get("assets").get("api")
            },
            "attributes": {
                "Description": [
                    {
                        "value": description
                    }
                ]
            }
        }

        import_data.append(json_object)

    import_data.extend(process_schemas(json_data, domains.get("data_assets"), community_name))
    import_data.extend(process_paths(json_data, title, config_data, community_name, domains))

    send_import_data(import_data, properties)

if __name__ == "__main__":
    main()