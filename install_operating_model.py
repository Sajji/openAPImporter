from __future__ import print_function
import time
import collibra_core
from collibra_core.rest import ApiException
from pprint import pprint
import json
import os
from tabulate import tabulate
import logging.config
import yaml

# Setup logger
def setup_logger():

    with open('logging_config.yaml', 'rt') as f:
        logging_config = yaml.safe_load(f.read())

    logging.config.dictConfig(logging_config)
    logger = logging.getLogger('production')

    return logger

logger = setup_logger()

with open('config.json') as config_file:
    config = json.load(config_file)

# Configure HTTP basic authorization: basicAuth
configuration = collibra_core.Configuration()
configuration.host = config['url']
configuration.username = config['username']
configuration.password = config['password']

def load_json_files_from_directory(directory_path, required_fields, stats):
    if not os.path.exists(directory_path):
        return []

    json_files = []
    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):
            with open(os.path.join(directory_path, filename)) as file:
                try:
                    data = json.load(file)
                    missing_fields = [field for field in required_fields if field not in data]
                    if missing_fields:
                        logger.error(f"File {filename} is missing required fields: {missing_fields}")
                        stats['errors'] += 1
                    else:
                        json_files.append(data)
                except json.JSONDecodeError:
                    logger.error(f"File {filename} is not a valid JSON")
                    stats['errors'] += 1
    return json_files

def build_asset_type_request(asset, optional_fields, is_change_request=False, existing_asset_id=None):
    request_class = collibra_core.ChangeAssetTypeRequest if is_change_request else collibra_core.AddAssetTypeRequest

    request = request_class(
        id=existing_asset_id,
        name=asset['name'],
        symbol_type=asset.get('symbol_type', "NONE"),
        display_name_enabled=asset.get('display_name_enabled', False),
        rating_enabled=asset.get('rating_enabled', False)
    )

    # Update only optional fields that are present in the JSON
    for field in optional_fields:
        if field in asset:
            setattr(request, field, asset[field])

    return request

def build_community_request(community, optional_fields, is_change_request=False, existing_community_id=None):
    request_class = collibra_core.ChangeCommunityRequest if is_change_request else collibra_core.AddCommunityRequest

    request = request_class(
        id=existing_community_id,
        name=community['name']
    )

    # Update only optional fields that are present in the JSON
    for field in optional_fields:
        if field in community:
            setattr(request, field, community[field])

    return request

def build_domain_request(domain, optional_fields, is_change_request=False, existing_domain_id=None):
    request_class = collibra_core.ChangeDomainRequest if is_change_request else collibra_core.AddDomainRequest

    request = request_class(
        id=existing_domain_id,
        community_id=domain['community_id'],
        type_id=domain['type_id'],
        name=domain['name']
    )

    # Update only optional fields that are present in the JSON
    for field in optional_fields:
        if field in domain:
            setattr(request, field, domain[field])

    return request

def build_relation_type_request(relation_type, optional_fields, is_change_request=False, existing_relation_type_id=None):
    request_class = collibra_core.ChangeRelationTypeRequest if is_change_request else collibra_core.AddRelationTypeRequest

    request = request_class(
        id=existing_relation_type_id,
        source_type_id=relation_type['source_type_id'],
        role=relation_type['role'],
        target_type_id=relation_type['target_type_id'],
        co_role=relation_type['co_role']
    )

    # Update only optional fields that are present in the JSON
    for field in optional_fields:
        if field in relation_type:
            setattr(request, field, relation_type[field])

    return request

def build_assignment_request(assignment, optional_fields, is_change_request=False, existing_assignment_id=None):

    request = collibra_core.AddAssignmentRequest(
        asset_type_id=assignment['asset_type_id'],
        status_ids=assignment['status_ids'],
        default_status_id=assignment['default_status_id']
    )

    if(is_change_request):
        request_class = collibra_core.ChangeAssignmentRequest(
            status_ids=assignment['status_ids'],
            default_status_id=assignment['default_status_id']
        )

    # Update only optional fields that are present in the JSON
    for field in optional_fields:
        if field in assignment:
            if field == "articulation_rules" and isinstance(assignment[field], list):
                articulation_rules = []
                for rule in assignment[field]:
                    rule_request = collibra_core.ArticulationRuleRequest(
                        operation=rule.get("operation"),
                        score=rule.get("score")
                    )
                    for key in ["id", "status_id", "attribute_type_id"]:
                        if key in rule:
                            setattr(rule_request, key, rule[key])
                    articulation_rules.append(rule_request)
                setattr(request, field, articulation_rules)
            elif field == "characteristic_types" and isinstance(assignment[field], list):
                characteristic_types = []
                for rule in assignment[field]:
                    rule_request = collibra_core.CharacteristicTypeAssignmentReference(
                        id=rule.get("id"),
                        type=rule.get("type")
                    )
                    for key in []:
                        if key in rule:
                            setattr(rule_request, key, rule[key])
                    characteristic_types.append(rule_request)
                setattr(request, field, characteristic_types)
            else:
                setattr(request, field, assignment[field])

    return request

def create_or_update_asset(api_client, asset, optional_fields, stats):
    try:
        api_instance = collibra_core.AssetTypesApi(api_client)

        if 'id' in asset:
            try:
                existing_asset = api_instance.get_asset_type(asset['id'])
                if existing_asset:
                    # Update existing asset type
                    change_asset_type_request = build_asset_type_request(asset, optional_fields, True, existing_asset.id)
                    api_response = api_instance.change_asset_type(existing_asset.id, body=change_asset_type_request)
                    logger.info("Asset updated: %s", api_response)
                    stats['updated'] += 1
                    return
            except ApiException as e:
                if e.status != 404:
                    logger.error("Exception when calling AssetTypesApi->get_asset_type: %s", e.body)
                    stats['errors'] += 1
                    return

        # Check if asset type exists by name
        existing_assets_response = api_instance.find_asset_types(name=asset['name'])
        existing_assets = existing_assets_response.results
        if existing_assets and 'id' not in asset:
            # Update existing asset type
            existing_asset_id = existing_assets[0].id
            change_asset_type_request = build_asset_type_request(asset, optional_fields, True, existing_asset_id)
            api_response = api_instance.change_asset_type(existing_asset_id, body=change_asset_type_request)
            logger.info("Asset updated: %s", api_response)
            stats['updated'] += 1
        else:
            # Create new asset type
            add_asset_type_request = build_asset_type_request(asset, optional_fields)
            api_response = api_instance.add_asset_type(body=add_asset_type_request)
            logger.info("Asset added: %s", api_response)
            stats['created'] += 1
    except ApiException as e:
        logger.error("Exception when calling AssetTypesApi: %s", e.body)
        stats['errors'] += 1

def create_or_update_community(api_client, community, optional_fields, stats):
    try:
        api_instance = collibra_core.CommunitiesApi(api_client)

        if 'id' in community:
            try:
                existing_community = api_instance.get_community(community['id'])
                if existing_community:
                    # Update existing community
                    change_community_request = build_community_request(community, optional_fields, True, existing_community.id)
                    api_response = api_instance.change_community(existing_community.id, body=change_community_request)
                    logger.info("Community updated: %s", api_response)
                    stats['updated'] += 1
                    return
            except ApiException as e:
                if e.status != 404:
                    logger.error("Exception when calling CommunitiesApi->get_community: %s", e.body)
                    stats['errors'] += 1
                    return

        # Check if community exists by name
        existing_communities_response = api_instance.find_communities(name=community['name'], sort_field="NAME")
        existing_communities = existing_communities_response.results
        if existing_communities and 'id' not in community:
            # Update existing community
            existing_community_id = existing_communities[0].id
            change_community_request = build_community_request(community, optional_fields, True, existing_community_id)
            api_response = api_instance.change_community(existing_community_id, body=change_community_request)
            logger.info("Community updated: %s", api_response)
            stats['updated'] += 1
        else:
            # Create new community
            add_community_request = build_community_request(community, optional_fields)
            api_response = api_instance.add_community(body=add_community_request)
            logger.info("Community added: %s", api_response)
            stats['created'] += 1
    except ApiException as e:
        logger.error("Exception when calling CommunitiesApi: %s", e.body)
        stats['errors'] += 1

def create_or_update_domain(api_instance, item, optional_fields, stats):
    try:
        if 'id' in item:
            try:
                existing_item = api_instance.get_domain(item['id'])
                if existing_item:
                    change_item_request = build_domain_request(item, optional_fields, True, existing_item.id)
                    api_response = api_instance.change_domain(existing_item.id, body=change_item_request)
                    logger.info("Domain updated: %s", api_response)
                    stats['updated'] += 1
                    return
            except ApiException as e:
                if e.status != 404:
                    logger.error("Exception when calling DomainsApi->get_domain: %s", e.body)
                    stats['errors'] += 1
                    return

        # Check if item exists by name and community_id
        existing_items_response = api_instance.find_domains(name=item['name'], community_id=item['community_id'])
        existing_items = existing_items_response.results

        if existing_items and 'id' not in item:
            # Update existing item
            existing_item_id = existing_items[0].id
            change_item_request = build_domain_request(item, optional_fields, True, existing_item_id)
            api_response = api_instance.change_domain(existing_item_id, body=change_item_request)
            logger.info("Domain updated: %s", api_response)
            stats['updated'] += 1
        else:
            # Create new item
            add_item_request = build_domain_request(item, optional_fields)
            api_response = api_instance.add_domain(body=add_item_request)
            logger.info("Domain added: %s", api_response)
            stats['created'] += 1
    except ApiException as e:
        logger.error("Exception when calling DomainsApi: %s", e.body)
        stats['errors'] += 1

def create_or_update_relation_type(api_instance, item, optional_fields, stats):
    try:
        if 'id' in item:
            try:
                existing_item = api_instance.get_relation_type(item['id'])
                if existing_item:
                    change_item_request = build_relation_type_request(item, optional_fields, True, existing_item.id)
                    api_response = api_instance.change_relation_type(existing_item.id, body=change_item_request)
                    logger.info("Relation Type updated: %s", api_response)
                    stats['updated'] += 1
                    return
            except ApiException as e:
                if e.status != 404:
                    logger.error("Exception when calling RelationTypesApi->get_relation_type: %s", e.body)
                    stats['errors'] += 1
                    return

        # Create new item
        add_item_request = build_relation_type_request(item, optional_fields)
        api_response = api_instance.add_relation_type(body=add_item_request)
        logger.info("Relation type added: %s", api_response)
        stats['created'] += 1

    except ApiException as e:
        logger.error("Exception when calling RelationTypesApi: %s", e.body)
        stats['errors'] += 1

def create_or_update_assignment(api_instance, item, optional_fields, stats):
    try:
        if 'id' in item:
            try:
                change_item_request = build_assignment_request(item, optional_fields, True, item['id'])
                api_response = api_instance.change_assignment(item['id'], body=change_item_request)
                logger.info("Assignment updated: %s", api_response)
                stats['updated'] += 1
                return
            except AttributeError as e:
                stats['updated'] += 1
                return
            except ApiException as e:
                if e.status != 404:
                    logger.error("Exception when calling AssignmentsApi->change_assignment: %s", e.body)
                    stats['errors'] += 1
                    return

        # Create new item
        add_item_request = build_assignment_request(item, optional_fields)
        api_response = api_instance.add_assignment(body=add_item_request)
        logger.info("Assignment added: %s", api_response)
        stats['created'] += 1
    except AttributeError as e:
        stats['created'] += 1
    except ApiException as e:
        logger.error("Exception when calling DomainsApi: %s", e.body)
        stats['errors'] += 1

def create_assets(api_client, stats):
    required_fields = ['name', 'symbol_type', 'display_name_enabled', 'rating_enabled']
    optional_fields = ['id', 'description', 'parent_id', 'color', 'icon_code', 'acronym_code']
    assets = load_json_files_from_directory('resources/AssetType', required_fields, stats)
    for asset in assets:
        create_or_update_asset(api_client, asset, optional_fields, stats)

def create_communities(api_client, stats):
    required_fields = ['name']
    optional_fields = ['description', 'parent_id', 'id']
    communities = load_json_files_from_directory('resources/Community', required_fields, stats)
    for community in communities:
        create_or_update_community(api_client, community, optional_fields, stats)

def create_domains(api_client, stats):
    required_fields = ['name', 'community_id', 'type_id']
    optional_fields = ['description', 'excluded_from_auto_hyperlinking', 'id']
    domains = load_json_files_from_directory('resources/Domain', required_fields, stats)
    for domain in domains:
        create_or_update_domain(collibra_core.DomainsApi(api_client), domain, optional_fields, stats)

def create_relation_types(api_client, stats):
    required_fields = ['source_type_id', 'role', 'target_type_id', 'co_role']
    optional_fields = ['description', 'id']
    relation_types = load_json_files_from_directory('resources/RelationType', required_fields, stats)
    for relation_type in relation_types:
        create_or_update_relation_type(collibra_core.RelationTypesApi(api_client), relation_type, optional_fields, stats)

def create_assignments(api_client, stats):
    required_fields = ['asset_type_id', 'status_ids', 'default_status_id']
    optional_fields = ['id', 'characteristic_types', 'articulation_rules', 'validation_rule_ids', 'data_quality_rule_ids', 'domain_type_ids', 'scope_id']
    relation_types = load_json_files_from_directory('resources/Assignment', required_fields, stats)
    for relation_type in relation_types:
        create_or_update_assignment(collibra_core.AssignmentsApi(api_client), relation_type, optional_fields, stats)

def main():
    stats = {
        'assets': {'created': 0, 'updated': 0, 'errors': 0},
        'communities': {'created': 0, 'updated': 0, 'errors': 0},
        'domains': {'created': 0, 'updated': 0, 'errors': 0},
        'relation_types': {'created': 0, 'updated': 0, 'errors': 0},
        'assignments': {'created': 0, 'updated': 0, 'errors': 0}
    }

    api_client = collibra_core.ApiClient(configuration)
    create_assets(api_client, stats['assets'])
    create_communities(api_client, stats['communities'])
    create_domains(api_client, stats['domains'])
    create_relation_types(api_client, stats['relation_types'])
    create_assignments(api_client, stats['assignments'])

    stats_table = []
    for category, values in stats.items():
        stats_table.append([category, values['created'], values['updated'], values['errors']])

    print(tabulate(stats_table, headers=["Category", "Created", "Updated", "Failed"], tablefmt="pretty"))

if __name__ == "__main__":
    main()
