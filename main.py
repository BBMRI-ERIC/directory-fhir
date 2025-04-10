import json

import blaze_client
import requests
from typing import List
from miabis_model import Biobank, Network, collection  # Assuming Biobank class is imported

from miabis_model import Collection, Gender, StorageTemperature  # Assuming these classes are imported


def populate_collection_from_json(json_str: str) -> List[Collection]:
    """
    Populate Collection objects from the JSON data.
    """
    # Parse the JSON string into a Python list of dictionaries
    json_data = json.loads(json_str)

    collections = []
    for collection_json in json_data.get("data", {}).get("Collections", []):
        # Extract necessary fields from the JSON
        identifier = collection_json.get("id", "")
        name = collection_json.get("name", "")
        description = collection_json.get("description", "unknown")
        contact_info = collection_json.get("contact", {})  # Extract contact info from nested object
        country = collection_json.get("country", {}).get("name", "unknown")  # Extract country name from nested object
        sex = collection_json.get("sex", [])  # Extract gender information
        age_low = collection_json.get("age_low")
        age_high = collection_json.get("age_high")
        storage_temperatures = collection_json.get("storage_temperatures", [])  # Extract storage temperatures
        diagnoses = collection_json.get("diagnosis_available", [])
        sample_types = collection_json.get("materials", [])
        material_types = [StorageTemperature[sample_type.get("name")] for sample_type in sample_types if sample_type.get("name")]
        managing_biobank_id = collection_json.get("biobank", {}).get("id", "")  # Extract managing biobank ID

        # Extract contact details
        contact_name = contact_info.get("first_name", "unknown")
        contact_surname = contact_info.get("last_name", "unknown")
        contact_email = contact_info.get("email", "unknown")

        # Map genders to Gender enum
        genders = [
            Gender[("UNKNOWN" if (name := g.get("name")) == "NAV" or "NASK" else name).upper()]
            for g in sex
            if g.get("name") is not None
        ]
        # Map storage temperatures to StorageTemperature enum
        #storage_temps = [StorageTemperature[temp.get("name")] for temp in storage_temperatures if temp.get("name")]

        # Extract diagnosis codes
        diagnosis_codes = [diagnosis.get("code") for diagnosis in diagnoses if diagnosis.get("code")]

        # Extract material types
        material_type_codes = [material.get("name") for material in material_types if material.get("name")]

        # Create a Collection instance
        try:
            collection = Collection(
                identifier=identifier,
                name=name,
                managing_biobank_id=managing_biobank_id,
                contact_name=contact_name,
                contact_surname=contact_surname,
                contact_email=contact_email,
                country=country,
                genders=genders,
                material_types=material_type_codes,
                age_range_low=age_low,
                age_range_high=age_high,
                diagnoses=diagnosis_codes,
                description=description
            )
        except ValueError as e:
            print(f"Error creating Collection: {e}")
            continue

        collections.append(collection)

    return collections
def fetch_quality_names(quality_data: list) -> List[str]:
    """
    Fetch all 'name' fields under the 'quality -> quality_standard -> name' path.
    """
    names = []
    for item in quality_data:
        quality_standard = item.get("quality_standard", {})
        name = quality_standard.get("name")
        if name:
            names.append(name)
    return names

def populate_network_from_json(json_data: dict) -> List[Network]:
    """
    Populate Network objects from the JSON data.
    """
    networks = []
    for network_json in json_data.get("data", {}).get("Networks", []):
        # Extract necessary fields from the JSON
        identifier = network_json.get("id", "")
        name = network_json.get("name", "")
        description = network_json.get("description", "")
        juristic_person = network_json.get("juridical_person", "unknown")
        contact_info = network_json.get("contact", {})  # Extract contact info from nested object
        country = network_json.get("national_node", "unknown")
        common_collaboration_topics = network_json.get("common_network_elements", "").split(",") if network_json.get("common_network_elements") else []

        # Extract contact details
        contact_name = contact_info.get("first_name", "unknown")
        contact_surname = contact_info.get("last_name", "unknown")
        contact_email = contact_info.get("email", "unknown")

        # Assuming managing_biobank_id is not directly available in the JSON, you might need to derive it
        managing_biobank_id = "bbmri-eric:ID:AT_MUG"  # Replace with actual logic to get this value

        # Create a Network instance
        network = Network(
            identifier=identifier,
            name=name,
            managing_biobank_id=managing_biobank_id,
            contact_email=contact_email,
            country=country,
            juristic_person=juristic_person,
            members_collections_ids=[],  # Replace with actual member collection IDs if available
            members_biobanks_ids=[],  # Replace with actual member biobank IDs if available
            contact_name=contact_name,
            contact_surname=contact_surname,
            common_collaboration_topics=common_collaboration_topics,
            description=description
        )

        networks.append(network)

    return networks

def populate_biobank_from_json(json_data: dict) -> List[Biobank]:
    """
    Populate Biobank objects from the JSON data.
    """
    biobanks = []
    for biobank_json in json_data.get("data", {}).get("Biobanks", []):
        # Extract necessary fields from the JSON
        identifier = biobank_json.get("id", "")
        name = biobank_json.get("name", "")
        alias = biobank_json.get("acronym", "unknown")
        country = biobank_json.get("country", {}).get("name", "")  # Extract country name from nested object
        description = biobank_json.get("description", "")
        contact_info = biobank_json.get("contact", {})  # Extract contact info from nested object

        # Extract contact details
        contact_name = contact_info.get("first_name", "unknown")
        contact_surname = contact_info.get("last_name", "unknown")
        contact_email = contact_info.get("email", "unknown")

        # Extract quality management standards
        quality_management_standards = fetch_quality_names(biobank_json.get("quality", []))

        # Create a Biobank instance
        biobank = Biobank(
            identifier=identifier,
            name=name,
            alias=alias,
            country=country,
            contact_name=contact_name,
            contact_surname=contact_surname,
            contact_email=contact_email,
            quality__management_standards=quality_management_standards,  # Not available in the JSON, leave as empty string
            description=description
        )

        biobanks.append(biobank)

    return biobanks


# GraphQL query to fetch biobank data
query = '''
{
    Biobanks {
        id
        name
        acronym
        country {
            name
        }
        description
        url
        withdrawn
        contact {
            email
            first_name
            last_name
        }
        quality {
            quality_standard {
                name
            }
        }
    }
}
'''
networks_query = '''
{
    Networks {
        id
        name
        acronym
        description
        url
        contact {
            email
            first_name
            last_name
        }
    }
}
'''
collections_query = '''
{
    Collections {
        id
        name
      biobank {
        id
      }
      
        acronym
        description
        url
        contact {
            email
            first_name
            last_name
        }
      country {
        name
      }
      sex {
        name
      }
      age_low
      age_high
      storage_temperatures {
        name
      }
      diagnosis_available {
        code
      }
      number_of_donors
      url
      description
      type {
        name
      }
      access_description
    }
}
'''

def sync_biobanks():
    # Fetch data from the BBMRI-ERIC GraphQL API
    response = requests.post(
        'https://directory.bbmri-eric.eu/ERIC/directory/graphql',
        json={'query': query}
    )
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        json_data = response.json()

        # Populate Biobank objects from the JSON data
        biobanks = populate_biobank_from_json(json_data)

        # Print FHIR-compliant JSON representation of each biobank
        for biobank in biobanks:
            print(json.dumps(biobank.to_fhir().as_json(), indent=4))
            blaze_client.BlazeClient(blaze_url='http://localhost:8080/fhir', blaze_password="",
                                     blaze_username="").upload_biobank(biobank)
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")


def fetch_organization_fhir_id(fhir_server_url: str, identifier: str) -> str:
    """
    Fetch the FHIR ID of an organization based on its identifier.

    :param fhir_server_url: The base URL of the FHIR server (e.g., "https://fhir.example.com/fhir").
    :param identifier: The identifier of the organization (e.g., "bbmri-eric:ID:CZ_MMCI").
    :return: The FHIR ID of the organization, or None if not found.
    """
    # Construct the search URL
    search_url = f"{fhir_server_url}/Organization"

    # Define the query parameters
    params = {
        "identifier": identifier
    }

    try:
        # Send a GET request to the FHIR server
        response = requests.get(search_url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the JSON response
        search_result = response.json()

        # Check if any organizations were found
        if search_result.get("total", 0) > 0:
            # Return the FHIR ID of the first organization in the search results
            return search_result["entry"][0]["resource"]["id"]
        else:
            print(f"No organization found with identifier: {identifier}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch organization: {e}")
        return None

def sync_networks():
    response = requests.post(
        'https://directory.bbmri-eric.eu/ERIC/directory/graphql',
        json={'query': networks_query}
    )
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        json_data = response.json()

        # Populate Network objects from the JSON data
        networks = populate_network_from_json(json_data)

        # Print FHIR-compliant JSON representation of each network
        for network in networks:
            print(json.dumps(network.to_fhir(network_organization_fhir_id='DFFW3QMQC7KOYRHJ').as_json(), indent=4))
            blaze_client.BlazeClient(blaze_url='http://localhost:8080/fhir', blaze_password="",
                                     blaze_username="").upload_network(network)
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")

def sync_collections():
    response = requests.post(
        'https://directory-emx2-acc.molgenis.net/ERIC/directory/graphql',
        json={'query': collections_query}
    )
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        json_data = response.json()

        # Populate Network objects from the JSON data
        collections = populate_collection_from_json(json.dumps(json_data))

        # Print FHIR-compliant JSON representation of each network
        for collection in collections:
            blaze_client.BlazeClient(blaze_url='http://localhost:8080/fhir', blaze_password="",
                                     blaze_username="").upload_collection(collection)
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")


#sync_biobanks()
#sync_networks()
sync_collections()