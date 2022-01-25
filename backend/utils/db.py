from typing import Any, Dict

import requests
from config.settings import settings
from fastapi import status
from fastapi.exceptions import HTTPException


class DataStorage:
    """Serves as a layer of abstraction for communication between zc_messaging
    data and the database on zc_core.

    It uses API endpoints from zc_core to perform CRUD operations on zc_messaging
    collection data.

    Attributes:
        write_api str): Zc_core API endpoint for writing (POST) and updating (PUT) data.
        read_api (str): Zc_core API endpoint for reading data.
        delete_api (str): Zc_core API endpoint for deleting data.
        get_members_api (str): Zc_core API endpoint for getting members of an organization.
        organization_id (str): The organization id where the operations are to be performed.
        plugin_id (str): The zc_messaging plugin id in the plugins marketplace.

    """

    def __init__(self, organization_id: str) -> None:
        """Initializes the data storage instance with zc_messaging plugin id.

        A request is sent to the plugins marketplace API endpoint on zc_core to get
        the plugin id.

        Args:
            organization_id: The organization id where the operations are to be performed.

        Raises:
            HTTPException: {"detail": "Request Timeout"}
        """
        self.write_api = f"{settings.BASE_URL}/data/write"
        self.read_api = f"{settings.BASE_URL}/data/read"
        self.delete_api = f"{settings.BASE_URL}/data/delete"
        self.get_members_api = (
            f"{settings.BASE_URL}/organizations/{organization_id}/members/"
        )
        self.organization_id = organization_id

        try:
            response = requests.get(url=f"{settings.BASE_URL}/marketplace/plugins")
            plugins = response.json().get("data").get("plugins")
            plugin = next(
                item for item in plugins if settings.PLUGIN_KEY in item["template_url"]
            )
            self.plugin_id = plugin.get("id")
        except requests.exceptions.RequestException as exception:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT, detail="Request Timeout"
            ) from exception

    async def write(self, collection_name: str, data: Dict[str, Any]) -> Any:
        """Writes data to zc_messaging collections.

        Calls the zc_core write endpoint (POST) and writes `data` to `collection_name`.

        Args:
            collection_name (str): The name of the collection where to write `data`.
            data (dict): The actual data the plugin wants to store.

        Returns:
            On success, a dict containing the success status and
            how many documents were successfully written.

            {
                    "status": 200,
                    "message": "success",
                    "data": {
                            "insert_count": 1,
                            "object_id": "61efec7365934b58b8e5d26b"
                    }
            }

            In case of error:

            {
                "status_code": 4xx|5xx,
                "message":
                    {
                        "status": 4xx|5xx,
                        "message": 'error occurred'
                    }
            }

        Raises:
            RequestException: Unable to connect to zc_core
        """

        body = {
            "plugin_id": self.plugin_id,
            "organization_id": self.organization_id,
            "collection_name": collection_name,
            "payload": data,
        }

        try:
            response = requests.post(url=self.write_api, json=body)
        except requests.exceptions.RequestException as exception:
            print(exception)
            return None
        if response.status_code == 201:
            return response.json()
        return {"status_code": response.status_code, "message": response.json()}

    async def update(
        self, collection_name: str, document_id: str, data: Dict[str, Any]
    ) -> Any:
        """Updates data to zc_messaging collections.

        Calls the zc_core write endpoint (PUT) and updates a specific `document_id` with `data`.

        Args:
            collection_name (str): The name of the collection where to update `document_id`
            document_id (str): The document id that will be updated.
            data (dict): The new data the plugin wants to store.

        Returns:
            On success, a dict containing the success status and
            how many documents were successfully updated.

            {
                    "status": 200,
                    "message": "success",
                "data": {
                    "matched_documents": 1,
                    "modified_documents": 1
                }
            }

            In case of error:

            {
                    "status": 200,
                    "message": "success",
                "data": {
                    "matched_documents": 0,
                    "modified_documents": 0
                }
            }

        Raises:
            RequestException: Unable to connect to zc_core

        """
        body = {
            "plugin_id": self.plugin_id,
            "organization_id": self.organization_id,
            "collection_name": collection_name,
            "object_id": document_id,
            "payload": data,
        }

        try:
            response = requests.put(url=self.write_api, json=body)
        except requests.exceptions.RequestException as exception:
            print(exception)
            return None
        if response.status_code == 200:
            return response.json()
        return {"status_code": response.status_code, "message": response.json()}

    # NB: refactoring read_query into read, DB.read now has functionality of read and read_query
    async def read(
        self,
        collection_name: str,
        query: dict,
        options: dict = None,
        resource_id: str = None,
    ):
        """
        Function to read data flexibly from db, with the option to query, filter and more
        Args:
            Collection_name (str): Name of COllection,
            Resource_id (str): Document ID,
            query (dict): Filter query
            options (dict):
        Returns:
            None: cannot connect to db
            data: list; on success
            data: dict; on api call fails or errors
        """
        body = {
            "collection_name": collection_name,
            "filter": query,
            "object_id": resource_id,
            "organization_id": self.organization_id,
            "plugin_id": self.plugin_id,
            "options": options,
        }

        try:
            response = requests.post(url=self.read_api, json=body)
        except requests.exceptions.RequestException as exception:
            print(exception)
            return None
        if response.status_code == 200:
            return response.json().get("data")
        return {"status_code": response.status_code, "message": response.reason}

    async def delete(self, collection_name, document_id):
        """
        Function to del data resource from db.

        Args:
            collection_name (str): Name of collection
            Document_ID (str): Resource ID

        Returns:
            None: cannot connect to db
            data: Json object; on success
            data: dict; on api call fails or errors
        """
        body = {
            "plugin_id": self.plugin_id,
            "organization_id": self.organization_id,
            "collection_name": collection_name,
            "object_id": document_id,
        }

        try:
            response = requests.post(url=self.delete_api, json=body)
        except requests.exceptions.RequestException as exception:
            print(exception)
            return None
        if response.status_code == 200:
            return response.json()
        return {"status_code": response.status_code, "message": response.reason}

    async def get_all_members(self):
        """Gets a list of all members registered in an organisation
        Args:
            org_id (str): The organization's id
        Returns:
            [List]: [list of objects]
        """
        url = self.get_members_api.format(org_id=self.organization_id)
        try:
            response = requests.get(url=url)
        except requests.exceptions.RequestException as exception:
            print(exception)
            return []
        if response.status_code == 200:
            return response.json()["data"]

    async def get_member(self, member_id: str, members: list):
        """Get info of a single registered member in an organisation
        Args:
            org_id (str): The organization's id,
            member_id (str): The member's id
        Returns:
            {dict}: {dict containing user info}
        """
        if members:
            for member in members:
                if member["_id"] == member_id:
                    return member
        return {}
