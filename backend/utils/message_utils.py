from typing import List

from schema.message import Emoji
from utils.db import DataStorage

MESSAGE_COLLECTION = "messages"


async def get_org_messages(org_id: str) -> list:
    """Get all messages in a room
    Args:
        org_id (str): The organization id

    Returns:
        dict: key value pair of message info mapped according to message schema
    """

    DB = DataStorage(org_id)
    response = await DB.read(MESSAGE_COLLECTION, {})
    if response and "status_code" not in response:
        return response
    return {}


async def get_room_messages(org_id: str, room_id: str) -> list:
    """Get all messages in a room
    Args:
        org_id (str): The organization id
        room_id (str): The room id

    Returns:
        dict: key value pair of message info mapped according to message schema
    """

    DB = DataStorage(org_id)
    response = await DB.read(MESSAGE_COLLECTION, query={"room_id": room_id})

    return response or []


async def get_message(org_id: str, room_id: str, message_id: str) -> dict:
    """Get messages in a room
    Args:
        org_id (str): The organization id
        room_id (str): The room id
        message_id (str): The message id
    Returns:
        dict: key value pair of message info mapped according to message schema
    """
    DB = DataStorage(org_id)
    query = {"room_id": room_id, "_id": message_id}
    response = await DB.read(MESSAGE_COLLECTION, query=query)
    if response and "status_code" not in response:
        return response
    return {}


async def update_reaction(org_id: str, message: dict) -> dict:
    """Update message reactions
    Args:
        org_id (str): The organization id
        message_id (str): The message id
        message (dict): The data to update
    Returns:
        dict: key value pair of message info mapped according to message schema
    """
    DB = DataStorage(org_id)

    data = {"emojis": message["emojis"]}
    response = await DB.update(MESSAGE_COLLECTION, message["_id"], data)
    if response and "status_code" not in response:
        return response
    return {}


def append_emoji(emoji: Emoji, payload: Emoji) -> Emoji:
    """[summary]

    Args:
        emoji (Emoji): [description]
        payload (Emoji): [description]

    Returns:
        Emoji: [description]
    """

    emoji["reactedUsersId"].append(payload.reactedUsersId[0])
    emoji["count"] += 1

    return emoji


def remove_emoji(emoji: Emoji, payload: Emoji, reactions: List[Emoji]) -> Emoji:
    """[summary]

    Args:
        emoji (Emoji): [description]
        payload (Emoji): [description]
        reactions (List[Emoji]): [description]

    Returns:
        Emoji: [description]
    """

    emoji["reactedUsersId"].remove(payload.reactedUsersId[0])
    emoji["count"] -= 1
    if emoji["count"] == 0:
        reactions.remove(emoji)

    return emoji


def toggle_reaction(emoji: Emoji, payload: Emoji, reactions: List[Emoji]) -> Emoji:
    """[summary]

    Args:
        emoji (Emoji): [description]
        payload (Emoji): [description]
        reactions (List[Emoji]): [description]

    Returns:
        Emoji: [description]
    """
    if payload.reactedUsersId[0] not in emoji["reactedUsersId"]:
        return append_emoji(emoji, payload)
    return remove_emoji(emoji, payload, reactions)


def get_member_emoji(emoji_name: str, emojis: List[Emoji]):
    """[summary]

    Args:
        emoji_name (str): [description]
        emojis (List[Emoji]): [description]

    Returns:
        [type]: [description]
    """
    if not emojis:
        return None
    for emoji in emojis:
        return emoji if emoji_name == emoji["name"] else None
