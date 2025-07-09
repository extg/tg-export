#!/usr/bin/env python3
"""
Telegram Export Script
Exports contacts and chats from Telegram using Telethon
"""

import asyncio
import json
import os
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import User, Chat, Channel
from telethon.tl import functions
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
API_ID = os.getenv('TG_API_ID')
API_HASH = os.getenv('TG_API_HASH')
PHONE_NUMBER = os.getenv('TG_PHONE_NUMBER')

if not API_ID or not API_HASH or not PHONE_NUMBER:
    print("Error: Set environment variables TG_API_ID, TG_API_HASH and TG_PHONE_NUMBER")
    exit(1)

client = TelegramClient('session', API_ID, API_HASH)

# Create export directory
EXPORT_DIR = 'out'
os.makedirs(EXPORT_DIR, exist_ok=True)

async def export_contacts():
    """Export contacts"""
    print("Exporting contacts...")
    # Use GetContactsRequest with hash=0 to get all contacts
    result = await client(functions.contacts.GetContactsRequest(hash=0))
    
    contacts_data = []
    # result.users contains list of users from contacts
    for contact in result.users:
        contact_info = {
            'id': contact.id,
            'username': contact.username,
            'first_name': contact.first_name,
            'last_name': contact.last_name,
            'phone': contact.phone,
            'is_bot': contact.bot
        }
        contacts_data.append(contact_info)
    
    filename = f'contacts_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    filepath = os.path.join(EXPORT_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(contacts_data, f, ensure_ascii=False, indent=2)
    
    print(f"Contacts exported to file: {filepath}")
    return len(contacts_data)

async def export_chats():
    """Export chats"""
    print("Exporting chats...")
    dialogs = await client.get_dialogs()
    
    chats_data = []
    for dialog in dialogs:
        entity = dialog.entity
        chat_info = {
            'id': entity.id,
            'title': None,
            'type': type(entity).__name__,
            'username': getattr(entity, 'username', None),
            'participants_count': getattr(entity, 'participants_count', None),
            'unread_count': dialog.unread_count,
            'last_message_date': dialog.date.isoformat() if dialog.date else None
        }
        
        if isinstance(entity, User):
            chat_info['title'] = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
        elif isinstance(entity, (Chat, Channel)):
            chat_info['title'] = entity.title
        
        chats_data.append(chat_info)
    
    filename = f'chats_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    filepath = os.path.join(EXPORT_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(chats_data, f, ensure_ascii=False, indent=2)
    
    print(f"Chats exported to file: {filepath}")
    return len(chats_data)

async def main():
    """Main function"""
    print("Starting Telegram data export...")
    
    await client.start(phone=PHONE_NUMBER)
    print("Successfully connected to Telegram!")
    
    try:
        # Export contacts
        contacts_count = await export_contacts()
        print(f"Exported contacts: {contacts_count}")
        
        # Export chats
        chats_count = await export_chats()
        print(f"Exported chats: {chats_count}")
        
        print("Export completed successfully!")
        
    except Exception as e:
        print(f"Error during export: {e}")
    
    finally:
        await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
