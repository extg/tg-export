#!/usr/bin/env python3
"""
Telegram Export Script
Exports contacts and chats from Telegram using Telethon with provider integration
"""

import asyncio
import json
import os
import pandas as pd
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import User, Chat, Channel
from telethon.tl import functions
from dotenv import load_dotenv
from typing import List, Optional
from data_providers import ProviderManager

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



async def export_contacts(provider_manager: Optional[ProviderManager] = None):
    """Export contacts and sync to providers in batches"""
    print("Exporting contacts...")
    # Use GetContactsRequest with hash=0 to get all contacts
    result = await client(functions.contacts.GetContactsRequest(hash=0))
    
    contacts_data = []
    # result.users contains list of users from contacts
    for contact in result.users:
        contact_info = {
            'id': contact.id,
            'username': contact.username or '',
            'first_name': contact.first_name or '',
            'last_name': contact.last_name or '',
            'title': f"{contact.first_name or ''} {contact.last_name or ''}".strip(),
            'phone': contact.phone or '',
            'is_contact': 'Yes',
            'is_bot': 'Yes' if contact.bot else 'No',
            'has_chat': 'No',  # Will be updated when processing chats
            'unread_count': 0,
            'last_message_date': ''
        }
        contacts_data.append(contact_info)
    
    # Sync all contacts to providers
    if provider_manager and contacts_data:
        provider_manager.sync_data(contacts_data)
    
    print(f"Processed {len(contacts_data)} contacts")
    return len(contacts_data)

async def export_chats(provider_manager: Optional[ProviderManager] = None):
    """Export chats and sync to providers in batches"""
    print("Exporting chats...")
    dialogs = await client.get_dialogs()
    
    user_chat_records = []
    total_chats = 0
    
    for dialog in dialogs:
        entity = dialog.entity
        total_chats += 1
        
        if isinstance(entity, User):
            # Collect user chat records for batch sync
            if provider_manager:
                user_record = {
                    'id': entity.id,
                    'username': getattr(entity, 'username', None) or '',
                    'first_name': entity.first_name or '',
                    'last_name': entity.last_name or '',
                    'title': f"{entity.first_name or ''} {entity.last_name or ''}".strip(),
                    'phone': '',  # Phone not available in chat info
                    'is_contact': 'No',  # Will be updated by provider's sync logic if contact exists
                    'is_bot': 'Yes' if getattr(entity, 'bot', False) else 'No',
                    'has_chat': 'Yes',
                    'unread_count': dialog.unread_count,
                    'last_message_date': dialog.date.strftime("%Y-%m-%d %H:%M:%S") if dialog.date else ''
                }
                user_chat_records.append(user_record)
    
    # Sync all user chats to providers
    if provider_manager and user_chat_records:
        provider_manager.sync_data(user_chat_records)
    
    print(f"Processed {total_chats} chats ({len(user_chat_records)} user chats synced to providers)")
    return total_chats

async def main():
    """Main function with provider sync (required)"""
    print("Starting Telegram data export...")
    
    provider_manager = ProviderManager()
    
    await client.start(phone=PHONE_NUMBER)
    print("Successfully connected to Telegram!")
    
    try:
        contacts_count = await export_contacts(provider_manager)
        print(f"Exported {contacts_count} contacts")
        
        chats_count = await export_chats(provider_manager)
        print(f"Exported {chats_count} chats")
        
        print("Export completed successfully!")
        
    except Exception as e:
        print(f"Error during export: {e}")
        raise
    
    finally:
        await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
