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
    print("[Config]: Error: Set environment variables TG_API_ID, TG_API_HASH and TG_PHONE_NUMBER")
    exit(1)

client = TelegramClient('session', API_ID, API_HASH)

# Create export directory
EXPORT_DIR = 'out'
os.makedirs(EXPORT_DIR, exist_ok=True)



async def collect_contacts():
    """Collect contacts data without syncing"""
    print("[Collect]: Collecting contacts...")
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
            'has_chat': 'No',  # Will be updated when merging with chats
            'unread_count': 0,
            'last_message_date': '',
            'common_groups': ''  # Will be populated by common_groups_loader.py
        }
        contacts_data.append(contact_info)
    
    print(f"[Collect]: Collected {len(contacts_data)} contacts")
    return contacts_data

async def collect_chats():
    """Collect chats data without syncing"""
    print("[Collect]: Collecting chats...")
    dialogs = await client.get_dialogs()
    
    user_chat_records = []
    total_chats = 0
    
    for dialog in dialogs:
        entity = dialog.entity
        total_chats += 1
        
        if isinstance(entity, User):
            # Collect user chat records
            user_record = {
                'id': entity.id,
                'username': getattr(entity, 'username', None) or '',
                'first_name': entity.first_name or '',
                'last_name': entity.last_name or '',
                'title': f"{entity.first_name or ''} {entity.last_name or ''}".strip(),
                'phone': '',  # Phone not available in chat info
                'is_contact': 'No',  # Will be updated when merging with contacts
                'is_bot': 'Yes' if getattr(entity, 'bot', False) else 'No',
                'has_chat': 'Yes',
                'unread_count': dialog.unread_count,
                'last_message_date': dialog.date.strftime("%Y-%m-%d %H:%M:%S") if dialog.date else '',
                'common_groups': ''  # Will be populated by common_groups_loader.py
            }
            user_chat_records.append(user_record)
    
    print(f"[Collect]: Collected {total_chats} chats ({len(user_chat_records)} user chats)")
    return user_chat_records

def merge_contacts_and_chats(contacts_data, chats_data):
    """Merge contacts and chats data into a single unified dataset"""
    print("[Merge]: Merging contacts and chats data...")
    
    # Create a dictionary for efficient lookup by user ID
    merged_records = {}
    
    # First, add all contacts
    for contact in contacts_data:
        user_id = str(contact['id'])
        merged_records[user_id] = contact.copy()
    
    # Then merge chat data
    for chat in chats_data:
        user_id = str(chat['id'])
        
        if user_id in merged_records:
            # User exists in contacts, merge chat info
            existing_record = merged_records[user_id]
            
            # Update fields that should come from chat data
            existing_record['has_chat'] = 'Yes'
            existing_record['unread_count'] = chat['unread_count']
            existing_record['last_message_date'] = chat['last_message_date']
            
            # For fields like username, first_name, last_name - prefer non-empty values
            for field in ['username', 'first_name', 'last_name']:
                if chat[field] and not existing_record[field]:
                    existing_record[field] = chat[field]
                    # Update title if names were updated
                    if field in ['first_name', 'last_name']:
                        existing_record['title'] = f"{existing_record['first_name'] or ''} {existing_record['last_name'] or ''}".strip()
        else:
            # User doesn't exist in contacts, add as new record
            merged_records[user_id] = chat.copy()
    
    # Convert back to list
    merged_list = list(merged_records.values())
    
    print(f"[Merge]: Merged data: {len(merged_list)} total records ({len(contacts_data)} contacts, {len(chats_data)} chats)")
    return merged_list

async def main():
    """Main function with unified data collection and single sync"""
    print("[Main]: Starting Telegram data export...")
    
    provider_manager = ProviderManager('sync_config.json')
    
    await client.start(phone=PHONE_NUMBER)
    print("[Main]: Successfully connected to Telegram!")
    
    try:
        # Step 1: Collect all data without syncing
        contacts_data = await collect_contacts()
        chats_data = await collect_chats()
        
        # Step 2: Merge contacts and chats data
        merged_data = merge_contacts_and_chats(contacts_data, chats_data)
        
        # Step 3: Single final sync to all providers
        print("[Main]: Performing final data synchronization...")
        if merged_data:
            provider_manager.sync_data(merged_data)
            print(f"[Main]: ✓ Successfully synced {len(merged_data)} records to all providers")
        else:
            print("[Main]: No data to sync")
        
        print("[Main]: Export completed successfully!")
        
    except Exception as e:
        print(f"[Main]: Error during export: {e}")
        raise
    
    finally:
        await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
