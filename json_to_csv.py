#!/usr/bin/env python3
"""
JSON to CSV converter for exported Telegram data
Creates CSV file only with personal chats (User)
"""

import json
import csv
import glob
import os
from datetime import datetime

def find_latest_files():
    """Finds latest contact and chat files in out folder"""
    # Check if out folder exists
    if not os.path.exists('out'):
        print("Folder 'out' not found!")
        return None, None
    
    contact_files = glob.glob('out/contacts_*.json')
    chat_files = glob.glob('out/chats_*.json')
    
    if not contact_files:
        print("Contact files not found in out folder!")
        return None, None
    
    if not chat_files:
        print("Chat files not found in out folder!")
        return None, None
    
    # Sort by modification time and take latest
    latest_contacts = max(contact_files, key=os.path.getmtime)
    latest_chats = max(chat_files, key=os.path.getmtime)
    
    return latest_contacts, latest_chats

def load_json_data(filename):
    """Loads data from JSON file"""
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)

def create_user_csv(contacts_file, chats_file):
    """Creates CSV file only with personal chats"""
    # Load data
    print(f"Loading contacts from {contacts_file}...")
    contacts_data = load_json_data(contacts_file)
    
    print(f"Loading chats from {chats_file}...")
    chats_data = load_json_data(chats_file)
    
    # Create contacts dictionary for quick lookup
    contacts_dict = {contact['id']: contact for contact in contacts_data}
    
    # Filter only personal chats (User)
    user_chats = [chat for chat in chats_data if chat['type'] == 'User']
    chats_dict = {chat['id']: chat for chat in user_chats}
    
    print(f"Found {len(contacts_data)} contacts")
    print(f"Found {len(user_chats)} personal chats out of {len(chats_data)} total")
    
    # Collect all unique records
    all_users = {}
    
    # First add all contacts
    for contact in contacts_data:
        user_id = contact['id']
        chat_info = chats_dict.get(user_id, {})
        
        all_users[user_id] = {
            'id': user_id,
            'username': contact.get('username', '') or chat_info.get('username', ''),
            'first_name': contact.get('first_name', ''),
            'last_name': contact.get('last_name', ''),
            'title': chat_info.get('title', '') or f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip(),
            'phone': contact.get('phone', ''),
            'is_contact': 'Yes',
            'is_bot': 'Yes' if contact.get('is_bot', False) else 'No',
            'unread_count': chat_info.get('unread_count', 0),
            'last_message_date': chat_info.get('last_message_date', ''),
            'has_chat': 'Yes' if user_id in chats_dict else 'No'
        }
    
    # Then add users from chats who are not in contacts
    for chat in user_chats:
        user_id = chat['id']
        if user_id not in all_users:
            all_users[user_id] = {
                'id': user_id,
                'username': chat.get('username', ''),
                'first_name': '',
                'last_name': '',
                'title': chat['title'],
                'phone': '',
                'is_contact': 'No',
                'is_bot': 'No',  # Assume not a bot if not in contacts
                'unread_count': chat.get('unread_count', 0),
                'last_message_date': chat.get('last_message_date', ''),
                'has_chat': 'Yes'
            }
    
    # Create CSV file
    output_filename = f'user_chats_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    os.makedirs('out', exist_ok=True)
    output_filepath = os.path.join('out', output_filename)
    
    with open(output_filepath, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'id',
            'username',
            'first_name',
            'last_name',
            'title',
            'phone',
            'is_contact',
            'is_bot',
            'has_chat',
            'unread_count',
            'last_message_date'
        ]
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Sort by ID for convenience
        for user_id in sorted(all_users.keys()):
            writer.writerow(all_users[user_id])
    
    print(f"\nCSV file created: {output_filepath}")
    print(f"Exported {len(all_users)} unique records")
    
    # Show statistics
    contacts_count = sum(1 for u in all_users.values() if u['is_contact'] == 'Yes')
    chats_count = sum(1 for u in all_users.values() if u['has_chat'] == 'Yes')
    both_count = sum(1 for u in all_users.values() if u['is_contact'] == 'Yes' and u['has_chat'] == 'Yes')
    
    print(f"\nStatistics:")
    print(f"- Total unique users: {len(all_users)}")
    print(f"- In contacts: {contacts_count}")
    print(f"- Has chat: {chats_count}")
    print(f"- Both in contacts and has chat: {both_count}")
    print(f"- Only in contacts (no chat): {contacts_count - both_count}")
    print(f"- Only chat (not in contacts): {chats_count - both_count}")
    
    return output_filepath

def main():
    """Main function"""
    print("Converting Telegram data to CSV...")
    
    # Find latest files
    contacts_file, chats_file = find_latest_files()
    
    if not contacts_file or not chats_file:
        print("Could not find files to process!")
        return
    
    # Create CSV
    create_user_csv(contacts_file, chats_file)

if __name__ == '__main__':
    main() 
