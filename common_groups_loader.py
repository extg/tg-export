#!/usr/bin/env python3
"""
Common Groups Loader Script
Loads common groups between users and updates Google Sheets
"""

import asyncio
import json
import os
import pandas as pd
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import User, Chat, Channel
from telethon.tl import functions
from telethon.errors import ChannelPrivateError, ChatAdminRequiredError, FloodWaitError, UserPrivacyRestrictedError
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any
from data_providers import ProviderManager, GoogleSheetsProvider
import time

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

# Default configuration
COMMON_GROUPS_COLUMN = 'common_groups'


class CommonGroupsLoader:
    """Handles loading common groups for users and updating Google Sheets"""
    
    def __init__(self, config_path: str = 'sync_config.json'):
        self.config_path = config_path
        self.provider_manager = ProviderManager(config_path)
        self.sheets_provider = None
        
        # Find Google Sheets provider
        for provider in self.provider_manager.providers:
            if isinstance(provider, GoogleSheetsProvider):
                self.sheets_provider = provider
                break
        
        if not self.sheets_provider:
            raise Exception("Google Sheets provider not found in configuration")
    
    async def get_common_chats_with_user(self, user_id: int) -> Dict[str, Any]:
        """Get common chats with a specific user
        
        Args:
            user_id: Telegram user ID
            
        Returns:
            Dictionary with common chats data
        """
        try:
            print(f"[CommonGroupsLoader]: Getting common chats for user ID: {user_id}")
            
            # Get the user entity
            try:
                user_entity = await client.get_entity(user_id)
            except Exception as e:
                print(f"[CommonGroupsLoader]: ⚠ Could not get user entity for ID {user_id}: {e}")
                return {
                    'common_groups': [],
                    'error': f"Could not access user: {e}"
                }
            
            try:
                # Use GetCommonChatsRequest to get common chats
                # Note: This requires the user to be a contact or have mutual contact
                result = await client(functions.messages.GetCommonChatsRequest(
                    user_id=user_entity,
                    max_id=0,
                    limit=100
                ))
                
                common_groups = []
                
                for chat in result.chats:
                    if isinstance(chat, (Chat, Channel)):
                        # Only include groups and channels, not private chats
                        group_info = {
                            'id': chat.id,
                            'title': getattr(chat, 'title', 'Unknown'),
                            'type': 'Channel' if isinstance(chat, Channel) else 'Group',
                            'members_count': getattr(chat, 'participants_count', None)
                        }
                        common_groups.append(group_info)
                
                # Format as readable text for the spreadsheet
                if common_groups:
                    group_names = [f"{group['title']} ({group['type']})" for group in common_groups]
                    groups_text = "\n".join(group_names)
                else:
                    groups_text = "[Нет общих групп]"
                
                print(f"[CommonGroupsLoader]: Found {len(common_groups)} common groups for user {user_id}")
                
                return {
                    'common_groups': groups_text,
                    'common_groups_count': len(common_groups),
                    'error': None
                }
                
            except UserPrivacyRestrictedError:
                print(f"[CommonGroupsLoader]: ⚠ User {user_id} has privacy restrictions")
                return {
                    'common_groups': "[Приватность ограничена]",
                    'common_groups_count': 0,
                    'error': "User privacy restricted"
                }
            except (ChannelPrivateError, ChatAdminRequiredError) as e:
                print(f"[CommonGroupsLoader]: ⚠ Access denied for user {user_id}: {e}")
                return {
                    'common_groups': "[Доступ запрещен]",
                    'common_groups_count': 0,
                    'error': f"Access denied: {e}"
                }
            except FloodWaitError as e:
                print(f"[CommonGroupsLoader]: ⚠ Flood wait for {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                # Retry once after flood wait
                return await self.get_common_chats_with_user(user_id)
                
        except Exception as e:
            print(f"[CommonGroupsLoader]: ✗ Error getting common chats for user {user_id}: {e}")
            return {
                'common_groups': f"[Ошибка: {str(e)}]",
                'common_groups_count': 0,
                'error': str(e)
            }

    def get_pending_rows(self) -> pd.DataFrame:
        """Get rows that need common groups loading (empty common_groups column)"""
        try:
            # Read current data from Google Sheets
            data = self.sheets_provider.read_data()
            
            if data.empty:
                print("[CommonGroupsLoader]: No data found in Google Sheets")
                return pd.DataFrame()
            
            # Ensure common_groups column exists
            if COMMON_GROUPS_COLUMN not in data.columns:
                data[COMMON_GROUPS_COLUMN] = ''
            
            # Filter rows that need processing
            # Rows with empty common_groups column and valid ID
            pending_mask = (
                (data[COMMON_GROUPS_COLUMN].isna() | (data[COMMON_GROUPS_COLUMN] == '')) &
                (data['id'].notna()) &
                (data['id'] != '')
            )
            
            pending_rows = data[pending_mask].copy()
            print(f"[CommonGroupsLoader]: Found {len(pending_rows)} rows pending common groups loading")
            
            return pending_rows
            
        except Exception as e:
            print(f"[CommonGroupsLoader]: Error getting pending rows: {e}")
            return pd.DataFrame()
    
    def get_processing_status(self) -> Dict[str, int]:
        """Get processing status statistics for all rows
        
        Returns:
            Dictionary with processing statistics
        """
        try:
            # Read current data from Google Sheets
            data = self.sheets_provider.read_data()
            
            if data.empty:
                print("[CommonGroupsLoader]: No data found in Google Sheets")
                return {
                    'total_rows': 0,
                    'processed': 0,
                    'pending': 0,
                    'errors': 0,
                    'no_id': 0
                }
            
            # Ensure common_groups column exists
            if COMMON_GROUPS_COLUMN not in data.columns:
                data[COMMON_GROUPS_COLUMN] = ''
            
            # Filter out rows without valid ID (these are not processable)
            valid_id_mask = (data['id'].notna()) & (data['id'] != '')
            valid_rows = data[valid_id_mask]
            no_id_count = len(data) - len(valid_rows)
            
            # Count different statuses (now processing all valid rows, not just contacts)
            status_counts = {
                'total_rows': len(data),
                'valid_rows': len(valid_rows),
                'no_id': no_id_count,
                'processed': 0,
                'pending': 0,
                'errors': 0
            }
            
            if len(valid_rows) == 0:
                return status_counts
            
            # Count by common_groups status for all valid rows
            for _, row in valid_rows.iterrows():
                common_groups = row.get(COMMON_GROUPS_COLUMN, '').strip()
                
                if not common_groups:
                    # Empty common_groups column - pending
                    status_counts['pending'] += 1
                elif common_groups.startswith('[ОШИБКА') or common_groups.startswith('[СИСТЕМНАЯ ОШИБКА'):
                    # Error messages
                    status_counts['errors'] += 1
                else:
                    # Has data (groups, no groups, privacy restricted, etc.) - processed
                    status_counts['processed'] += 1
            
            return status_counts
            
        except Exception as e:
            print(f"[CommonGroupsLoader]: Error getting processing status: {e}")
            return {
                'total_rows': 0,
                'processed': 0,
                'pending': 0,
                'errors': 0,
                'no_id': 0
            }
    
    def update_row_common_groups(self, row_index: int, common_groups_data: str) -> bool:
        """Update a specific row's common groups data
        
        Args:
            row_index: Index of the row in the DataFrame
            common_groups_data: Text string of common groups
        """
        try:
            # Read current data
            data = self.sheets_provider.read_data()
            
            if row_index >= len(data):
                print(f"[CommonGroupsLoader]: Row index {row_index} out of range")
                return False
            
            # Ensure common_groups column exists
            if COMMON_GROUPS_COLUMN not in data.columns:
                data[COMMON_GROUPS_COLUMN] = ''
            
            # Update the specific row
            data.loc[row_index, COMMON_GROUPS_COLUMN] = common_groups_data
            
            # Update last_updated timestamp
            data.loc[row_index, 'last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Write back to Google Sheets
            success = self.sheets_provider.write_data(data, create_backup=False)
            
            if success:
                user_id = data.loc[row_index, 'id']
                print(f"[CommonGroupsLoader]: Updated row {row_index} (user {user_id}) with common groups")
            else:
                print(f"[CommonGroupsLoader]: Failed to update row {row_index}")
            
            return success
            
        except Exception as e:
            print(f"[CommonGroupsLoader]: Error updating row {row_index}: {e}")
            return False
    
    async def process_single_row(self, row: pd.Series, row_index: int) -> bool:
        """Process a single row - load common groups and update
        
        Args:
            row: Pandas Series representing the row
            row_index: Index of the row in the DataFrame
            
        Returns:
            True if successful, False otherwise
        """
        user_id = row['id']
        user_title = row.get('title', f'User {user_id}')
        
        print(f"[CommonGroupsLoader]: Processing row {row_index}: {user_title} (ID: {user_id})")
        
        try:
            # Get common chats with the user
            result = await self.get_common_chats_with_user(int(user_id))
            
            # Update row with common groups data
            success = self.update_row_common_groups(row_index, result['common_groups'])
            
            if success:
                print(f"[CommonGroupsLoader]: ✓ Successfully processed {user_title}: {result['common_groups_count']} common groups")
            else:
                print(f"[CommonGroupsLoader]: ✗ Failed to update {user_title} with results")
            
            return success
            
        except Exception as e:
            print(f"[CommonGroupsLoader]: ✗ Unexpected error processing row {row_index}: {e}")
            
            # Try to set error message
            error_text = f"[СИСТЕМНАЯ ОШИБКА]: {str(e)}"
            self.update_row_common_groups(row_index, error_text)
            
            return False
    
    async def process_all_pending_rows(self, delay_between_rows: int = 3, max_rows: int = None) -> Dict[str, int]:
        """Process all pending rows one by one
        
        Args:
            delay_between_rows: Delay in seconds between processing rows (to avoid rate limits)
            max_rows: Maximum number of rows to process (None = process all)
            
        Returns:
            Dictionary with processing statistics
        """
        print("[CommonGroupsLoader]: Starting batch processing of pending rows...")
        
        # Get pending rows
        pending_rows = self.get_pending_rows()
        
        if pending_rows.empty:
            print("[CommonGroupsLoader]: No pending rows to process")
            return {'total': 0, 'success': 0, 'errors': 0}
        
        # Apply row limit if specified
        if max_rows and max_rows > 0:
            if len(pending_rows) > max_rows:
                pending_rows = pending_rows.head(max_rows)
                print(f"[CommonGroupsLoader]: Limited processing to first {max_rows} rows")
        
        stats = {'total': len(pending_rows), 'success': 0, 'errors': 0}
        
        print(f"[CommonGroupsLoader]: Found {stats['total']} rows to process")
        
        # Process each row
        for idx, (row_index, row) in enumerate(pending_rows.iterrows()):
            print(f"\n[CommonGroupsLoader]: Processing {idx + 1}/{stats['total']}")
            
            try:
                success = await self.process_single_row(row, row_index)
                
                if success:
                    stats['success'] += 1
                else:
                    stats['errors'] += 1
                
                # Add delay between rows (except for the last one)
                if idx < len(pending_rows) - 1 and delay_between_rows > 0:
                    print(f"[CommonGroupsLoader]: Waiting {delay_between_rows} seconds before next row...")
                    await asyncio.sleep(delay_between_rows)
                    
            except Exception as e:
                print(f"[CommonGroupsLoader]: ✗ Failed to process row {row_index}: {e}")
                stats['errors'] += 1
        
        # Print final statistics
        print(f"\n[CommonGroupsLoader]: Batch processing completed!")
        print(f"[CommonGroupsLoader]: Total: {stats['total']}, Success: {stats['success']}, Errors: {stats['errors']}")
        
        return stats


async def main():
    """Main function"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Load common groups for Telegram contacts')
    parser.add_argument('--max-rows', type=int, default=None, 
                       help='Maximum number of rows to process (default: all)')
    parser.add_argument('--delay', type=int, default=3,
                       help='Delay between rows in seconds (default: 3)')
    parser.add_argument('--status', action='store_true',
                       help='Show processing status (how many records processed vs remaining)')
    
    args = parser.parse_args()
    
    try:
        # Initialize common groups loader (no need to connect to Telegram for status check)
        loader = CommonGroupsLoader()
        
        # Handle --status parameter
        if args.status:
            print("[CommonGroupsLoader]: Checking processing status...")
            
            # Get processing status without connecting to Telegram
            status = loader.get_processing_status()
            
            print("\n" + "="*50)
            print("COMMON GROUPS PROCESSING STATUS REPORT")
            print("="*50)
            print(f"Total rows in sheet: {status['total_rows']}")
            print(f"Rows with valid ID (processable): {status['valid_rows']}")
            if status['no_id'] > 0:
                print(f"Rows without ID (skipped): {status['no_id']}")
            print()
            print("Processing Status:")
            print(f"  ✓ Processed: {status['processed']} rows")
            print(f"  ⏳ Pending: {status['pending']} rows")
            if status['errors'] > 0:
                print(f"  ❌ Errors: {status['errors']} rows")
            print()
            
            if status['valid_rows'] > 0:
                processed_percent = (status['processed'] / status['valid_rows']) * 100
                pending_percent = (status['pending'] / status['valid_rows']) * 100
                print(f"Progress: {processed_percent:.1f}% completed, {pending_percent:.1f}% remaining")
            else:
                print("No valid rows found to process")
            
            print("="*50)
            return
        
        print("[CommonGroupsLoader]: Starting Telegram common groups loading...")
        print(f"[CommonGroupsLoader]: Configuration:")
        print(f"  - Max rows: {args.max_rows or 'unlimited'}")
        print(f"  - Delay between rows: {args.delay} seconds")
        
        # Connect to Telegram
        await client.start(phone=PHONE_NUMBER)
        print("[CommonGroupsLoader]: Successfully connected to Telegram!")
        
        # Process pending rows with specified limits
        stats = await loader.process_all_pending_rows(
            delay_between_rows=args.delay,
            max_rows=args.max_rows
        )
        
        if stats['total'] > 0:
            success_rate = (stats['success'] / stats['total']) * 100
            print(f"[CommonGroupsLoader]: ✓ Processing completed with {success_rate:.1f}% success rate")
        else:
            print("[CommonGroupsLoader]: No rows were processed")
        
    except Exception as e:
        print(f"[CommonGroupsLoader]: ✗ Error during common groups loading: {e}")
        raise
    
    finally:
        # Only disconnect if we actually connected
        if not args.status:
            await client.disconnect()
            print("[CommonGroupsLoader]: Disconnected from Telegram")


if __name__ == '__main__':
    asyncio.run(main())
