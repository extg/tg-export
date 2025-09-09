#!/usr/bin/env python3
"""
Telegram Message Loader Script
Loads recent messages from chats/contacts and updates Google Sheets
"""

import asyncio
import json
import os
import pandas as pd
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import User, Chat, Channel
from telethon.errors import ChannelPrivateError, ChatAdminRequiredError, FloodWaitError
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
DEFAULT_MESSAGE_LIMIT = 20
PROCESSING_STATUS_COLUMN = 'processing_status'
MESSAGES_COLUMN = 'messages'
LAST_LOADED_MESSAGE_COLUMN = 'last_loaded_message'
TOTAL_MESSAGES_COLUMN = 'total_messages'


class MessageLoader:
    """Handles loading messages from Telegram and updating Google Sheets"""
    
    def __init__(self, config_path: str = 'sync_config.json', message_limit: int = DEFAULT_MESSAGE_LIMIT, 
                 skip_total_count: bool = False):
        self.config_path = config_path
        self.message_limit = message_limit
        self.skip_total_count = skip_total_count
        self.provider_manager = ProviderManager(config_path)
        self.sheets_provider = None
        
        # Find Google Sheets provider
        for provider in self.provider_manager.providers:
            if isinstance(provider, GoogleSheetsProvider):
                self.sheets_provider = provider
                break
        
        if not self.sheets_provider:
            raise Exception("Google Sheets provider not found in configuration")
    
    async def check_chat_has_messages(self, chat_id: int) -> Dict[str, Any]:
        """Check if chat has any messages without loading them
        
        Args:
            chat_id: Telegram chat/user ID
            
        Returns:
            Dictionary with check result
        """
        try:
            print(f"[MessageLoader]: Checking messages for chat ID: {chat_id}")
            
            # Get the entity (user/chat/channel)
            try:
                entity = await client.get_entity(chat_id)
            except Exception as e:
                print(f"[MessageLoader]: ⚠ Could not get entity for ID {chat_id}: {e}")
                return {
                    'has_messages': False,
                    'error': f"Could not access chat: {e}"
                }
            
            try:
                # Try to get just one message to check if chat has any messages
                message_found = False
                async for message in client.iter_messages(entity, limit=5):  # Check first 5 messages for better analysis
                    message_found = True
                    
                    # Log message details for debugging
                    # print(f"[MessageLoader]: DEBUG - Message ID: {message.id}")
                    # print(f"[MessageLoader]: DEBUG - Message type: {type(message).__name__}")
                    # print(f"[MessageLoader]: DEBUG - Has text: {bool(message.text)}")
                    # print(f"[MessageLoader]: DEBUG - Text content: {repr(message.text) if message.text else 'None'}")
                    # print(f"[MessageLoader]: DEBUG - Media type: {type(message.media).__name__ if message.media else 'None'}")
                    # print(f"[MessageLoader]: DEBUG - Message class: {message.__class__.__name__}")
                    
                    # Check for various message content types
                    has_content = bool(
                        message.text or 
                        message.media or 
                        hasattr(message, 'action') and message.action
                    )
                    print(f"[MessageLoader]: DEBUG - Has any content: {has_content}")
                    
                    if message.text:  # Found at least one text message
                        print(f"[MessageLoader]: Chat {chat_id} has text messages")
                        return {
                            'has_messages': True,
                            'error': None
                        }
                    
                    print("---")  # Separator between messages
                
                if message_found:
                    print(f"[MessageLoader]: Chat {chat_id} has messages but no text content")
                    # Consider messages with media/actions as valid messages too
                    return {
                        'has_messages': True,  # Changed: consider non-text messages as valid
                        'error': None
                    }
                else:
                    print(f"[MessageLoader]: Chat {chat_id} has no messages at all")
                    return {
                        'has_messages': False,
                        'error': None
                    }
                
            except (ChannelPrivateError, ChatAdminRequiredError) as e:
                print(f"[MessageLoader]: ⚠ Access denied for chat {chat_id}: {e}")
                return {
                    'has_messages': False,
                    'error': f"Access denied: {e}"
                }
            except FloodWaitError as e:
                print(f"[MessageLoader]: ⚠ Flood wait for {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                # Retry once after flood wait
                return await self.check_chat_has_messages(chat_id)
                
        except Exception as e:
            print(f"[MessageLoader]: ✗ Error checking messages for chat {chat_id}: {e}")
            return {
                'has_messages': False,
                'error': str(e)
            }

    async def count_total_messages_in_chat(self, chat_id: int) -> Dict[str, Any]:
        """Count total number of messages in a chat
        
        Args:
            chat_id: Telegram chat/user ID
            
        Returns:
            Dictionary with total message count
        """
        try:
            print(f"[MessageLoader]: Counting total messages for chat ID: {chat_id}")
            
            # Get the entity (user/chat/channel)
            try:
                entity = await client.get_entity(chat_id)
            except Exception as e:
                print(f"[MessageLoader]: ⚠ Could not get entity for ID {chat_id}: {e}")
                return {
                    'total_count': 0,
                    'error': f"Could not access chat: {e}"
                }
            
            try:
                # Count all text messages in the chat
                total_count = 0
                async for message in client.iter_messages(entity):
                    if message.text:  # Only count messages with text
                        total_count += 1
                
                print(f"[MessageLoader]: Total messages in chat {chat_id}: {total_count}")
                
                return {
                    'total_count': total_count,
                    'error': None
                }
                
            except (ChannelPrivateError, ChatAdminRequiredError) as e:
                print(f"[MessageLoader]: ⚠ Access denied for chat {chat_id}: {e}")
                return {
                    'total_count': 0,
                    'error': f"Access denied: {e}"
                }
            except FloodWaitError as e:
                print(f"[MessageLoader]: ⚠ Flood wait for {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                # Retry once after flood wait
                return await self.count_total_messages_in_chat(chat_id)
                
        except Exception as e:
            print(f"[MessageLoader]: ✗ Error counting messages for chat {chat_id}: {e}")
            return {
                'total_count': 0,
                'error': str(e)
            }

    async def load_messages_for_chat(self, chat_id: int) -> Dict[str, Any]:
        """Load recent messages from a specific chat
        
        Args:
            chat_id: Telegram chat/user ID
            
        Returns:
            Dictionary with messages data and last message ID
        """
        try:
            print(f"[MessageLoader]: Loading messages for chat ID: {chat_id}")
            
            # Get the entity (user/chat/channel)
            try:
                entity = await client.get_entity(chat_id)
            except Exception as e:
                print(f"[MessageLoader]: ⚠ Could not get entity for ID {chat_id}: {e}")
                return {
                    'messages': [],
                    'last_message_id': None,
                    'error': f"Could not access chat: {e}"
                }
            
            # Get recent messages
            messages = []
            last_message_id = None
            
            try:
                async for message in client.iter_messages(entity, limit=self.message_limit):
                    if message.text:  # Only include messages with text
                        message_data = {
                            'id': message.id,
                            'date': message.date.strftime("%Y-%m-%d %H:%M:%S") if message.date else None,
                            'text': message.text,
                            'from_id': message.from_id.user_id if message.from_id else None,
                            'is_outgoing': message.out
                        }
                        messages.append(message_data)
                        
                        # Track the latest (first) message ID
                        if last_message_id is None:
                            last_message_id = message.id
                
                print(f"[MessageLoader]: Loaded {len(messages)} messages from chat {chat_id}")
                
                return {
                    'messages': messages,
                    'last_message_id': last_message_id,
                    'error': None
                }
                
            except (ChannelPrivateError, ChatAdminRequiredError) as e:
                print(f"[MessageLoader]: ⚠ Access denied for chat {chat_id}: {e}")
                return {
                    'messages': [],
                    'last_message_id': None,
                    'error': f"Access denied: {e}"
                }
            except FloodWaitError as e:
                print(f"[MessageLoader]: ⚠ Flood wait for {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                # Retry once after flood wait
                return await self.load_messages_for_chat(chat_id)
                
        except Exception as e:
            print(f"[MessageLoader]: ✗ Error loading messages for chat {chat_id}: {e}")
            return {
                'messages': [],
                'last_message_id': None,
                'error': str(e)
            }
    
    def format_messages_as_text(self, messages: List[Dict], entity_name: str = "Chat") -> str:
        """Format messages as readable text conversation (clean format without === headers)
        
        Args:
            messages: List of message dictionaries
            entity_name: Name of the chat/contact for header
            
        Returns:
            Formatted text string of the conversation
        """
        if not messages:
            return "[Нет сообщений]"
        
        # Sort messages by date (oldest first for natural conversation flow)
        sorted_messages = sorted(messages, key=lambda x: x.get('date', ''))
        
        # Create clean conversation without headers
        conversation_lines = []
        
        # Format each message
        for msg in sorted_messages:
            date_str = msg.get('date', 'N/A')
            text = msg.get('text', '').strip()
            is_outgoing = msg.get('is_outgoing', False)
            
            # Determine sender
            if is_outgoing:
                sender = "Я"
            else:
                sender = entity_name
            
            # Format message line
            message_line = f"[{date_str}] {sender}: {text}"
            conversation_lines.append(message_line)
        
        return "\n".join(conversation_lines)
    
    def get_pending_rows(self) -> pd.DataFrame:
        """Get rows that need message loading (empty messages column)"""
        try:
            # Read current data from Google Sheets
            data = self.sheets_provider.read_data()
            
            if data.empty:
                print("[MessageLoader]: No data found in Google Sheets")
                return pd.DataFrame()
            
            # Ensure required columns exist
            required_columns = [PROCESSING_STATUS_COLUMN, MESSAGES_COLUMN, LAST_LOADED_MESSAGE_COLUMN, TOTAL_MESSAGES_COLUMN]
            for col in required_columns:
                if col not in data.columns:
                    data[col] = ''
            
            # Filter rows that need processing
            # Rows with empty messages column, not in progress, and not already done
            pending_mask = (
                (data[MESSAGES_COLUMN].isna() | (data[MESSAGES_COLUMN] == '')) &
                (data[PROCESSING_STATUS_COLUMN] != 'in_progress') &
                (data[PROCESSING_STATUS_COLUMN] != 'done') &
                (data['id'].notna()) &
                (data['id'] != '')
            )
            
            pending_rows = data[pending_mask].copy()
            print(f"[MessageLoader]: Found {len(pending_rows)} rows pending message loading")
            
            # Debug: show which rows will be processed
            # if len(pending_rows) > 0:
            #     print("[MessageLoader]: Rows to process:")
            #     for idx, (row_index, row) in enumerate(pending_rows.iterrows()):
            #         chat_title = row.get('title', f'Chat {row["id"]}')
            #         status = row.get(PROCESSING_STATUS_COLUMN, 'empty')
            #         messages_status = 'empty' if pd.isna(row.get(MESSAGES_COLUMN)) or row.get(MESSAGES_COLUMN) == '' else 'has_data'
            #         print(f"  {idx + 1}. Row {row_index}: {chat_title} (ID: {row['id']}) - Status: {status}, Messages: {messages_status}")
            
            return pending_rows
            
        except Exception as e:
            print(f"[MessageLoader]: Error getting pending rows: {e}")
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
                print("[MessageLoader]: No data found in Google Sheets")
                return {
                    'total_rows': 0,
                    'processed': 0,
                    'pending': 0,
                    'in_progress': 0,
                    'errors': 0,
                    'no_id': 0
                }
            
            # Ensure required columns exist
            required_columns = [PROCESSING_STATUS_COLUMN, MESSAGES_COLUMN]
            for col in required_columns:
                if col not in data.columns:
                    data[col] = ''
            
            # Filter out rows without valid ID (these are not processable)
            valid_id_mask = (data['id'].notna()) & (data['id'] != '')
            valid_rows = data[valid_id_mask]
            no_id_count = len(data) - len(valid_rows)
            
            # Count different statuses
            status_counts = {
                'total_rows': len(data),
                'valid_rows': len(valid_rows),
                'no_id': no_id_count,
                'processed': 0,
                'pending': 0,
                'in_progress': 0,
                'errors': 0
            }
            
            if len(valid_rows) == 0:
                return status_counts
            
            # Count by processing status
            for _, row in valid_rows.iterrows():
                status = row.get(PROCESSING_STATUS_COLUMN, '').strip().lower()
                messages = row.get(MESSAGES_COLUMN, '').strip()
                
                if status == 'done':
                    status_counts['processed'] += 1
                elif status == 'in_progress':
                    status_counts['in_progress'] += 1
                elif status == 'error':
                    status_counts['errors'] += 1
                else:
                    # Check if row has messages but no status (legacy processed rows)
                    if messages and not messages.startswith('[ОШИБКА'):
                        status_counts['processed'] += 1
                    else:
                        status_counts['pending'] += 1
            
            return status_counts
            
        except Exception as e:
            print(f"[MessageLoader]: Error getting processing status: {e}")
            return {
                'total_rows': 0,
                'processed': 0,
                'pending': 0,
                'in_progress': 0,
                'errors': 0,
                'no_id': 0
            }
    
    def update_row_status(self, row_index: int, status: str, messages_data: str = None, 
                         last_message_id: int = None, total_messages: int = None) -> bool:
        """Update a specific row's processing status and message data
        
        Args:
            row_index: Index of the row in the DataFrame
            status: Processing status ('in_progress', 'done', 'error')
            messages_data: Text string of messages (optional)
            last_message_id: ID of the last loaded message (optional)
            total_messages: Total number of messages loaded (optional)
        """
        try:
            # Read current data
            data = self.sheets_provider.read_data()
            
            if row_index >= len(data):
                print(f"[MessageLoader]: Row index {row_index} out of range")
                return False
            
            # Ensure required columns exist
            required_columns = [PROCESSING_STATUS_COLUMN, MESSAGES_COLUMN, LAST_LOADED_MESSAGE_COLUMN, TOTAL_MESSAGES_COLUMN]
            for col in required_columns:
                if col not in data.columns:
                    data[col] = ''
            
            # Update the specific row
            data.loc[row_index, PROCESSING_STATUS_COLUMN] = status
            
            if messages_data is not None:
                data.loc[row_index, MESSAGES_COLUMN] = messages_data
            
            if last_message_id is not None:
                data.loc[row_index, LAST_LOADED_MESSAGE_COLUMN] = str(last_message_id)
            
            if total_messages is not None:
                data.loc[row_index, TOTAL_MESSAGES_COLUMN] = str(total_messages)
            
            # Update last_updated timestamp
            data.loc[row_index, 'last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Write back to Google Sheets
            success = self.sheets_provider.write_data(data, create_backup=False)
            
            if success:
                chat_id = data.loc[row_index, 'id']
                print(f"[MessageLoader]: Updated row {row_index} (chat {chat_id}) with status: {status}")
            else:
                print(f"[MessageLoader]: Failed to update row {row_index}")
            
            return success
            
        except Exception as e:
            print(f"[MessageLoader]: Error updating row {row_index}: {e}")
            return False
    
    async def process_single_row(self, row: pd.Series, row_index: int) -> bool:
        """Process a single row - load messages and update status
        
        Args:
            row: Pandas Series representing the row
            row_index: Index of the row in the DataFrame
            
        Returns:
            True if successful, False otherwise
        """
        chat_id = row['id']
        chat_title = row.get('title', f'Chat {chat_id}')
        
        print(f"[MessageLoader]: Processing row {row_index}: {chat_title} (ID: {chat_id})")
        
        try:
            # Set status to in_progress
            if not self.update_row_status(row_index, 'in_progress'):
                print(f"[MessageLoader]: Failed to set in_progress status for row {row_index}")
                return False
            
            # First check if chat has any messages
            check_result = await self.check_chat_has_messages(int(chat_id))
            
            if check_result['error']:
                # Handle error from check
                error_text = f"[ОШИБКА ПРОВЕРКИ]: {check_result['error']}\nВремя: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                success = self.update_row_status(row_index, 'error', error_text, None, 0)
                print(f"[MessageLoader]: ✗ Error checking {chat_title}: {check_result['error']}")
                return success
            
            if not check_result['has_messages']:
                # Chat has no messages - mark as done with appropriate message
                no_messages_text = "[Нет сообщений]"
                success = self.update_row_status(row_index, 'done', no_messages_text, None, 0)
                print(f"[MessageLoader]: ✓ {chat_title} has no messages - skipped")
                return success
            
            # Count total messages in chat (if not skipped)
            if self.skip_total_count:
                total_messages_count = 0
                print(f"[MessageLoader]: Skipping total message count for {chat_title} (--skip-total-count)")
            else:
                count_result = await self.count_total_messages_in_chat(int(chat_id))
                total_messages_count = count_result['total_count']
                
                if count_result['error']:
                    print(f"[MessageLoader]: ⚠ Could not count total messages for {chat_title}: {count_result['error']}")
                    # Continue with loading, but total count will be 0
                    total_messages_count = 0
            
            # Load recent messages from Telegram
            result = await self.load_messages_for_chat(int(chat_id))
            
            if result['error']:
                # Handle error case - format as readable text
                error_text = f"[ОШИБКА]: {result['error']}\nВремя: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                
                success = self.update_row_status(row_index, 'error', error_text, None, 0)
                print(f"[MessageLoader]: ✗ Error processing {chat_title}: {result['error']}")
                return success
            
            # Format messages as readable text conversation
            messages_text = self.format_messages_as_text(result['messages'], chat_title)
            
            # Check if message text is too long for Google Sheets (limit: 50,000 characters)
            if len(messages_text) > 50000:
                # Truncate and add warning
                messages_text = messages_text[:49900] + "\n\n[ВНИМАНИЕ: Сообщения обрезаны из-за ограничения Google Sheets - 50,000 символов]"
                print(f"[MessageLoader]: ⚠ Messages for {chat_title} truncated due to Google Sheets limit")
            
            # Update row with success status
            success = self.update_row_status(
                row_index, 
                'done', 
                messages_text, 
                result['last_message_id'],
                total_messages_count  # Total messages in chat, not just loaded
            )
            
            if success:
                loaded_count = len(result['messages'])
                print(f"[MessageLoader]: ✓ Successfully processed {chat_title}: loaded {loaded_count} of {total_messages_count} total messages")
            else:
                print(f"[MessageLoader]: ✗ Failed to update {chat_title} with results")
            
            return success
            
        except Exception as e:
            print(f"[MessageLoader]: ✗ Unexpected error processing row {row_index}: {e}")
            
            # Try to set error status with readable text format
            error_text = f"[СИСТЕМНАЯ ОШИБКА]: {str(e)}\nВремя: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            self.update_row_status(row_index, 'error', error_text, None, 0)
            
            return False
    
    async def process_all_pending_rows(self, delay_between_rows: int = 2, max_rows: int = None) -> Dict[str, int]:
        """Process all pending rows one by one
        
        Args:
            delay_between_rows: Delay in seconds between processing rows (to avoid rate limits)
            max_rows: Maximum number of rows to process (None = process all)
            
        Returns:
            Dictionary with processing statistics
        """
        print("[MessageLoader]: Starting batch processing of pending rows...")
        
        # Get pending rows
        pending_rows = self.get_pending_rows()
        
        if pending_rows.empty:
            print("[MessageLoader]: No pending rows to process")
            return {'total': 0, 'success': 0, 'errors': 0}
        
        # Apply row limit if specified
        if max_rows and max_rows > 0:
            if len(pending_rows) > max_rows:
                pending_rows = pending_rows.head(max_rows)
                print(f"[MessageLoader]: Limited processing to first {max_rows} rows")
        
        stats = {'total': len(pending_rows), 'success': 0, 'errors': 0}
        
        print(f"[MessageLoader]: Found {stats['total']} rows to process")
        
        # Process each row
        for idx, (row_index, row) in enumerate(pending_rows.iterrows()):
            print(f"\n[MessageLoader]: Processing {idx + 1}/{stats['total']}")
            
            try:
                success = await self.process_single_row(row, row_index)
                
                if success:
                    stats['success'] += 1
                else:
                    stats['errors'] += 1
                
                # Add delay between rows (except for the last one)
                if idx < len(pending_rows) - 1 and delay_between_rows > 0:
                    print(f"[MessageLoader]: Waiting {delay_between_rows} seconds before next row...")
                    await asyncio.sleep(delay_between_rows)
                    
            except Exception as e:
                print(f"[MessageLoader]: ✗ Failed to process row {row_index}: {e}")
                stats['errors'] += 1
        
        # Print final statistics
        print(f"\n[MessageLoader]: Batch processing completed!")
        print(f"[MessageLoader]: Total: {stats['total']}, Success: {stats['success']}, Errors: {stats['errors']}")
        
        return stats


async def main():
    """Main function"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Load messages from Telegram chats')
    parser.add_argument('--max-rows', type=int, default=None, 
                       help='Maximum number of rows to process (default: all)')
    parser.add_argument('--delay', type=int, default=2,
                       help='Delay between rows in seconds (default: 2)')
    parser.add_argument('--message-limit', type=int, default=DEFAULT_MESSAGE_LIMIT,
                       help=f'Number of messages to load per chat (default: {DEFAULT_MESSAGE_LIMIT})')
    parser.add_argument('--skip-total-count', action='store_true',
                       help='Skip counting total messages in chat (faster, but total_messages will be 0)')
    parser.add_argument('--status', action='store_true',
                       help='Show processing status (how many records processed vs remaining)')
    
    args = parser.parse_args()
    
    try:
        # Initialize message loader (no need to connect to Telegram for status check)
        loader = MessageLoader(message_limit=args.message_limit, skip_total_count=args.skip_total_count)
        
        # Handle --status parameter
        if args.status:
            print("[MessageLoader]: Checking processing status...")
            
            # Get processing status without connecting to Telegram
            status = loader.get_processing_status()
            
            print("\n" + "="*50)
            print("PROCESSING STATUS REPORT")
            print("="*50)
            print(f"Total rows in sheet: {status['total_rows']}")
            print(f"Rows with valid ID: {status['valid_rows']}")
            if status['no_id'] > 0:
                print(f"Rows without ID (skipped): {status['no_id']}")
            print()
            print("Processing Status:")
            print(f"  ✓ Processed: {status['processed']} rows")
            print(f"  ⏳ Pending: {status['pending']} rows")
            if status['in_progress'] > 0:
                print(f"  🔄 In progress: {status['in_progress']} rows")
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
        
        print("[MessageLoader]: Starting Telegram message loading...")
        print(f"[MessageLoader]: Configuration:")
        print(f"  - Max rows: {args.max_rows or 'unlimited'}")
        print(f"  - Delay between rows: {args.delay} seconds")
        print(f"  - Messages per chat: {args.message_limit}")
        print(f"  - Count total messages: {'No' if args.skip_total_count else 'Yes'}")
        
        # Connect to Telegram
        await client.start(phone=PHONE_NUMBER)
        print("[MessageLoader]: Successfully connected to Telegram!")
        
        # Process pending rows with specified limits
        stats = await loader.process_all_pending_rows(
            delay_between_rows=args.delay,
            max_rows=args.max_rows
        )
        
        if stats['total'] > 0:
            success_rate = (stats['success'] / stats['total']) * 100
            print(f"[MessageLoader]: ✓ Processing completed with {success_rate:.1f}% success rate")
        else:
            print("[MessageLoader]: No rows were processed")
        
    except Exception as e:
        print(f"[MessageLoader]: ✗ Error during message loading: {e}")
        raise
    
    finally:
        # Only disconnect if we actually connected
        if not args.status:
            await client.disconnect()
            print("[MessageLoader]: Disconnected from Telegram")


if __name__ == '__main__':
    asyncio.run(main())
