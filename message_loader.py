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


class MessageLoader:
    """Handles loading messages from Telegram and updating Google Sheets"""
    
    def __init__(self, config_path: str = 'sync_config.json', message_limit: int = DEFAULT_MESSAGE_LIMIT):
        self.config_path = config_path
        self.message_limit = message_limit
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
                async for message in client.iter_messages(entity, limit=1):
                    if message.text:  # Found at least one text message
                        print(f"[MessageLoader]: Chat {chat_id} has messages")
                        return {
                            'has_messages': True,
                            'error': None
                        }
                
                # No text messages found
                print(f"[MessageLoader]: Chat {chat_id} has no text messages")
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
        """Format messages as readable text conversation
        
        Args:
            messages: List of message dictionaries
            entity_name: Name of the chat/contact for header
            
        Returns:
            Formatted text string of the conversation
        """
        if not messages:
            return f"=== {entity_name} ===\n[Нет сообщений]"
        
        # Sort messages by date (oldest first for natural conversation flow)
        sorted_messages = sorted(messages, key=lambda x: x.get('date', ''))
        
        # Create conversation header
        conversation_lines = [
            f"=== {entity_name} ===",
            f"Загружено сообщений: {len(messages)}",
            f"Период: {sorted_messages[0].get('date', 'N/A')} - {sorted_messages[-1].get('date', 'N/A')}",
            ""
        ]
        
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
        
        # Add footer with metadata
        conversation_lines.extend([
            "",
            f"--- Конец переписки ---",
            f"Последнее сообщение ID: {messages[0].get('id', 'N/A')}"
        ])
        
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
            required_columns = [PROCESSING_STATUS_COLUMN, MESSAGES_COLUMN, LAST_LOADED_MESSAGE_COLUMN]
            for col in required_columns:
                if col not in data.columns:
                    data[col] = ''
            
            # Filter rows that need processing
            # Rows with empty messages column and not currently being processed
            pending_mask = (
                (data[MESSAGES_COLUMN].isna() | (data[MESSAGES_COLUMN] == '')) &
                (data[PROCESSING_STATUS_COLUMN] != 'in_progress') &
                (data['id'].notna()) &
                (data['id'] != '')
            )
            
            pending_rows = data[pending_mask].copy()
            print(f"[MessageLoader]: Found {len(pending_rows)} rows pending message loading")
            
            return pending_rows
            
        except Exception as e:
            print(f"[MessageLoader]: Error getting pending rows: {e}")
            return pd.DataFrame()
    
    def update_row_status(self, row_index: int, status: str, messages_data: str = None, 
                         last_message_id: int = None) -> bool:
        """Update a specific row's processing status and message data
        
        Args:
            row_index: Index of the row in the DataFrame
            status: Processing status ('in_progress', 'done', 'error')
            messages_data: JSON string of messages (optional)
            last_message_id: ID of the last loaded message (optional)
        """
        try:
            # Read current data
            data = self.sheets_provider.read_data()
            
            if row_index >= len(data):
                print(f"[MessageLoader]: Row index {row_index} out of range")
                return False
            
            # Ensure required columns exist
            required_columns = [PROCESSING_STATUS_COLUMN, MESSAGES_COLUMN, LAST_LOADED_MESSAGE_COLUMN]
            for col in required_columns:
                if col not in data.columns:
                    data[col] = ''
            
            # Update the specific row
            data.loc[row_index, PROCESSING_STATUS_COLUMN] = status
            
            if messages_data is not None:
                data.loc[row_index, MESSAGES_COLUMN] = messages_data
            
            if last_message_id is not None:
                data.loc[row_index, LAST_LOADED_MESSAGE_COLUMN] = str(last_message_id)
            
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
                error_text = f"=== {chat_title} ===\n[ОШИБКА ПРОВЕРКИ]: {check_result['error']}\nВремя: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                success = self.update_row_status(row_index, 'error', error_text)
                print(f"[MessageLoader]: ✗ Error checking {chat_title}: {check_result['error']}")
                return success
            
            if not check_result['has_messages']:
                # Chat has no messages - mark as done with appropriate message
                no_messages_text = f"=== {chat_title} ===\n[Нет сообщений]\nВремя проверки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                success = self.update_row_status(row_index, 'done', no_messages_text, None)
                print(f"[MessageLoader]: ✓ {chat_title} has no messages - skipped")
                return success
            
            # Load messages from Telegram (only if we know there are messages)
            result = await self.load_messages_for_chat(int(chat_id))
            
            if result['error']:
                # Handle error case - format as readable text
                error_text = f"=== {chat_title} ===\n[ОШИБКА]: {result['error']}\nВремя: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                
                success = self.update_row_status(row_index, 'error', error_text)
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
                result['last_message_id']
            )
            
            if success:
                print(f"[MessageLoader]: ✓ Successfully processed {chat_title}: {len(result['messages'])} messages")
            else:
                print(f"[MessageLoader]: ✗ Failed to update {chat_title} with results")
            
            return success
            
        except Exception as e:
            print(f"[MessageLoader]: ✗ Unexpected error processing row {row_index}: {e}")
            
            # Try to set error status with readable text format
            error_text = f"=== {chat_title} ===\n[СИСТЕМНАЯ ОШИБКА]: {str(e)}\nВремя: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            self.update_row_status(row_index, 'error', error_text)
            
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
    
    args = parser.parse_args()
    
    print("[MessageLoader]: Starting Telegram message loading...")
    print(f"[MessageLoader]: Configuration:")
    print(f"  - Max rows: {args.max_rows or 'unlimited'}")
    print(f"  - Delay between rows: {args.delay} seconds")
    print(f"  - Messages per chat: {args.message_limit}")
    
    try:
        # Initialize message loader
        loader = MessageLoader(message_limit=args.message_limit)
        
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
        await client.disconnect()
        print("[MessageLoader]: Disconnected from Telegram")


if __name__ == '__main__':
    asyncio.run(main())
