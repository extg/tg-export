#!/usr/bin/env python3
"""
Data Providers for Telegram Export Tool
Supports CSV and Google Sheets synchronization
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime
import os
import json


class DataProvider(ABC):
    """Abstract base class for data providers"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.last_sync_time: Optional[datetime] = None
        # Standard columns that come from Telegram export
        self.standard_columns = {
            'id', 'username', 'first_name', 'last_name', 'title',
            'phone', 'is_contact', 'is_bot', 'has_chat', 
            'unread_count', 'last_message_date', 'last_updated'
        }
    
    @abstractmethod
    def read_data(self) -> pd.DataFrame:
        """Read data from the provider"""
        pass
    
    @abstractmethod
    def write_data(self, data: pd.DataFrame) -> bool:
        """Write data to the provider"""
        pass
    
    @abstractmethod
    def sync_data(self, new_data: pd.DataFrame) -> pd.DataFrame:
        """Synchronize new data with existing data"""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if provider is available and properly configured"""
        pass
    
    def get_last_sync_time(self) -> Optional[datetime]:
        """Get last synchronization time"""
        return self.last_sync_time
    
    def set_last_sync_time(self, sync_time: datetime):
        """Set last synchronization time"""
        self.last_sync_time = sync_time
    
    def _has_data_changed(self, existing_record: dict, new_record: dict) -> bool:
        """Check if any data field (except id and last_updated) has changed"""
        # Get ID for debug messages
        record_id = existing_record.get('id', new_record.get('id', 'unknown'))

        if record_id == 'unknown':
          raise ValueError("ID is unknown")
        
        # Fields to compare (all except id and last_updated)
        comparable_fields = self.standard_columns - {'id', 'last_updated'}

        for field in comparable_fields:
            existing_val = str(existing_record.get(field, '')).strip()
            new_val = str(new_record.get(field, '')).strip()

            # Normalize empty values for comparison
            existing_val = existing_val if existing_val else ''
            new_val = new_val if new_val else ''

            # Special normalization for phone numbers
            if field == 'phone':
                # Remove + prefix and non-digits for comparison
                existing_val = ''.join(filter(str.isdigit, existing_val))
                new_val = ''.join(filter(str.isdigit, new_val))

            # Special logic for fields that prefer non-empty new values
            if field in ['unread_count', 'last_message_date']:
                # For these fields, check if the effective value would change
                # (same logic as in preserve_additional_columns)
                effective_old = existing_val
                effective_new = new_val if new_val else existing_val

                # Special handling for date formatting (leading zeros)
                if field == 'last_message_date' and effective_old and effective_new:
                    # Normalize date formats for comparison (remove leading zeros from hours)
                    import re
                    old_normalized = re.sub(r'(\d{4}-\d{2}-\d{2} )\b0(\d):', r'\1\2:', effective_old)
                    new_normalized = re.sub(r'(\d{4}-\d{2}-\d{2} )\b0(\d):', r'\1\2:', effective_new)
                    if old_normalized == new_normalized:
                        continue  # Same date, just different formatting

                if effective_old != effective_new:
                    print(f"DEBUG: ID {record_id} - Field '{field}' changed: '{effective_old}' -> '{effective_new}'")
                    return True
            elif field == 'is_contact':
                # Special logic for is_contact field
                # This field is merged using OR logic, so individual source changes shouldn't count
                # as "data changes" since they represent different data sources, not actual data changes
                continue  # Skip is_contact changes - they don't represent actual data modifications
            else:
                # Regular comparison for other fields
                # IMPORTANT FIX: Don't count empty new values as changes when existing value exists
                # Only count as change if:
                # 1. New value is non-empty AND different from existing
                # 2. OR both values are non-empty and different
                if new_val and existing_val != new_val:
                    print(f"DEBUG: ID {record_id} - Field '{field}' changed: '{existing_val}' -> '{new_val}'")
                    return True
                elif existing_val and not new_val:
                    # This case should NOT trigger change - empty new value doesn't overwrite existing
                    continue

        # Also check for any additional fields that might be in either record
        all_fields = set(existing_record.keys()) | set(new_record.keys())
        for field in all_fields:
            if field in {'id', 'last_updated'} or field in comparable_fields:
                continue  # Skip already processed fields

            existing_val = str(existing_record.get(field, '')).strip()
            new_val = str(new_record.get(field, '')).strip()

            # Normalize empty values for comparison
            existing_val = existing_val if existing_val else ''
            new_val = new_val if new_val else ''

            # Same logic for additional fields
            if field == 'is_contact':
                # Skip is_contact changes for additional fields too
                continue
            elif new_val and existing_val != new_val:
                print(f"DEBUG: ID {record_id} - Additional field '{field}' changed: '{existing_val}' -> '{new_val}'")
                return True

        return False

    def preserve_additional_columns(self, existing_record: dict, new_record: dict) -> dict:
        """Preserve additional columns that are not part of standard Telegram export"""
        merged_record = existing_record.copy()
        
        # Check if data has actually changed
        data_changed = self._has_data_changed(existing_record, new_record)
        
        # Update last_updated if data has changed (do this first)
        if data_changed:
            merged_record['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Update with new data based on column type
        for key, new_val in new_record.items():
            existing_val = existing_record.get(key, '')
            
            # Special merge logic for key fields
            if key == 'is_contact':
                # If either source says it's a contact, it's a contact
                merged_record[key] = 'Yes' if (existing_val == 'Yes' or new_val == 'Yes') else 'No'
            elif key == 'has_chat':
                # If either source says it has chat, it has chat
                merged_record[key] = 'Yes' if (existing_val == 'Yes' or new_val == 'Yes') else 'No'
            elif key in ['phone', 'first_name', 'last_name']:
                # Prefer non-empty values, existing wins if both have values
                merged_record[key] = new_val if new_val else existing_val
            elif key in ['unread_count', 'last_message_date']:
                # Prefer newer/non-empty values for chat-related fields
                merged_record[key] = new_val if new_val else existing_val
            elif key == 'last_updated':
                # Skip - already handled above
                pass
            elif key in self.standard_columns:
                # For other standard fields - prefer new values if they exist
                merged_record[key] = new_val if new_val else existing_val
            else:
                # For additional/custom fields (like status) - preserve existing if new is empty
                if new_val or key not in existing_record:
                    merged_record[key] = new_val
                # else keep existing value
        
        return merged_record


class CSVDataProvider(DataProvider):
    """CSV file data provider"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.csv_path = config.get('csv_path', 'out/telegram_data.csv')
        self.backup_enabled = config.get('backup_enabled', True)
        self.encoding = config.get('encoding', 'utf-8')
    
    def read_data(self) -> pd.DataFrame:
        """Read data from CSV file"""
        if not os.path.exists(self.csv_path):
            # Return empty DataFrame with expected columns if file doesn't exist
            return pd.DataFrame(columns=[
                'id', 'username', 'first_name', 'last_name', 'title',
                'phone', 'is_contact', 'is_bot', 'has_chat', 
                'unread_count', 'last_message_date', 'last_updated'
            ])
        
        try:
            df = pd.read_csv(self.csv_path, encoding=self.encoding)
            # Ensure last_updated column exists
            if 'last_updated' not in df.columns:
                df['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return df
        except Exception as e:
            print(f"Error reading CSV file {self.csv_path}: {e}")
            return pd.DataFrame()
    
    def write_data(self, data: pd.DataFrame) -> bool:
        """Write data to CSV file"""
        try:
            # Create backup if enabled
            if self.backup_enabled and os.path.exists(self.csv_path):
                backup_path = f"{self.csv_path}.backup_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
                os.rename(self.csv_path, backup_path)
                print(f"Created backup: {backup_path}")
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.csv_path) if os.path.dirname(self.csv_path) else '.', exist_ok=True)
            
            # Ensure last_updated column exists for new records
            data = data.copy()
            if 'last_updated' not in data.columns:
                data['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                # Fill empty last_updated values for new records
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data['last_updated'] = data['last_updated'].fillna(current_time)
                # Replace empty strings with current time
                data.loc[data['last_updated'] == '', 'last_updated'] = current_time
            
            # Write to CSV
            data.to_csv(self.csv_path, index=False, encoding=self.encoding)
            self.set_last_sync_time(datetime.now())
            print(f"Data written to CSV: {self.csv_path}")
            return True
            
        except Exception as e:
            print(f"Error writing to CSV file {self.csv_path}: {e}")
            return False
    
    def sync_data(self, new_data: pd.DataFrame) -> pd.DataFrame:
        """Synchronize new data with existing CSV data with smart merging"""
        existing_data = self.read_data()
        
        if existing_data.empty:
            # For new data, ensure last_updated is set to current time
            new_data = new_data.copy()
            new_data['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return new_data
        
        # Convert id columns to string for consistent comparison
        existing_data['id'] = existing_data['id'].astype(str)
        new_data = new_data.copy()
        new_data['id'] = new_data['id'].astype(str)
        
        # Smart merging: combine information from both sources
        merged_records = {}
        
        # First, add all existing records
        for idx, row in existing_data.iterrows():
            merged_records[row['id']] = row.to_dict()
        
        # Then merge with new data
        for idx, new_row in new_data.iterrows():
            user_id = new_row['id']
            new_record = new_row.to_dict()
            
            if user_id in merged_records:
                # Merge existing and new data intelligently using preserve method
                existing_record = merged_records[user_id]
                merged_record = self.preserve_additional_columns(existing_record, new_record)
                merged_records[user_id] = merged_record
            else:
                # New record, add it with current timestamp
                new_record['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                merged_records[user_id] = new_record
        
        # Convert back to DataFrame
        merged_data = pd.DataFrame(list(merged_records.values()))
        
        return merged_data
    
    def is_available(self) -> bool:
        """Check if CSV provider is available"""
        try:
            # Check if we can write to the directory
            csv_dir = os.path.dirname(self.csv_path) if os.path.dirname(self.csv_path) else '.'
            return os.path.exists(csv_dir) or os.access(os.path.dirname(csv_dir) or '.', os.W_OK)
        except Exception:
            return False


class GoogleSheetsProvider(DataProvider):
    """Google Sheets data provider"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.spreadsheet_id = config.get('spreadsheet_id')
        self.sheet_name = config.get('sheet_name', 'Telegram Data')
        
        # Service Account credentials (file or JSON content)
        self.service_account_path = config.get('service_account_path', 'service_account.json')
        self.service_account_info = config.get('service_account_info')  # For env vars/secrets
        
        # Backup settings
        self.backup_enabled = config.get('backup_enabled', True)
        self.backup_spreadsheet_id = config.get('backup_spreadsheet_id')  # Optional: separate spreadsheet for backups
        self.backup_keep_count = config.get('backup_keep_count', 3)  # Number of backups to keep
        self.backup_auto_cleanup = config.get('backup_auto_cleanup', True)  # Auto cleanup old backups
        
        self._sheets_service = None
    
    def _get_sheets_service(self):
        """Get Google Sheets service instance using Service Account"""
        if self._sheets_service is not None:
            return self._sheets_service
            
        try:
            from google.oauth2.service_account import Credentials
            from googleapiclient.discovery import build
            import json
            
            SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
            
            # Get credentials from file or environment variable
            if self.service_account_info:
                # From environment variable (JSON string)
                if isinstance(self.service_account_info, str):
                    service_account_data = json.loads(self.service_account_info)
                else:
                    service_account_data = self.service_account_info
                
                creds = Credentials.from_service_account_info(
                    service_account_data, scopes=SCOPES
                )
            else:
                # From file
                if not os.path.exists(self.service_account_path):
                    raise Exception(f"Service account file not found: {self.service_account_path}")
                
                creds = Credentials.from_service_account_file(
                    self.service_account_path, scopes=SCOPES
                )
            
            self._sheets_service = build('sheets', 'v4', credentials=creds)
            return self._sheets_service
            
        except ImportError:
            raise Exception("Google Sheets dependencies not installed. Run: pip install google-auth google-auth-httplib2 google-api-python-client")
        except Exception as e:
            raise Exception(f"Failed to initialize Google Sheets service: {e}")
    
    def read_data(self) -> pd.DataFrame:
        """Read data from Google Sheets"""
        try:
            service = self._get_sheets_service()
            
            # Get sheet data
            result = service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_name}!A:Z"
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                # Return empty DataFrame with expected columns
                return pd.DataFrame(columns=[
                    'id', 'username', 'first_name', 'last_name', 'title',
                    'phone', 'is_contact', 'is_bot', 'has_chat', 
                    'unread_count', 'last_message_date', 'last_updated'
                ])
            
            # Convert to DataFrame
            headers = values[0]
            data = values[1:] if len(values) > 1 else []
            
            # Pad rows with empty strings if they're shorter than headers
            padded_data = []
            for row in data:
                padded_row = row + [''] * (len(headers) - len(row))
                padded_data.append(padded_row[:len(headers)])
            
            df = pd.DataFrame(padded_data, columns=headers)
            
            # Ensure last_updated column exists
            if 'last_updated' not in df.columns:
                df['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return df
            
        except Exception as e:
            print(f"Error reading from Google Sheets: {e}")
            return pd.DataFrame()
    
    def create_backup_sheet(self, backup_suffix: str = None) -> Optional[str]:
        """Create a backup copy of the current sheet within the same spreadsheet
        
        Args:
            backup_suffix: Optional suffix for backup sheet name (default: timestamp with minute precision)
            
        Returns:
            Name of created backup sheet or None if failed
        """
        try:
            service = self._get_sheets_service()
            
            # Get current sheet info to find sheet ID and existing sheets
            spreadsheet = service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            sheet_id = None
            existing_sheet_names = set()
            
            for sheet in spreadsheet['sheets']:
                sheet_title = sheet['properties']['title']
                existing_sheet_names.add(sheet_title)
                if sheet_title == self.sheet_name:
                    sheet_id = sheet['properties']['sheetId']
            
            if sheet_id is None:
                print(f"Sheet '{self.sheet_name}' not found!")
                return None
            
            # Generate backup name with minute precision
            if backup_suffix is None:
                backup_suffix = datetime.now().strftime("%Y%m%d_%H%M")  # Only to minute precision
            backup_name = f"{self.sheet_name}_backup_{backup_suffix}"
            
            # Check if backup with this name already exists
            if backup_name in existing_sheet_names:
                print(f"✓ Backup sheet '{backup_name}' already exists, skipping creation")
                return backup_name
            
            # Create duplicate sheet request
            duplicate_request = {
                'requests': [{
                    'duplicateSheet': {
                        'sourceSheetId': sheet_id,
                        'insertSheetIndex': 0,  # Insert at beginning
                        'newSheetName': backup_name
                    }
                }]
            }
            
            # Execute the duplication
            result = service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=duplicate_request
            ).execute()
            
            print(f"✓ Created backup sheet: '{backup_name}'")
            
            # Auto cleanup old backups if enabled
            if self.backup_auto_cleanup:
                deleted_count = self.cleanup_old_backups(self.backup_keep_count)
                if deleted_count > 0:
                    print(f"✓ Auto-cleaned {deleted_count} old backup sheets")
            
            return backup_name
            
        except Exception as e:
            print(f"Error creating backup sheet: {e}")
            return None
    
    def cleanup_old_backups(self, keep_count: int = 5) -> int:
        """Clean up old backup sheets, keeping only the most recent ones
        
        Args:
            keep_count: Number of recent backups to keep (default: 5)
            
        Returns:
            Number of backup sheets deleted
        """
        try:
            service = self._get_sheets_service()
            
            # Get all sheets in the spreadsheet
            spreadsheet = service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            
            # Find backup sheets for this sheet
            backup_prefix = f"{self.sheet_name}_backup_"
            backup_sheets = []
            
            for sheet in spreadsheet['sheets']:
                sheet_title = sheet['properties']['title']
                if sheet_title.startswith(backup_prefix):
                    # Extract timestamp from backup name
                    try:
                        timestamp_str = sheet_title[len(backup_prefix):]
                        # Parse timestamp (format: YYYYMMDD_HHMM)
                        timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M")
                        backup_sheets.append({
                            'name': sheet_title,
                            'id': sheet['properties']['sheetId'],
                            'timestamp': timestamp
                        })
                    except ValueError:
                        # Skip sheets with invalid timestamp format
                        continue
            
            # Sort by timestamp (newest first)
            backup_sheets.sort(key=lambda x: x['timestamp'], reverse=True)
            
            # Keep only the specified number of backups
            sheets_to_delete = backup_sheets[keep_count:]
            
            if not sheets_to_delete:
                print(f"✓ No old backup sheets to clean up (found {len(backup_sheets)} backups)")
                return 0
            
            # Delete old backup sheets
            delete_requests = []
            for sheet_info in sheets_to_delete:
                delete_requests.append({
                    'deleteSheet': {
                        'sheetId': sheet_info['id']
                    }
                })
            
            if delete_requests:
                batch_request = {'requests': delete_requests}
                service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=batch_request
                ).execute()
                
                deleted_names = [s['name'] for s in sheets_to_delete]
                print(f"✓ Deleted {len(sheets_to_delete)} old backup sheets: {', '.join(deleted_names)}")
            
            return len(sheets_to_delete)
            
        except Exception as e:
            print(f"Error cleaning up old backup sheets: {e}")
            return 0
    
    def copy_sheet_to_spreadsheet(self, destination_spreadsheet_id: str, 
                                  new_sheet_name: str = None) -> bool:
        """Copy current sheet to another spreadsheet
        
        Args:
            destination_spreadsheet_id: ID of destination spreadsheet
            new_sheet_name: Name for the sheet in destination (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            service = self._get_sheets_service()
            
            # Get current sheet info to find sheet ID
            spreadsheet = service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            sheet_id = None
            
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == self.sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
            
            if sheet_id is None:
                print(f"Sheet '{self.sheet_name}' not found!")
                return False
            
            # Copy sheet to destination spreadsheet
            copy_request = {
                'destinationSpreadsheetId': destination_spreadsheet_id
            }
            
            result = service.spreadsheets().sheets().copyTo(
                spreadsheetId=self.spreadsheet_id,
                sheetId=sheet_id,
                body=copy_request
            ).execute()
            
            copied_sheet_id = result['sheetId']
            
            # Rename the copied sheet if new name provided
            if new_sheet_name:
                rename_request = {
                    'requests': [{
                        'updateSheetProperties': {
                            'properties': {
                                'sheetId': copied_sheet_id,
                                'title': new_sheet_name
                            },
                            'fields': 'title'
                        }
                    }]
                }
                
                service.spreadsheets().batchUpdate(
                    spreadsheetId=destination_spreadsheet_id,
                    body=rename_request
                ).execute()
                
                print(f"✓ Sheet copied to destination spreadsheet as '{new_sheet_name}'")
            else:
                print(f"✓ Sheet copied to destination spreadsheet")
            
            return True
            
        except Exception as e:
            print(f"Error copying sheet to destination: {e}")
            return False
    
    def write_data(self, data: pd.DataFrame, create_backup: bool = None) -> bool:
        """Write data to Google Sheets with optional backup creation
        
        Args:
            data: DataFrame to write
            create_backup: Whether to create backup before writing (None=use config default)
        """
        try:
            # Create backup if requested (use config default if not specified)
            should_backup = create_backup if create_backup is not None else self.backup_enabled
            if should_backup:
                backup_name = self.create_backup_sheet()
                if backup_name:
                    print(f"✓ Backup created: {backup_name}")
                else:
                    print("⚠ Could not create backup, continuing anyway...")
            
            service = self._get_sheets_service()
            
            # Ensure last_updated column exists for new records
            data = data.copy()
            if 'last_updated' not in data.columns:
                data['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                # Fill empty last_updated values for new records
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data['last_updated'] = data['last_updated'].fillna(current_time)
                # Replace empty strings with current time
                data.loc[data['last_updated'] == '', 'last_updated'] = current_time
            
            # Prepare data for Google Sheets
            values = self._prepare_data_with_hyperlinks(data)
            
            # Clear existing data and write new data
            # First, clear the sheet
            service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_name}!A:Z"
            ).execute()
            
            # Then write new data with USER_ENTERED option for formulas
            body = {
                'values': values
            }
            
            result = service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_name}!A1",
                valueInputOption='USER_ENTERED',  # Changed to parse formulas
                body=body
            ).execute()
            
            self.set_last_sync_time(datetime.now())
            print(f"Data written to Google Sheets. Updated {result.get('updatedCells', 0)} cells.")
            return True
            
        except Exception as e:
            print(f"Error writing to Google Sheets: {e}")
            return False
    
    def sync_data(self, new_data: pd.DataFrame, create_backup: bool = None) -> pd.DataFrame:
        """Synchronize new data with existing Google Sheets data with smart merging
        
        Args:
            new_data: New data to sync
            create_backup: Whether to create backup before syncing (None=use config default)
        """
        # Create backup before syncing if requested (use config default if not specified)
        should_backup = create_backup if create_backup is not None else self.backup_enabled
        if should_backup:
            backup_name = self.create_backup_sheet()
            if backup_name:
                print(f"✓ Backup created before sync: {backup_name}")
            else:
                print("⚠ Could not create backup before sync, continuing anyway...")
        
        existing_data = self.read_data()
        
        if existing_data.empty:
            # For new data, ensure last_updated is set to current time
            new_data = new_data.copy()
            new_data['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return new_data
        
        # Convert id columns to string for consistent comparison
        existing_data['id'] = existing_data['id'].astype(str)
        new_data = new_data.copy()
        new_data['id'] = new_data['id'].astype(str)
        
        # Smart merging: combine information from both sources
        merged_records = {}
        
        # First, add all existing records
        for idx, row in existing_data.iterrows():
            merged_records[row['id']] = row.to_dict()
        
        # Then merge with new data
        for idx, new_row in new_data.iterrows():
            user_id = new_row['id']
            new_record = new_row.to_dict()
            
            if user_id in merged_records:
                # Merge existing and new data intelligently using preserve method
                existing_record = merged_records[user_id]
                merged_record = self.preserve_additional_columns(existing_record, new_record)
                merged_records[user_id] = merged_record
            else:
                # New record, add it with current timestamp
                new_record['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                merged_records[user_id] = new_record
        
        # Convert back to DataFrame
        merged_data = pd.DataFrame(list(merged_records.values()))
        
        return merged_data
    
    def is_available(self) -> bool:
        """Check if Google Sheets provider is available"""
        try:
            if not self.spreadsheet_id:
                return False
            
            # Try to get sheets service
            service = self._get_sheets_service()
            
            # Try to access the spreadsheet
            service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            return True
            
        except Exception as e:
            print(f"Google Sheets provider not available: {e}")
            return False
    
    def _prepare_data_with_hyperlinks(self, data: pd.DataFrame) -> list:
        """
        Prepare data for Google Sheets with HYPERLINK formulas for username and id columns.
        
        - ID column: Creates links to Telegram web app in format https://web.telegram.org/a/#<id>
        - Username column: Creates links to Telegram profiles in format https://t.me/<username>
        """
        # Get column headers
        headers = data.columns.tolist()
        username_col_index = None
        id_col_index = None
        
        # Find the username and id column indices
        if 'username' in headers:
            username_col_index = headers.index('username')
        if 'id' in headers:
            id_col_index = headers.index('id')
        
        # Start with headers
        values = [headers]
        
        # Process each row
        for _, row in data.iterrows():
            row_values = []
            for col_index, (col_name, cell_value) in enumerate(zip(headers, row.values)):
                # Convert to string and handle NaN values
                cell_str = str(cell_value) if pd.notna(cell_value) and cell_value != '' else ''
                
                # Special handling for id column
                if col_index == id_col_index and cell_str and cell_str != 'nan':
                    # Create HYPERLINK formula for Telegram web app
                    telegram_web_url = f"https://web.telegram.org/a/#{cell_str}"
                    # Create HYPERLINK formula with @ prefix: =HYPERLINK("url", "@url")
                    hyperlink_formula = f'=HYPERLINK("{telegram_web_url}","{cell_str}")'
                    row_values.append(hyperlink_formula)
                # Special handling for username column
                elif col_index == username_col_index and cell_str and cell_str != 'nan':
                    # Create HYPERLINK formula for Telegram profile
                    if cell_str.startswith('@'):
                        # Remove @ if present
                        username_clean = cell_str[1:]
                    else:
                        username_clean = cell_str
                    
                    # Only create hyperlink for valid usernames (not empty, not just numbers)
                    if username_clean and not username_clean.isdigit():
                        telegram_url = f"https://t.me/{username_clean}"
                        # Create HYPERLINK formula: =HYPERLINK("url", "display_text")
                        hyperlink_formula = f'=HYPERLINK("{telegram_url}","{cell_str}")'
                        row_values.append(hyperlink_formula)
                    else:
                        # If username is invalid, just use the original value
                        row_values.append(cell_str)
                else:
                    row_values.append(cell_str)
            
            values.append(row_values)
        
        return values


def create_provider(provider_type: str, config: Dict[str, Any]) -> DataProvider:
    """Factory function to create data providers"""
    providers = {
        'csv': CSVDataProvider,
        'google_sheets': GoogleSheetsProvider
    }
    
    if provider_type not in providers:
        raise ValueError(f"Unknown provider type: {provider_type}")
    
    return providers[provider_type](config)


class ProviderManager:
    """Manager for data providers - handles loading, initialization and batch operations"""
    
    def __init__(self, config_path: str = 'sync_config.json'):
        self.config_path = config_path
        self.providers: List[DataProvider] = []
        self._load_providers()
    
    def _load_providers(self):
        """Load and initialize providers from config file"""
        import json
        import os
        
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file {self.config_path} not found")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_text = f.read()
            
            # Substitute environment variables
            expanded_config_text = os.path.expandvars(config_text)
            config = json.loads(expanded_config_text)
            
            for provider_config in config.get('providers', []):
                try:
                    provider_type = provider_config.get('type')
                    if provider_type:
                        provider = create_provider(provider_type, provider_config)
                        if provider.is_available():
                            self.providers.append(provider)
                            print(f"Initialized {provider_type} provider")
                        else:
                            raise Exception(f"{provider_type} provider not available")
                except Exception as e:
                    print(f"Error initializing provider {provider_config}: {e}")
                    raise
                    
        except Exception as e:
            raise Exception(f"Error loading config from {self.config_path}: {e}")
        
        if not self.providers:
            raise Exception("No providers could be initialized")
        
        provider_names = [provider.__class__.__name__ for provider in self.providers]
        print(f"Successfully loaded {len(self.providers)} provider(s): {', '.join(provider_names)}")
    
    def sync_data(self, records: List[dict]) -> bool:
        """Sync records to all providers using their deduplication logic"""
        if not records:
            print("No records to sync")
            return True
        
        # Convert records to DataFrame
        import pandas as pd
        df = pd.DataFrame(records)
        
        success_count = 0
        for provider in self.providers:
            try:
                # Use provider's sync logic for deduplication
                synced_data = provider.sync_data(df)
                if provider.write_data(synced_data):
                    print(f"Synced {len(records)} records to {provider.__class__.__name__}")
                    success_count += 1
                else:
                    print(f"Failed to write data to {provider.__class__.__name__}")
            except Exception as e:
                print(f"Error syncing to {provider.__class__.__name__}: {e}")
        
        if success_count == 0:
            raise Exception("Failed to sync to any provider")
        
        # Log completion summary
        if success_count == len(self.providers):
            print(f"✓ Successfully synced {len(records)} records to all {len(self.providers)} provider(s)")
        else:
            print(f"⚠ Partially synced {len(records)} records to {success_count}/{len(self.providers)} provider(s)")
        
        return success_count == len(self.providers)
    
    def get_provider_count(self) -> int:
        """Get number of active providers"""
        return len(self.providers)
    
    def get_provider_names(self) -> List[str]:
        """Get names of active providers"""
        return [provider.__class__.__name__ for provider in self.providers]


def load_providers_from_config(config_path: str = 'sync_config.json') -> List[DataProvider]:
    """Legacy function - use ProviderManager instead"""
    import warnings
    warnings.warn("load_providers_from_config is deprecated, use ProviderManager instead", DeprecationWarning)
    
    manager = ProviderManager(config_path)
    return manager.providers
