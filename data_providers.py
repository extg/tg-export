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
                df['last_updated'] = datetime.now().isoformat()
            return df
        except Exception as e:
            print(f"Error reading CSV file {self.csv_path}: {e}")
            return pd.DataFrame()
    
    def write_data(self, data: pd.DataFrame) -> bool:
        """Write data to CSV file"""
        try:
            # Create backup if enabled
            if self.backup_enabled and os.path.exists(self.csv_path):
                backup_path = f"{self.csv_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                os.rename(self.csv_path, backup_path)
                print(f"Created backup: {backup_path}")
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.csv_path) if os.path.dirname(self.csv_path) else '.', exist_ok=True)
            
            # Add/update last_updated timestamp
            data = data.copy()
            data['last_updated'] = datetime.now().isoformat()
            
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
                # Merge existing and new data intelligently
                existing_record = merged_records[user_id]
                
                # Combine data - prefer non-empty values
                merged_record = {}
                for key in new_record.keys():
                    existing_val = existing_record.get(key, '')
                    new_val = new_record.get(key, '')
                    
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
                    else:
                        # Default: prefer new values
                        merged_record[key] = new_val if new_val else existing_val
                
                merged_records[user_id] = merged_record
            else:
                # New record, add it
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
                df['last_updated'] = datetime.now().isoformat()
            
            return df
            
        except Exception as e:
            print(f"Error reading from Google Sheets: {e}")
            return pd.DataFrame()
    
    def write_data(self, data: pd.DataFrame) -> bool:
        """Write data to Google Sheets"""
        try:
            service = self._get_sheets_service()
            
            # Add/update last_updated timestamp
            data = data.copy()
            data['last_updated'] = datetime.now().isoformat()
            
            # Prepare data for Google Sheets
            values = [data.columns.tolist()] + data.fillna('').astype(str).values.tolist()
            
            # Clear existing data and write new data
            # First, clear the sheet
            service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_name}!A:Z"
            ).execute()
            
            # Then write new data
            body = {
                'values': values
            }
            
            result = service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_name}!A1",
                valueInputOption='RAW',
                body=body
            ).execute()
            
            self.set_last_sync_time(datetime.now())
            print(f"Data written to Google Sheets. Updated {result.get('updatedCells', 0)} cells.")
            return True
            
        except Exception as e:
            print(f"Error writing to Google Sheets: {e}")
            return False
    
    def sync_data(self, new_data: pd.DataFrame) -> pd.DataFrame:
        """Synchronize new data with existing Google Sheets data with smart merging"""
        existing_data = self.read_data()
        
        if existing_data.empty:
            return new_data
        
        # Convert id columns to string for consistent comparison
        existing_data['id'] = existing_data['id'].astype(str)
        new_data = new_data.copy()
        new_data['id'] = new_data['id'].astype(str)
        
        # Smart merging: combine information from both sources (same logic as CSV)
        merged_records = {}
        
        # First, add all existing records
        for idx, row in existing_data.iterrows():
            merged_records[row['id']] = row.to_dict()
        
        # Then merge with new data
        for idx, new_row in new_data.iterrows():
            user_id = new_row['id']
            new_record = new_row.to_dict()
            
            if user_id in merged_records:
                # Merge existing and new data intelligently
                existing_record = merged_records[user_id]
                
                # Combine data - prefer non-empty values
                merged_record = {}
                for key in new_record.keys():
                    existing_val = existing_record.get(key, '')
                    new_val = new_record.get(key, '')
                    
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
                    else:
                        # Default: prefer new values
                        merged_record[key] = new_val if new_val else existing_val
                
                merged_records[user_id] = merged_record
            else:
                # New record, add it
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
                config = json.load(f)
            
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
