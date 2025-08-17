# Telegram Export Tool

Export contacts and chats from Telegram with support for CSV files and Google Sheets synchronization.

## Features

- Export contacts and chats using Telegram API
- Save data to CSV files or Google Sheets
- Merge data from multiple exports
- Google Sheets setup wizard
- Environment variable configuration
- Automatic data backups

## Architecture

### Core Components

- **`tg_export.py`** - Main export script
- **`data_providers.py`** - Data provider system for CSV and Google Sheets
- **`setup_google_sheets.py`** - Google Sheets configuration wizard

### Data Providers

1. **CSV Provider** - Local file storage
2. **Google Sheets Provider** - Cloud synchronization

## Installation

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Set up Telegram API credentials:**
   - Go to https://my.telegram.org/auth
   - Log in to your Telegram account
   - Click "API Development Tools"
   - Create a new application:
     - App title: any name (e.g., "Export Tool")
     - Short name: short name (e.g., "export")
     - Platform: Desktop
     - Description: description (optional)
   - Get your `api_id` and `api_hash`

## Required Configuration

### 🔑 Environment Variables (Required)

Create a `.env` file in the project root with the following variables:

```bash
# Telegram API settings (get from https://my.telegram.org/auth)
TG_API_ID=your_api_id_here           # Your API ID (numeric value)
TG_API_HASH=your_api_hash_here       # Your API Hash (string)
TG_PHONE_NUMBER=+1XXXXXXXXXX         # Your phone number in international format

# Google Sheets settings (optional, only if using Google Sheets)
SPREADSHEET_ID=your_spreadsheet_id_here    # Google Sheets spreadsheet ID
```

**❗ Important:**
- `TG_API_ID` and `TG_API_HASH` - required parameters for Telegram API access
- `TG_PHONE_NUMBER` - phone number linked to your Telegram account
- `SPREADSHEET_ID` - only needed if you plan to use Google Sheets synchronization

### 📊 Data Provider Configuration

The `sync_config.json` file defines where and how to save exported data:

```json
{
  "retry_attempts": 3,        // Number of retry attempts on error
  "retry_delay": 5,          // Delay between retries (seconds)
  "providers": [
    {
      "type": "csv",                        // Provider type: csv or google_sheets
      "csv_path": "out/telegram_data.csv",  // Path to CSV file
      "backup_enabled": true,               // Create backups before overwriting
      "encoding": "utf-8"                   // File encoding
    },
    {
      "type": "google_sheets",
      "spreadsheet_id": "${SPREADSHEET_ID}",     // Uses variable from .env
      "sheet_name": "Telegram Data",             // Sheet name in spreadsheet
      "service_account_path": "service_account.json"  // Path to Google credentials file
    }
  ]
}
```

**Provider Parameters:**

#### CSV Provider:
- `csv_path` - path to save data file
- `backup_enabled` - whether to create backups (recommended: `true`)
- `encoding` - file encoding (recommended: `utf-8`)

#### Google Sheets Provider:
- `spreadsheet_id` - Google Sheets spreadsheet ID (can use variable `${SPREADSHEET_ID}`)
- `sheet_name` - worksheet name in the spreadsheet
- `service_account_path` - path to JSON file with Google service account keys

### 🔒 Google Sheets Files (if using)

If you plan to use Google Sheets synchronization, you need a `service_account.json` file with access keys:

1. Create a project in Google Cloud Console
2. Enable Google Sheets API
3. Create a service account
4. Download the JSON key file
5. Rename it to `service_account.json` and place in project root

**Structure of service_account.json:**
```json
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...",
  "client_email": "your-service-account@project.iam.gserviceaccount.com",
  "client_id": "client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"
}
```

### ✅ Configuration Verification

Use built-in commands to verify your setup:

```bash
# Check Google Sheets configuration
python setup_google_sheets.py check

# Test connection to specific spreadsheet
python setup_google_sheets.py test YOUR_SPREADSHEET_ID

# Run Google Sheets setup wizard
python setup_google_sheets.py wizard
```

## Quick Start

After installing dependencies and setting up configuration (see above), create the necessary files:

1. **Create `.env` file** with your Telegram API credentials
2. **Check `sync_config.json`** - file is already configured for CSV and Google Sheets
3. **For Google Sheets**: add `service_account.json` file (if you plan to use it)

Minimal configuration for CSV-only operation:
```bash
# .env file
TG_API_ID=12345678
TG_API_HASH=abcdef1234567890abcdef1234567890
TG_PHONE_NUMBER=+1234567890
```

## Google Sheets Setup

Use the setup wizard:

```bash
python setup_google_sheets.py wizard
```

Manual setup:

1. Create Google Cloud Project and enable Google Sheets API
2. Create Service Account and download JSON credentials
3. Create Google Sheet and share with service account email
4. Update configuration with your spreadsheet ID

For detailed instructions:
```bash
python setup_google_sheets.py instructions
```

### Google Sheets Commands

```bash
python setup_google_sheets.py wizard        # Setup wizard
python setup_google_sheets.py instructions  # Setup steps
python setup_google_sheets.py check         # Verify setup
python setup_google_sheets.py test [ID]     # Test connection
python setup_google_sheets.py config ID     # Update config
```

## Usage

### Basic Export

Run the export script:
```bash
python tg_export.py
```

On first run, you'll need to enter the verification code sent to your Telegram account.

### What the Script Does

1. **Connects to Telegram** using your API credentials
2. **Exports contacts** - retrieves all contacts from your Telegram account
3. **Exports chats** - retrieves information about all your active chats
4. **Synchronizes data** - uses configured providers to store/update the data
5. **Smart merging** - combines new data with existing records intelligently

## Data Structure

The tool exports data in a unified format with the following fields:

- `id` - Telegram user ID
- `username` - User's username (if available)
- `first_name` - First name
- `last_name` - Last name
- `title` - Display name (combination of first and last name)
- `phone` - Phone number (if available)
- `is_contact` - Whether the user is in your contacts (Yes/No)
- `is_bot` - Whether the user is a bot (Yes/No)
- `has_chat` - Whether you have an active chat with this user (Yes/No)
- `unread_count` - Number of unread messages in the chat
- `last_message_date` - Date of the last message in ISO format
- `last_updated` - Timestamp when the record was last updated

## Smart Data Merging

The tool intelligently merges data from multiple sources:

- **Contacts + Chats**: Combines contact information with chat activity
- **Incremental Updates**: New exports merge with existing data
- **Field Priority**: Preserves important information (contacts, phone numbers)
- **Deduplication**: Prevents duplicate records based on Telegram user ID

## File Organization

```
out/                          # Export directory
├── telegram_data.csv         # Current CSV data (if CSV provider enabled)
├── telegram_data.csv.backup_* # Automatic backups
service_account.json          # Google Sheets credentials (not in git)
sync_config.json             # Provider configuration
.env                         # Environment variables (not in git)
```

## Data Providers

### CSV Provider

- Stores data in local CSV files
- Automatic backup creation before overwriting
- Configurable file path and encoding
- Supports incremental updates and merging

### Google Sheets Provider

- Real-time cloud synchronization
- Service Account authentication
- Automatic sheet creation and management
- Support for multiple worksheets
- Share spreadsheets with team members

## Security

- **API credentials** are stored in environment variables or `.env` files
- **Service account keys** are used for Google Sheets (more secure than OAuth)
- **Local backups** protect against data loss
- **Gitignore protection** prevents committing sensitive files

### Files excluded from git:
- `service_account.json`
- `.env`
- `session.session` (Telegram session)
- `out/*.csv` (exported data)

## Troubleshooting

### Common Issues

1. **Missing dependencies**: Run `pip install -r requirements.txt`
2. **Telegram auth error**: Check API credentials in `.env` file
3. **Google Sheets access**: Verify service account is shared with spreadsheet
4. **Configuration errors**: Use `setup_google_sheets.py check` to diagnose

### Verification Commands

```bash
# Check Google Sheets setup
python setup_google_sheets.py check

# Test specific spreadsheet
python setup_google_sheets.py test YOUR_SPREADSHEET_ID

# Check sync configuration
cat sync_config.json
```

## Development

### Provider System

The tool uses an abstract provider system for extensibility:

```python
from data_providers import DataProvider, ProviderManager

# Create custom provider
class MyProvider(DataProvider):
    def read_data(self) -> pd.DataFrame: ...
    def write_data(self, data: pd.DataFrame) -> bool: ...
    def sync_data(self, new_data: pd.DataFrame) -> pd.DataFrame: ...
    def is_available(self) -> bool: ...

# Use provider manager
manager = ProviderManager('sync_config.json')
manager.sync_data(records)
```

### Adding New Providers

1. Inherit from `DataProvider` class
2. Implement required abstract methods
3. Add to provider factory in `create_provider()`
4. Update configuration schema

## Contributing

When contributing:

1. Follow the existing code structure
2. Add tests for new providers
3. Update documentation for new features
4. Ensure security best practices

## License

This project is provided as-is for educational and personal use.
