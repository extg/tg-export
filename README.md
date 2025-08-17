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

## Configuration

### Environment Variables

Create a `.env` file or set environment variables:
```bash
TG_API_ID=your_api_id
TG_API_HASH=your_api_hash
TG_PHONE_NUMBER=your_phone_number
```

### Data Provider Configuration

Copy and configure the sync settings:
```bash
cp sync_config.json.example sync_config.json
```

Edit `sync_config.json` to configure your data providers:
```json
{
  "retry_attempts": 3,
  "retry_delay": 5,
  "providers": [
    {
      "type": "csv",
      "csv_path": "out/telegram_data.csv",
      "backup_enabled": true,
      "encoding": "utf-8"
    },
    {
      "type": "google_sheets",
      "spreadsheet_id": "YOUR_SPREADSHEET_ID_HERE",
      "sheet_name": "Telegram Data",
      "service_account_path": "service_account.json"
    }
  ]
}
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
