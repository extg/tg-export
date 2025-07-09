# Telegram Export Tool

A simple script for exporting contacts and chats from Telegram using the Telethon library.

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## API Keys Setup

1. Go to https://my.telegram.org/auth
2. Log in to your Telegram account
3. Click "API Development Tools"
4. Create a new application:
   - App title: any name (e.g., "Export Tool")
   - Short name: short name (e.g., "export")
   - Platform: Desktop
   - Description: description (optional)
5. Get your `api_id` and `api_hash`

## Usage

### Setting up environment variables

Copy the example file and fill in your data:
```bash
cp .env.example .env
```

Edit the `.env` file and specify your data:
```
TG_API_ID=your_api_id
TG_API_HASH=your_api_hash
TG_PHONE_NUMBER=your_phone_number
```

Alternatively, you can set environment variables directly:
```bash
export TG_API_ID="12345678"
export TG_API_HASH="abcdef1234567890abcdef1234567890"
export TG_PHONE_NUMBER="+71234567890"
```

### 1. Export data from Telegram

Run the export script:
```bash
python tg_export.py
```

On first run, enter the verification code that will be sent to Telegram.

### 2. Convert to CSV

After exporting data, you can create a CSV file with personal chats:
```bash
python json_to_csv.py
```

The script will automatically find the latest exported files and create a CSV with all contacts and personal chats.

## Results

All result files are saved in the `out/` folder.

### Export from Telegram
The `tg_export.py` script creates two JSON files:
- `out/contacts_YYYYMMDD_HHMMSS.json` - contacts export
- `out/chats_YYYYMMDD_HHMMSS.json` - chats export

### CSV file
The `json_to_csv.py` script creates a CSV file:
- `out/user_chats_YYYYMMDD_HHMMSS.csv` - combined contact and personal chat data

The CSV contains the following fields:
- `id` - Telegram user ID
- `username` - user's username
- `first_name` - first name
- `last_name` - last name
- `title` - display name in chat
- `phone` - phone number (if available)
- `is_contact` - whether in contacts (Yes/No)
- `is_bot` - whether is a bot (Yes/No)
- `has_chat` - whether has active chat (Yes/No)
- `unread_count` - number of unread messages
- `last_message_date` - date of last message

## Data Format

### Contacts
```json
{
  "id": 123456789,
  "username": "username",
  "first_name": "First Name",
  "last_name": "Last Name",
  "phone": "+71234567890",
  "is_bot": false
}
```

### Chats
```json
{
  "id": 123456789,
  "title": "Chat Title",
  "type": "User|Chat|Channel",
  "username": "username",
  "participants_count": 100,
  "unread_count": 5,
  "last_message_date": "2024-01-01T12:00:00"
}
```

## Security

- The session file `session.session` is created to save authorization
- Never share your API keys with third parties
- Keep environment variables secure
- It's recommended to use a `.env` file for storing environment variables
- All exported data (JSON and CSV files) in the `out/` folder are excluded from git via `.gitignore`
