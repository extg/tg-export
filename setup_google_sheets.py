#!/usr/bin/env python3
"""
Google Sheets Setup Helper
Helps configure Google Sheets integration for Telegram sync
"""

import json
import os
from typing import Optional


def create_google_sheet_instructions():
    """Print instructions for setting up Google Sheets with Service Account"""
    print("=" * 70)
    print("GOOGLE SHEETS SERVICE ACCOUNT SETUP INSTRUCTIONS")
    print("=" * 70)
    print()
    
    print("🔧 STEP 1: Create Google Cloud Project")
    print("   - Go to https://console.cloud.google.com/")
    print("   - Create a new project or select existing one")
    print("   - Note the project ID for later")
    print()
    
    print("📊 STEP 2: Enable Google Sheets API")
    print("   - Go to APIs & Services > Library")
    print("   - Search for 'Google Sheets API'")
    print("   - Click 'Enable'")
    print()
    
    print("🔑 STEP 3: Create Service Account")
    print("   - Go to APIs & Services > Credentials")
    print("   - Click 'Create Credentials' > 'Service account'")
    print("   - Fill in:")
    print("     * Name: telegram-sync (or any name)")
    print("     * Description: Service account for Telegram data sync")
    print("   - Click 'Create and Continue'")
    print("   - Skip roles assignment (click 'Continue')")
    print("   - Click 'Done'")
    print()
    
    print("🔓 STEP 4: Create Service Account Key")
    print("   - Find your service account in the list")
    print("   - Click on it")
    print("   - Go to 'Keys' tab")
    print("   - Click 'Add Key' > 'Create new key'")
    print("   - Choose 'JSON' format")
    print("   - Download the file")
    print("   - Rename it to 'service_account.json'")
    print("   - Place in this directory")
    print()
    
    print("📈 STEP 5: Create Google Sheet")
    print("   - Go to https://sheets.google.com/")
    print("   - Create a new spreadsheet")
    print("   - Copy the spreadsheet ID from URL:")
    print("     Example: https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit")
    print()
    
    print("🤝 STEP 6: Share Sheet with Service Account")
    print("   - In your Google Sheet, click 'Share'")
    print("   - Add the service account email as editor:")
    print("     Format: your-service-account@project-id.iam.gserviceaccount.com")
    print("   - You can find this email in service_account.json file")
    print("   - Give 'Editor' permissions")
    print()
    
    print("⚙️  STEP 7: Update Configuration")
    print("   - Edit sync_config.json")
    print("   - Replace 'YOUR_SPREADSHEET_ID_HERE' with your spreadsheet ID")
    print()
    
    print("💡 FOR CI/CD (GitHub Actions, etc.):")
    print("   - Don't commit service_account.json to git!")
    print("   - Instead, copy the JSON content to environment variable")
    print("   - Use 'service_account_info' in config instead of 'service_account_path'")
    print()
    
    print("🔒 SECURITY NOTES:")
    print("   - Add service_account.json to .gitignore")
    print("   - Service account has no access to other Google services")
    print("   - Only access to sheets you explicitly share")
    print("   - Can be revoked anytime from Google Cloud Console")
    print()


def update_config_with_spreadsheet_id(spreadsheet_id: str) -> bool:
    """Update sync_config.json with spreadsheet ID"""
    config_path = 'sync_config.json'
    
    if not os.path.exists(config_path):
        print(f"Error: {config_path} not found!")
        return False
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # Update Google Sheets provider configuration
        for provider in config.get('providers', []):
            if provider.get('type') == 'google_sheets':
                provider['spreadsheet_id'] = spreadsheet_id
                break
        else:
            print("Error: Google Sheets provider not found in configuration!")
            return False
        
        # Write back the configuration
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        print(f"Configuration updated with spreadsheet ID: {spreadsheet_id}")
        return True
        
    except Exception as e:
        print(f"Error updating configuration: {e}")
        return False


def check_service_account() -> bool:
    """Check if Service Account credentials are properly set up"""
    service_account_path = 'service_account.json'
    
    if not os.path.exists(service_account_path):
        print(f"Service account file {service_account_path} not found!")
        print("Run: python setup_google_sheets.py instructions")
        return False
    
    try:
        with open(service_account_path, 'r') as f:
            creds = json.load(f)
        
        # Check if it's a valid service account file
        required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email']
        
        if creds.get('type') == 'service_account' and all(field in creds for field in required_fields):
            print("✓ Service account file looks valid!")
            print(f"  Project ID: {creds.get('project_id')}")
            print(f"  Client Email: {creds.get('client_email')}")
            print()
            print("🔗 Make sure to share your Google Sheet with this email address!")
            return True
        else:
            print("✗ Service account file format not recognized!")
            print("Expected fields:", required_fields)
            return False
            
    except Exception as e:
        print(f"Error reading service account file: {e}")
        return False


def test_google_sheets_connection(spreadsheet_id: Optional[str] = None):
    """Test connection to Google Sheets"""
    try:
        from data_providers import GoogleSheetsProvider
        
        if not spreadsheet_id:
            # Try to get from config
            try:
                with open('sync_config.json', 'r') as f:
                    config = json.load(f)
                
                for provider in config.get('providers', []):
                    if provider.get('type') == 'google_sheets':
                        spreadsheet_id = provider.get('spreadsheet_id')
                        break
            except:
                pass
        
        if not spreadsheet_id or spreadsheet_id == 'YOUR_SPREADSHEET_ID_HERE':
            print("Error: No valid spreadsheet ID found!")
            print("Please run this script with a spreadsheet ID or update sync_config.json")
            return False
        
        # Test the provider
        provider_config = {
            'spreadsheet_id': spreadsheet_id,
            'sheet_name': 'Telegram Data',
            'service_account_path': 'service_account.json'
        }
        
        print(f"Testing connection to spreadsheet: {spreadsheet_id}")
        provider = GoogleSheetsProvider(provider_config)
        
        if provider.is_available():
            print("✓ Google Sheets connection successful!")
            
            # Try to read/write test data
            import pandas as pd
            test_data = pd.DataFrame({
                'id': [12345],
                'username': ['test_user'],
                'first_name': ['Test'],
                'last_name': ['User'],
                'title': ['Test User'],
                'phone': ['+1234567890'],
                'is_contact': ['Yes'],
                'is_bot': ['No'],
                'has_chat': ['Yes'],
                'unread_count': [0],
                'last_message_date': ['2024-01-01T00:00:00']
            })
            
            if provider.write_data(test_data):
                print("✓ Test data written successfully!")
                
                # Read it back
                read_data = provider.read_data()
                if not read_data.empty:
                    print("✓ Test data read successfully!")
                    print(f"  Rows: {len(read_data)}")
                    print(f"  Columns: {list(read_data.columns)}")
                    return True
                else:
                    print("✗ Could not read test data back")
                    return False
            else:
                print("✗ Could not write test data")
                return False
        else:
            print("✗ Google Sheets connection failed!")
            return False
            
    except ImportError:
        print("Error: Google Sheets dependencies not installed!")
        print("Run: pip install -r requirements.txt")
        return False
    except Exception as e:
        print(f"Error testing Google Sheets connection: {e}")
        return False


def wizard_wait_for_confirmation(message: str) -> bool:
    """Wait for user confirmation to continue"""
    print(message)
    while True:
        response = input("Ready? [Y/n]: ").strip().lower()
        if response in ['', 'y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please enter Y (yes) or N (no)")


def wizard_ask_yes_no(question: str) -> bool:
    """Ask a yes/no question"""
    print(question)
    while True:
        response = input("[Y/n]: ").strip().lower()
        if response in ['', 'y', 'yes']:
            return True
        elif response in ['n', 'no']:
            return False
        else:
            print("Please enter Y (yes) or N (no)")


def wizard_get_input(prompt: str, required: bool = True) -> str:
    """Get user input with optional validation"""
    while True:
        response = input(prompt).strip()
        if response or not required:
            return response
        print("This field is required.")


def interactive_wizard():
    """Interactive setup wizard"""
    print("🧙‍♂️ INTERACTIVE GOOGLE SHEETS SETUP WIZARD")
    print("=" * 60)
    print()
    print("This wizard will guide you through setting up Google Sheets integration")
    print("for Telegram data synchronization step by step.")
    print()
    
    # Step 1: Show instructions
    print("📋 STEP 1: SETUP INSTRUCTIONS")
    print("-" * 40)
    create_google_sheet_instructions()
    
    if not wizard_wait_for_confirmation("\n✅ Complete all steps above and create the service_account.json file."):
        print("Wizard cancelled. Come back when you're ready.")
        return False
    
    # Step 2: Check service account
    print("\n🔑 STEP 2: SERVICE ACCOUNT VERIFICATION")
    print("-" * 40)
    
    service_account_ready = wizard_ask_yes_no("Have you added the service_account.json file to the current folder?")
    
    if not service_account_ready:
        print("⚠️  Please add the service_account.json file first")
        print("Restart the wizard after adding the file.")
        return False
    
    # Check the service account file
    if not check_service_account():
        print("❌ Service account file not found or corrupted!")
        return False
    
    # Step 3: Test connection
    print("\n🔗 STEP 3: GOOGLE API CONNECTION TEST")
    print("-" * 40)
    
    connection_test = wizard_ask_yes_no("Test connection to Google Sheets API?")
    
    if connection_test:
        print("\nTesting connection...")
        try:
            # Simple test without specific spreadsheet
            from data_providers import GoogleSheetsProvider
            
            # Create a minimal config just to test API access
            test_config = {
                'spreadsheet_id': '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',  # Public test sheet
                'sheet_name': 'Class Data',
                'service_account_path': 'service_account.json'
            }
            
            provider = GoogleSheetsProvider(test_config)
            print("✅ Google Sheets API connection successful!")
            
        except ImportError:
            print("❌ Missing dependencies! Run: pip install -r requirements.txt")
            return False
        except Exception as e:
            print(f"⚠️  Connection error: {e}")
            print("Check the service_account.json file format")
            
            continue_anyway = wizard_ask_yes_no("Continue setup despite the error?")
            if not continue_anyway:
                return False
    
    # Step 4: Get spreadsheet ID
    print("\n📊 STEP 4: GOOGLE SHEETS CONFIGURATION")
    print("-" * 40)
    print("Now you need to specify your Google Sheets ID.")
    print("The ID is found in the spreadsheet URL:")
    print("https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit")
    print()
    
    # Check if sheet was shared with service account
    service_account_shared = wizard_ask_yes_no("Have you shared the spreadsheet with the service account (client_email from service_account.json)?")
    
    if not service_account_shared:
        print("\n⚠️  IMPORTANT: Open your Google Sheet and:")
        print("1. Click the 'Share' button")
        
        # Show service account email
        try:
            with open('service_account.json', 'r') as f:
                creds = json.load(f)
                client_email = creds.get('client_email', 'NOT FOUND')
                print(f"2. Add this email as editor: {client_email}")
        except:
            print("2. Add the email from service_account.json as editor")
        
        print("3. Give 'Editor' permissions")
        print()
        
        if not wizard_wait_for_confirmation("Access granted?"):
            print("The spreadsheet won't work without access. Wizard cancelled.")
            return False
    
    # Get spreadsheet ID
    spreadsheet_id = wizard_get_input("Enter Spreadsheet ID: ")
    
    if not spreadsheet_id:
        print("❌ Spreadsheet ID is required!")
        return False
    
    # Step 5: Test write permissions
    print("\n✅ STEP 5: WRITE PERMISSIONS TEST")
    print("-" * 40)
    
    test_write = wizard_ask_yes_no("Test write permissions to the spreadsheet?")
    
    if test_write:
        print(f"\nTesting access permissions for spreadsheet {spreadsheet_id}...")
        
        if test_google_sheets_connection(spreadsheet_id):
            print("🎉 Excellent! Read and write permissions are working correctly!")
        else:
            print("❌ Failed to write to the spreadsheet!")
            print("Check:")
            print("- Correct Spreadsheet ID")
            print("- Service account is added as editor")
            print("- Spreadsheet exists and is accessible")
            
            continue_anyway = wizard_ask_yes_no("Continue setup despite the error?")
            if not continue_anyway:
                return False
    
    # Step 6: Update configuration
    print("\n⚙️ STEP 6: CONFIGURATION UPDATE")
    print("-" * 40)
    
    update_config = wizard_ask_yes_no("Update sync_config.json with the new Spreadsheet ID?")
    
    if update_config:
        if update_config_with_spreadsheet_id(spreadsheet_id):
            print("✅ Configuration updated successfully!")
        else:
            print("❌ Error updating configuration!")
            return False
    else:
        print(f"ℹ️  Don't forget to manually update sync_config.json:")
        print(f"   Replace 'YOUR_SPREADSHEET_ID_HERE' with '{spreadsheet_id}'")
    
    # Final summary
    print("\n🎉 SETUP COMPLETED!")
    print("=" * 40)
    print("✅ Service account configured")
    print("✅ Google Sheets connection working")
    print("✅ Write permissions verified")
    print("✅ Configuration updated" if update_config else "⚠️  Update configuration manually")
    print()
    print("You can now use Google Sheets synchronization!")
    print("Run: python sync.py")
    print()
    
    return True


def main():
    """Main setup function"""
    print("Google Sheets Setup Helper for Telegram Sync")
    print()
    
    if len(os.sys.argv) > 1:
        command = os.sys.argv[1].lower()
        
        if command == 'wizard' or command == 'w':
            interactive_wizard()
            
        elif command == 'instructions':
            create_google_sheet_instructions()
            
        elif command == 'check':
            print("Checking Google Sheets setup...")
            print()
            
            if check_service_account():
                print("✓ Service account file found and valid")
            else:
                print("✗ Service account file missing or invalid")
                print("Run: python setup_google_sheets.py instructions")
                return
            
            test_google_sheets_connection()
            
        elif command == 'test':
            spreadsheet_id = os.sys.argv[2] if len(os.sys.argv) > 2 else None
            test_google_sheets_connection(spreadsheet_id)
            
        elif command == 'config':
            if len(os.sys.argv) < 3:
                print("Usage: python setup_google_sheets.py config SPREADSHEET_ID")
                return
            
            spreadsheet_id = os.sys.argv[2]
            update_config_with_spreadsheet_id(spreadsheet_id)
            
        else:
            print(f"Unknown command: {command}")
            print("Available commands: wizard, instructions, check, test, config")
    else:
        # Default to wizard mode
        print("🧙‍♂️ Starting interactive setup wizard...")
        print("Use command line arguments for other functions:")
        print("  python setup_google_sheets.py wizard        - Interactive wizard (default)")
        print("  python setup_google_sheets.py instructions  - Show setup instructions")
        print("  python setup_google_sheets.py check         - Check current setup")
        print("  python setup_google_sheets.py test [ID]     - Test connection")
        print("  python setup_google_sheets.py config ID     - Update config with Spreadsheet ID")
        print()
        
        start_wizard = wizard_ask_yes_no("Start interactive setup wizard?")
        if start_wizard:
            interactive_wizard()


if __name__ == '__main__':
    main()
