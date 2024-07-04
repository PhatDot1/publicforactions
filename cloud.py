import requests
import re
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def extract_email(text):
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    matches = re.findall(pattern, text)
    if matches:
        return matches[0].strip('\"<>[]()')
    return None

class GitHubApiHandler:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.request_count = 0
        self.max_requests_per_key = 3650

    def get_headers(self):
        return {'Authorization': f'token {self.api_keys[self.current_key_index]}'}

    def check_and_switch_key(self):
        remaining_requests = self.get_remaining_requests()
        print(f"Remaining requests for current key: {remaining_requests}")
        if remaining_requests < 10:
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            self.request_count = 0
            print(f"Switched to new API key: {self.current_key_index + 1}")

    def get_remaining_requests(self):
        headers = self.get_headers()
        url = 'https://api.github.com/rate_limit'
        response = requests_retry_session().get(url, headers=headers)
        if response.status_code == 200:
            rate_limit_data = response.json()
            remaining = rate_limit_data['rate']['remaining']
            return remaining
        return 0

    def get_user_info_from_github_api(self, username_or_url):
        self.check_and_switch_key()
        headers = self.get_headers()
        self.request_count += 1
        if username_or_url.startswith('https://github.com/'):
            username = username_or_url.split('/')[-1]
        else:
            username = username_or_url
        url = f'https://api.github.com/users/{username}'
        response = requests_retry_session().get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to fetch user info for {username_or_url}, status code: {response.status_code}")
            return None
        user_data = response.json()
        email = user_data.get('email', '') or self.get_email_from_readme(username, headers)
        return email

    def get_email_from_readme(self, username, headers):
        url = f'https://raw.githubusercontent.com/{username}/{username}/main/README.md'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return extract_email(response.text)
        return None

def get_airtable_records(api_key, base_id, table_name):
    url = f'https://api.airtable.com/v0/{base_id}/{table_name}?filterByFormula=AND(NOT({{GitHub}} = ""), {{Status}} = "Run")'
    headers = {
        'Authorization': f'Bearer {api_key}',
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error fetching records: {response.status_code}")
        return []
    return response.json().get('records', [])

def update_airtable_record(api_key, base_id, table_name, record_id, fields):
    url = f'https://api.airtable.com/v0/{base_id}/{table_name}/{record_id}'
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
    }
    data = {
        'fields': fields
    }
    response = requests.patch(url, headers=headers, json=data)
    if response.status_code != 200:
        print(f"Error updating record {record_id}: {response.status_code}")
        return False
    print(f"Successfully updated record {record_id}")
    return True

def create_airtable_records(api_key, base_id, table_name, records):
    url = f'https://api.airtable.com/v0/{base_id}/{table_name}'
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
    }
    data = {
        'records': records
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        print(f"Error creating records: {response.status_code}")
        return False
    print(f"Successfully created records in bulk.")
    return True

def main():
    import os

    airtable_api_key = os.environ['AIRTABLE_API_KEY']
    base_id = 'appKI3FL67UBkEnGP'
    source_table_name = 'tbloH1PF4n2nxUtja'
    target_table_name = 'tblXFCGdFJMObMewK'

    with open('github_api_keys.txt', 'r') as f:
        api_keys = f.read().split(',')

    github_api_handler = GitHubApiHandler(api_keys)

    print("Fetching records from Airtable...")
    records = get_airtable_records(airtable_api_key, base_id, source_table_name)
    print(f"Fetched {len(records)} records from Airtable.")

    for record in records:
        fields = record.get('fields', {})
        github_url = fields.get('GitHub', '')
        status = fields.get('Status', '')

        print(f"Record ID: {record['id']}, GitHub URL: {github_url}, Status: {status}")

        if github_url and status == 'Run':
            print(f"Setting record {record['id']} to 'Running'")
            update_airtable_record(airtable_api_key, base_id, source_table_name, record['id'], {'Status': 'Running'})
            
            print(f"Processing record: {record['id']} with GitHub URL: {github_url}")
            try:
                email = github_api_handler.get_user_info_from_github_api(github_url)
                update_fields = {
                    'Scouted Email': email if email else '',
                    'Status': 'Done'
                }
                print(f"Updating record {record['id']} with email: {update_fields['Scouted Email']}")
                update_airtable_record(airtable_api_key, base_id, source_table_name, record['id'], update_fields)

                if email:
                    new_record = {
                        'fields': {
                            'Name': fields.get('Name', ''),
                            'Github': github_url,
                            'Scouted Email': email,
                            'Repo to link': fields.get('Repo to Link', ''),
                            'Check External Hacker Github': 'Update (Github Repo)'
                        }
                    }
                    print(f"Creating new record in target table with email: {email}")
                    create_airtable_records(airtable_api_key, base_id, target_table_name, [new_record])
            except Exception as e:
                print(f"An error occurred while processing {github_url}: {e}")

if __name__ == "__main__":
    main()
