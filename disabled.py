import csv
import requests
import argparse
from requests.auth import HTTPBasicAuth

def is_pipeline_disabled(job_url, use_auth, user, token):
    api_url = job_url.rstrip('/') + '/api/json'
    try:
        if use_auth and user and token:
            resp = requests.get(api_url, auth=HTTPBasicAuth(user, token))
        else:
            resp = requests.get(api_url)
        if resp.status_code == 200:
            job_data = resp.json()
            return job_data.get('disabled', False)
        else:
            return f'error: HTTP {resp.status_code}'
    except Exception as e:
        return f'error: {e}'

def main():
    parser = argparse.ArgumentParser(description='Check Jenkins pipeline status.')
    parser.add_argument('--input', required=True, help='Input CSV file path')
    parser.add_argument('--output', required=True, help='Output CSV file path')
    parser.add_argument('--use-auth', action='store_true', help='Use Jenkins authentication')
    parser.add_argument('--user', default='', help='Jenkins username')
    parser.add_argument('--token', default='', help='Jenkins API token/password')
    args = parser.parse_args()

    with open(args.input, newline='') as infile:
        reader = list(csv.DictReader(infile))  # Convert to list to get total length
        total = len(reader)
    with open(args.input, newline='') as infile, open(args.output, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = ['url', 'is_disabled']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for i, row in enumerate(reader, 1):
            url = row['url']
            status = is_pipeline_disabled(url, args.use_auth, args.user, args.token)
            writer.writerow({'url': url, 'is_disabled': status})
            print(f"[{i}/{total}] Checked: {url} => {status}", flush=True)

if __name__ == '__main__':
    main()
