import csv
import json
import requests
from datetime import datetime

configs = {
    'production': {
        'api_base_url': "http://fireworktv.com"
    }
}

config = configs['production']


class SessionRefresher:
    def __init__(self, input_file_path, token_index):
        self.input_file_path = input_file_path
        self.header = self._get_header()
        self.accounts = self._get_accounts()
        self.updated_accounts = []
        self.failed_accounts = []
        self.jwt_token_index = token_index
        self.token_refresh_api_uri = '/api/sessions/refresh'

    def update(self):
        token_field_name = self.header[self.jwt_token_index]

        for i, account in enumerate(self.accounts):
            try:
                print('Processing row: {} \n'.format(i))
                current_token = account[token_field_name]
                new_token = self._update_one(current_token=current_token)
                account[token_field_name] = new_token
                self.updated_accounts.append(account)

                if current_token == new_token:
                    print('ERROR new token equal to current token')
            except Exception as e:
                self.failed_accounts.append(account)
                print('Failed to update token on row index: {index}, {error}'.format(index=i, error=e))

        ts = datetime.utcnow().strftime('%y_%m_%d_%H_%M_%S')

        if self.updated_accounts and len(self.updated_accounts) > 0:
            self._write_succeed_records_to_file(ts=ts)

        if self.failed_accounts and len(self.failed_accounts) > 0:
            self._write_failed_records_to_file(ts=ts)

        print('JOB COMPLETED!!!!!')

    def _update_one(self, current_token: str) -> str:
        new_token = ''
        api_endpoint = config['api_base_url'] + self.token_refresh_api_uri
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer: {current_token}'.format(current_token=current_token)
        }
        res = requests.post(url=api_endpoint, headers=headers)

        if res:
            data = json.loads(res.content)
            if data and 'token' in data:
                new_token = data['token']

        return new_token

    def _write_succeed_records_to_file(self, ts: str):
        output_file_path = self.input_file_path.replace('.csv', '_update_at_{ts}.csv'.format(ts=ts))
        with open(output_file_path, 'w') as w:
            writer = csv.DictWriter(w, fieldnames=self.header)
            writer.writeheader()
            writer.writerows(self.updated_accounts)
            print('Updated {updated_accounts_size}/{orig_accounts_size} accounts to {output_file_path}'.format(
                updated_accounts_size=len(self.updated_accounts),
                orig_accounts_size=len(self.accounts),
                output_file_path=output_file_path))

    def _write_failed_records_to_file(self, ts: str):
        output_file_path = self.input_file_path.replace('.csv', '_failed_at_{ts}.csv'.format(ts=ts))
        with open(output_file_path, 'w') as w:
            writer = csv.DictWriter(w, fieldnames=self.header)
            writer.writeheader()
            writer.writerows(self.failed_accounts)
            print('Updated {failed_accounts_size}/{orig_accounts_size} accounts to {output_file_path}'.format(
                failed_accounts_size=len(self.failed_accounts),
                orig_accounts_size=len(self.accounts),
                output_file_path=output_file_path))

    def _get_accounts(self) -> list:
        """
        Helper function to get a list of accounts from input csv file
        :return: a list of account dictionaries
        """
        with open(self.input_file_path, 'r') as f:
            instance = csv.DictReader(f)
            accounts = [account for account in instance] or []
            f.close()
        return accounts

    def _get_header(self) -> list:
        """
        Retreive the header fields for input csv file
        :return:
        """
        with open(self.input_file_path, 'r') as f:
            instance = csv.DictReader(f)
            headers = instance.fieldnames
            f.close()
        return headers

if __name__ == "__main__":
    s = SessionRefresher(
        input_file_path='/Users/lazybeam/Desktop/Firework/LicensedContent/accounts.csv',
        token_index=4
    )
    s.update()