import shutil
import requests
import subprocess
from os import path

__all__ = ['RescaleAPI']


class RescaleAPI:
    def __init__(self, api_base_url: str, api_token: str):
        self.api_base_url = api_base_url
        self.api_token = api_token
        self.headers = {'Authorization': f'Token {api_token}'}
        self.has_cli = True if shutil.which('rescale-cli') else False


    ########
    # Runs #
    def get_run_status(self, job_id: str, run_idx: int = 1):
        url = f'{self.api_base_url}/api/v2/jobs/{job_id}/runs/{run_idx}/'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        return response.json()


    def is_run_started(self, job_id: str, runs: int = 1):
        status = self.get_run_status(job_id, runs)

        if status:
            if 'dateStarted' in status.keys():
                return status['dateStarted'] != None


    def is_run_completed(self, job_id: str, runs: int = 1):
        status = self.get_run_status(job_id, runs)

        if status:
            if 'dateStarted' in status.keys():
                return status['dateCompleted'] != None
    ########
    # Runs #


    ########
    # Jobs #
    def create_job(self, job_json: dict):
        url = f'{self.api_base_url}/api/v2/jobs/'
        response = requests.post(url, headers=self.headers, json=job_json)
        response.raise_for_status()

        return response.json()['id']


    def submit_job(self, job_id: str):
        url = f'{self.api_base_url}/api/v2/jobs/{job_id}/submit/'
        response = requests.post(url, headers=self.headers)
        response.raise_for_status()

        return bool(response)


    def get_job_statuses(self, job_id: str):
        url = f'{self.api_base_url}/api/v2/jobs/{job_id}/statuses/'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        return response.json()


    def is_job_started(self, job_id: str):
        statuses = self.get_job_statuses(job_id)

        if statuses:
            results = sum(map(lambda result: result['status'] == 'Started', statuses['results']))
        return bool(results)


    def is_job_completed(self, job_id: str):
        statuses = self.get_job_statuses(job_id)

        if statuses:
            results = sum(map(lambda result: result['status'] == 'Completed', statuses['results']))
        return bool(results)


    def prioritize_job(self, organization_code: str, job_id: str, priority: int):
        if len(self.get_job_statuses(job_id)['results']):
            url = f'{self.api_base_url}/api/v2/organizations/{organization_code}/job-prioritization/'
            response = requests.post(url, headers=self.headers, json={'job': job_id, 'priority': priority})
            response.raise_for_status()

            return response.json()['priority'] == priority


    def assign_project(self, organization_code: str, job_id: str, project_id: str):
        url = f'{self.api_base_url}/api/v2/organizations/{organization_code}/jobs/{job_id}/project-assignment/'
        response = requests.post(url, headers=self.headers, json={'projectId': f'{project_id}'})
        response.raise_for_status()

        if project_id in response.text:
            return True
    ########
    # Jobs #


    #########
    # Files #
    def get_all_files(self, job_id: str):
        ret = []
        url = f'{self.api_base_url}/api/v2/jobs/{job_id}/files/'
        while url:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            ret.extend(response.json()['results'])
            url = response['next']

        if len(ret) == response['count']:
            return ret


    def upload_files(self, file_names: list[str], get_file_id: bool = True):
        if self.has_cli:
            return self.cli_base_upload(file_names, get_file_id)
        else:
            ret = tuple(self.api_base_upload(file_name, get_file_id) for file_name in file_names)
            return ret if get_file_id else len(ret) == len(file_names)


    def upload_file(self, file_name: str, get_file_id: bool = True):
        if self.has_cli:
            return self.cli_base_upload([file_name], get_file_id)
        else:
            return self.api_base_upload(file_name, get_file_id)


    def download_file(self, file_id: int, download_path: str, file_name: str, download_size: int):
        if self.has_cli and download_size > 134217728: # 128MB
            return self.cli_base_download(file_id, download_path, file_name, download_size)
        else:
            return self.api_base_download(file_id, download_path, file_name, download_size)


    def cli_base_download(self, file_id: int, download_path: str, file_name: str, download_size: int):
        command = f'rescale-cli -X {self.api_base_url} download-file -fid {file_id} -o {download_path} -p {self.api_token}'
        completed_proc = subprocess.run(command, shell=True, capture_output=True)
        completed_proc.check_returncode()

        download_dest = path.join(download_path, file_name)
        if path.exists(download_dest) and path.getsize(download_dest) == download_size:
            return True


    def cli_base_upload(self, file_names: list[str], get_file_id: bool = True):
        command = f'rescale-cli -X {self.api_base_url} upload -p {self.api_token} -f {' '.join(file_names)}'
        completed_proc = subprocess.run(command, shell=True, capture_output=True, text=True)
        completed_proc.check_returncode()

        if get_file_id:
            lines = completed_proc.stdout.split('\n')
            return tuple(line.split()[-2] for line in lines if 'File ID' in line)
        else:
            return True


    def api_base_download(self, file_id: int, download_path: str, file_name: str, download_size: int):
        url = f'{self.api_base_url}/api/v2/files/{file_id}/contents/'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        download_dest = path.join(download_path, file_name)
        with open(download_dest, 'wb') as fd:
            for chunk in response.iter_content(131072): # 128KB
                fd.write(chunk)
        if path.exists(download_dest) and path.getsize(download_dest) == download_size:
            return True


    def api_base_upload(self, file_name: str, get_file_id: bool = True):
        url = f'{self.api_base_url}/api/v2/files/contents/'
        response = requests.post(url, files={'file': open(file_name, 'rb')}, headers=self.headers)
        response.raise_for_status()

        if get_file_id:
            return response.json()['id']
        else:
            return True
    #########
    # Files #