import jobs_starccmp
from PyQt6.QtCore import QRunnable, pyqtSignal, QObject
from api import *

TEST_MODE = False	# RELEASE
PROJECT_ID = ''	# RELEASE

class WorkerSignals(QObject):
	finished = pyqtSignal(str)
	error = pyqtSignal(tuple)
	result = pyqtSignal(object)
	progress = pyqtSignal(int)


class SubmitWorker(QRunnable):
	def __init__(self, job_type, config, version, coretype, ncores, walltime,
				file_paths: list[str], java_file, sim_file, log_signal):
		super().__init__()
		self.job_type = job_type
		self.config = config
		self.version = version
		self.coretype = coretype
		self.ncores = ncores
		self.walltime = walltime
		self.file_paths = file_paths
		self.java_file_name = java_file
		self.sim_file_name = sim_file
		self.signals = WorkerSignals()
		self.log_signal = log_signal
		self.rescale_api = RescaleAPI(config['apibaseurl'], config['apikey'])

	def run(self):
		file_ids = self.upload_files()
		if file_ids:
			self.log_signal.emit(f"Upload successful: {self.file_paths[0]} and related files")
			self.submit_job(file_ids)
		else:
			self.log_signal.emit(f"Upload failed: {self.file_paths[0]} and related files")
			self.signals.error.emit(("UploadError", f"Failed to upload {self.file_paths[0]} and related files"))

	def upload_files(self):
		try:
			self.log_signal.emit(f"Uploading {self.file_paths[0]} and related files")
			return self.rescale_api.upload_files(self.file_paths, get_file_id=True)
		except Exception as e:
			self.log_signal.emit(f"Error during upload: {str(e)}")
			self.signals.error.emit((type(e).__name__, str(e)))

	def submit_job(self, file_ids):
		coretype = self.coretype
		sim_file_name = self.sim_file_name

		try:
			if TEST_MODE:
				submit_func = jobs_starccmp.create_job_test
			else:
				if coretype == 'hematite':
					submit_func = jobs_starccmp.create_job_hbv3
				elif coretype == 'natrolite':
					submit_func = jobs_starccmp.create_job_hbv4
				else:
					raise ValueError(f"Invalid coretype: {coretype}")

			# Create job
			self.log_signal.emit(f'Submitting job {sim_file_name}')
			jobname = sim_file_name.split('.')[0]
			job_data = submit_func(file_ids, self.java_file_name, sim_file_name, jobname,
									self.config['software'], self.version, self.config['license_server'],
									"1", coretype, self.ncores, self.walltime, self.config['project_code'])
			job_id = self.rescale_api.create_job(job_data)

			# Assign project (Test)
			if TEST_MODE:
				if not self.rescale_api.assign_project('rescale', job_id, PROJECT_ID):
					raise RuntimeError(f"Failed to assign project: {PROJECT_ID}")

			# Submit job
			if not self.rescale_api.submit_job(job_id):
				raise RuntimeError(f"Failed to submit the job: {job_id}")


			self.log_signal.emit(f'The job is submitted successfully (Job ID: {job_id})')
			self.signals.finished.emit(f"Done to submit the job(JOB ID: {job_id}).")
		except Exception as e:
			self.log_signal.emit(f"Error in job submission: {e}")
			self.signals.error.emit((type(e).__name__, f"Error in submit_job: {str(e)}"))