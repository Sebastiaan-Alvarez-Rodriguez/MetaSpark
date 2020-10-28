# This file contains a fast log reader for increasing log numbers

import util.fs as fs 

class Reader(object):
	'''
	Object to read throughput data from a path
	Expects a path with files '0.log', '1.log', ....
	Reader is ignorant of all other files and directories
	'''
	def __init__(self, path):
		if not fs.isdir(path):
			raise RuntimeError('Cannot read faulttolerance from path "{}"'.format(path))
		# Match all files with name '<number>.log', store as full path
		self.files = [x for x in fs.ls(path, only_files=True, full_paths=True) if x.endswith('.log') and fs.basename(x).split('.')[-2].isnumeric()]
		# Sort filelist on client global numbers
		self.files.sort(key=lambda x: int(fs.basename(x).split('.')[-2]))
		if self.num_files == 0:
			raise RuntimeError('Cannot find any files on path "{}"'.format(path))

	# Lazily read and return operations as they are needed
	def read_ops(self, client_id):
		with open(self.files[client_id], 'r') as file:
			return ((line.split(',')[0], int(line.split(',')[1])) for line in file.readlines())

	@property
	def num_files(self):
		return len(self.files)
		
