import unittest
import subprocess, os, time
import sys, tempfile, shutil

class TestMissterOperationsLocal(unittest.TestCase):
	misster = None
	tmp_path = None

	@classmethod
	def setUpClass(self):
		self.tmp_path = tempfile.mkdtemp()

		os.makedirs(self.tmp_path + '/source')
		os.makedirs(self.tmp_path + '/cache')
		os.makedirs(self.tmp_path + '/mount')

		# Fill the source
		os.makedirs(self.tmp_path + '/source/level1/level2/level3/')

		open(self.tmp_path + '/source/file1', 'wb').write('In the town, where I was born...')
		open(self.tmp_path + '/source/file2', 'wb').write('Lived a man, who sailed the seas\nAnd he told us of his life...')
		open(self.tmp_path + '/source/level1/level2/file0', 'wb').write('In the yellow...')
		open(self.tmp_path + '/source/level1/level2/file1', 'wb').write('Submarine!')

		os.chmod(self.tmp_path + '/source/level1/level2/file1', 0600)
		os.chmod(self.tmp_path + '/source/level1/', 0750)

		# Launch our FUSE service
		self.misster = subprocess.Popen([sys.executable, 'misster.py', self.tmp_path + '/mount', '-c', self.tmp_path + '/cache', '-r', self.tmp_path + '/source', '-f'])
		time.sleep(1) # TODO: tcl/expect

		os.stat_float_times(False) # Return ints for all time data

	@classmethod
	def tearDownClass(self):
		self.misster.terminate()
		time.sleep(1) # TODO: wait for unmounting
		shutil.rmtree(self.tmp_path)

	def test_000_list_structure(self):
		"""Make sure the source tree is mirrored properly in the mount"""
		expect = []
		source_root = self.tmp_path + '/source'
		for path, dirs, files in os.walk(source_root):
			path = path.replace(source_root, '', 1).rstrip('/') + '/'
			expect.append(path)
			for f in files:
				expect.append(path + f)

		get = []
		mount_root = self.tmp_path + '/mount'
		for path, dirs, files in os.walk(mount_root):
			path = path.replace(mount_root, '', 1).rstrip('/') + '/'
			get.append(path)
			for f in files:
				get.append(path + f)

		self.assertItemsEqual(get, expect)

	def test_001_list_attributes(self):
		"""Make sure all file attributes and permissions match"""

		# Equality assertion for stat_results
		def assertStatEqual(a, b, msg=None):
			for attr in ['mode', 'nlink', 'uid', 'gid', 'size', 'ctime', 'mtime']:
				self.assertEqual(getattr(a, 'st_' + attr), getattr(b, 'st_' + attr))
		self.addTypeEqualityFunc(os.stat_result, assertStatEqual)

		source_root = self.tmp_path + '/source'
		for path, dirs, files in os.walk(source_root):
			mount_path = self.tmp_path + '/mount/' + path.replace(source_root, '', 1).rstrip('/') + '/'
			self.assertEqual(os.stat(path), os.stat(mount_path))
			for f in files:
				self.assertEqual(os.stat(path + '/' + f), os.stat(mount_path + f))

	def test_002_read_file(self):
		"""Compare file contents"""
		source_root = self.tmp_path + '/source'
		for path, dirs, files in os.walk(source_root):
			mount_path = self.tmp_path + '/mount/' + path.replace(source_root, '', 1).rstrip('/') + '/'
			for f in files:
				self.assertEqual(open(path + '/' + f).read(), open(mount_path + f).read())

	def test_003_append_file(self):
		"""Append to file and make sure it has been updated in the source"""
		open(self.tmp_path + '/mount/file1', 'a').write('NOT') # Append to existing file
		time.sleep(1) # Wait for the thread
		self.assertEqual(open(self.tmp_path + '/source/file1').read(), open(self.tmp_path + '/mount/file1').read())
	
	def test_004_write_file(self):
		"""Write a new file and make sure it has been updated in the source"""
		open(self.tmp_path + '/mount/file3', 'w').write('We all live in the yellow submarine...') # Append to existing file
		time.sleep(1) # Wait for the background thread to update the file
		self.assertEqual(open(self.tmp_path + '/source/file3').read(), open(self.tmp_path + '/mount/file3').read())

	def test_005_truncate_file(self):
		"""Make sure the file is trunctated properly"""
		open(self.tmp_path + '/mount/file3', 'w').truncate() # Truncate file
		self.assertEqual(open(self.tmp_path + '/mount/file3').read(), '')
		time.sleep(1) # Wait for the background thread to update the file
		self.assertEqual(open(self.tmp_path + '/source/file3').read(), '')

	def test_006_delete_file(self):
		"""Removing a file works"""
		os.remove(self.tmp_path + '/mount/file2')
		self.assertFalse(os.path.exists(self.tmp_path + '/mount/file2'))
		time.sleep(1) # Wait for the background thread to update the file
		self.assertFalse(os.path.exists(self.tmp_path + '/source/file2'))
