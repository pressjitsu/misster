import unittest
import subprocess, os, time
import sys, tempfile, shutil, errno

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
		self.assertNotIn('file2', os.listdir(self.tmp_path + '/mount/'))
		time.sleep(1) # Wait for the background thread to update the file
		self.assertFalse(os.path.exists(self.tmp_path + '/source/file2'))
		self.assertNotIn('file2', os.listdir(self.tmp_path + '/source/'))

	def test_007_mknod(self):
		"""A created file should appear in the tree and the source with the correct permissions"""
		os.mknod(self.tmp_path + '/mount/file4', 0640)
		self.assertIn('file4', os.listdir(self.tmp_path + '/mount/'))
		self.assertEqual(os.stat(self.tmp_path + '/mount/file4').st_mode & 0777, 0640)
		time.sleep(1)
		self.assertIn('file4', os.listdir(self.tmp_path + '/source/'))
		self.assertEqual(os.stat(self.tmp_path + '/source/file4').st_mode & 0777, 0640)

	def test_008_create_dir(self):
		"""Creating a directory should work without any issues and correct permissions"""
		os.makedirs(self.tmp_path + '/mount/level4/level5/level6/', 0700)
		self.assertIn('level4', os.listdir(self.tmp_path + '/mount/'))
		self.assertIn('level5', os.listdir(self.tmp_path + '/mount/level4'))
		self.assertIn('level6', os.listdir(self.tmp_path + '/mount/level4/level5'))
		self.assertEqual(os.stat(self.tmp_path + '/mount/level4/level5/level6').st_mode & 0777, 0700)
		time.sleep(1)
		self.assertTrue(os.path.exists(self.tmp_path + '/source/level4/level5/level6'))
		self.assertEqual(os.stat(self.tmp_path + '/source/level4/level5/level6').st_mode & 0777, 0700)

	def test_009_rmdir(self):
		"""Removing a directory possible, most times"""
		with self.assertRaises(OSError) as e:
			os.rmdir(self.tmp_path + '/mount/level4')
		self.assertEqual(e.exception.errno, errno.ENOTEMPTY)

		os.rmdir(self.tmp_path + '/mount/level4/level5/level6')
		self.assertNotIn('level6', os.listdir(self.tmp_path + '/mount/level4/level5'))
		time.sleep(1)
		self.assertNotIn('level6', os.listdir(self.tmp_path + '/source/level4/level5'))

	def test_010_chmod(self):
		"""File and directory modes are set and propagated correctly"""
		os.chmod(self.tmp_path + '/mount/file1', 0600)
		self.assertEqual(os.stat(self.tmp_path + '/mount/file1').st_mode & 0777, 0600)
		time.sleep(1)
		self.assertEqual(os.stat(self.tmp_path + '/source/file1').st_mode & 0777, 0600)

	def test_011_time(self):
		"""atime and mtime change as files are accessed and modified"""
		# TODO: ctime for chmods should also work, and directories...
		# TODO: atime propagation depends on underlying fs really (noatime)
		atime = os.stat(self.tmp_path + '/mount/file1').st_atime
		mtime = os.stat(self.tmp_path + '/mount/file1').st_mtime
		s_mtime = os.stat(self.tmp_path + '/source/file1').st_mtime
		time.sleep(1)
		open(self.tmp_path + '/mount/file1').read()
		self.assertNotEqual(os.stat(self.tmp_path + '/mount/file1').st_atime, atime)
		open(self.tmp_path + '/mount/file1', 'w').write('hello')
		self.assertNotEqual(os.stat(self.tmp_path + '/mount/file1').st_mtime, mtime)
		time.sleep(1)
		self.assertNotEqual(os.stat(self.tmp_path + '/source/file1').st_mtime, s_mtime)

	def test_12_partial_reads(self):
		"""Make sure partial reading yields the correct results due to threading"""
		BLOCK = 131072
		STRING_A = "a" * BLOCK
		STRING_B = "b" * BLOCK
		with open(self.tmp_path + '/mount/file0', 'w') as f:
			f.write(STRING_A)
			f.write(STRING_B)

		f = os.open(self.tmp_path + '/mount/file0', os.O_RDONLY)
		self.assertTrue(os.read(f, BLOCK) == STRING_A)
		self.assertTrue(os.read(f, BLOCK) == STRING_B)
