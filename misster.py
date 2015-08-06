"""A glusterfs mount mirror with caching
capabilities and pretty logic.

miss + gluster = misster
"""

import sys, stat, time, logging, os, errno
import hashlib, shutil
from threading import Lock

import fuse
	
fuse.fuse_python_api = (0, 2)

# Logging
logger = logging.getLogger('misster')
handler = logging.FileHandler('/tmp/misster.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s (%(module)s.%(funcName)s:%(lineno)d %(threadName)s)')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

class SynchronizedCache:
	"""Provides threadsafe in-memory key-value storage."""

	def __init__(self):
		self.storage = {}
		self.lock = Lock()
	
	def get(self, key, default=None):
		"""Get an entry from tree_cache."""
		with self.lock:
			return self.storage.get(key, default)
	
	def set(self, key, value):
		"""Set cache entry."""
		with self.lock:
			self.storage[key] = value


class TreeCache(SynchronizedCache):
	"""Root filesystem attribute and structure caching.
	
	Metadata is stored in `fuse.Direntry` objects
	"""
	pass


class DescriptorCache(SynchronizedCache):
	"""Stores open files and descriptors."""
	pass

class Misster(fuse.Fuse):

	def getattr(self, path):
		logger.debug('getattr(%s)' % (path))

		entry = tree_cache.get(path)

		if not entry:
			return -errno.ENOENT

		s = fuse.Stat()
		s.st_mode  = entry.type | entry.stat.st_mode # (protection bits)
		s.st_size  = entry.stat.st_size  # (size of file, in bytes)
		s.st_ino   = entry.stat.st_ino   # (inode number)
		s.st_dev   = entry.stat.st_dev   # (device)
		s.st_nlink = entry.stat.st_nlink # (number of hard links)
		s.st_uid   = entry.stat.st_uid   # (user ID of owner)
		s.st_gid   = entry.stat.st_gid   # (group ID of owner) 
		s.st_atime = entry.stat.st_atime # (time of most recent access)
		s.st_mtime = entry.stat.st_mtime # (time of most recent content modification)
		s.st_ctime = entry.stat.st_ctime # (time of most recent metadata change)

		return s

	def open(self, path, flags):
		logger.debug('open(%s, %r)' % (path, flags))

		# Caching
		key = hashlib.sha1(path).hexdigest()
		cache_file = cache_path + key[:2] + '/' + key[2:]
		if not os.access(cache_file, os.F_OK | os.R_OK | os.W_OK):
			os.makedirs('/'.join(cache_file.split('/')[:-1]), mode=0700)
			logger.debug('copied %s to cache at %s' % (path, cache_file))
			shutil.copy(root + path, cache_file)
			os.chmod(cache_file, 0600)
		else:
			logger.debug('cache hit %s for %s' % (cache_file, path))

		descriptor_cache.set(path, os.fdopen(os.open(cache_file, flags)))
		return descriptor_cache.get(path)

	def release(self, path, flags, f):
		logger.debug('release(%s, %r, %r)' % (path, flags, f))
		f = descriptor_cache.get(path)
		f.close()

	def readdir(self, path, offset):
		logger.debug('readdir(%s)' % (path))

		entry = tree_cache.get(path)
		contents = entry.contents if entry else []
		directories = ['.', '..'] + contents
		for directory in directories:
			yield fuse.Direntry(directory)

	def read(self, path, length, offset, f):
		logger.debug('read(%s, %d, %d, %r)' % (path, length, offset, f))
		f = descriptor_cache.get(path)
		f.seek(offset)
		return f.read(length)


if __name__ == '__main__':
	logger.info('Starting misster with arguments: %s' % ' '.join(sys.argv[1:]))

	# Parse arguments
	m = Misster()
	m.parser.add_option('-c', dest='cache_path', help='local file cache directory')
	m.parser.add_option('-r', dest='rootpoint', help='source mount root')
	m.parse(errex=True)

	cache_path = m.cmdline[0].cache_path
	root = m.cmdline[0].rootpoint

	# Validate options and cleanup
	if not cache_path or not os.access(cache_path, os.F_OK | os.R_OK | os.W_OK | os.X_OK ):
		m.parser.error('Invalid cache path')
	cache_path = cache_path.rstrip('/') + '/'
	if not root or not os.access(root, os.F_OK | os.R_OK | os.W_OK | os.X_OK ):
		m.parser.error('Invalid source mount root')
	root = root.rstrip('/') + '/'

	print('Warming tree cache up. Might take a while...')

	descriptor_cache = DescriptorCache()

	# Warm up
	tree_cache = TreeCache()
	logger.info('Warming up cache for %s' % root)
	
	# Walk the tree getting the stats
	for path, dirs, files in os.walk(root):
		path = path.replace(root, '/', 1).rstrip('/') + '/' # Strip the root relation
		entry = fuse.Direntry(path.split('/')[-1])
		entry.type = stat.S_IFDIR
		if not entry.name:
			entry.name = '.'
		entry.contents = dirs + files
		entry.stat = os.stat(root + path)
		tree_cache.set(path.rstrip('/') or '/', entry)

		for f in files:
			entry = fuse.Direntry(f)
			entry.type = stat.S_IFREG
			entry.stat = os.stat(root + path + f)
			tree_cache.set(path + f, entry)

	logger.info('Cached %d tree elements' % len(tree_cache.storage))

	print('Mounting...')

	m.main()
