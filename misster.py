"""A glusterfs mount mirror with caching
capabilities and pretty logic.

miss + gluster = misster
"""

import fuse
import sys, stat, time, logging, os, errno, signal
import hashlib, shutil
from threading import Lock, Thread
from Queue import Queue
	
fuse.fuse_python_api = (0, 2)

# Logging
logger = logging.getLogger('misster')
handler = logging.FileHandler('/tmp/misster.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s (%(module)s.%(funcName)s:%(lineno)d %(threadName)s)')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

class MutableStatResult():
	"""Mirrors a os.stat_result object but with mutable properties."""
	def __init__(self, stat_result):
		for p in filter(lambda p: not p.startswith('_'), dir(stat_result)):
			setattr(self, p, getattr(stat_result, p))


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

	def readdir(self, path, offset):
		logger.debug('readdir(%s)' % (path))

		entry = tree_cache.get(path)
		contents = entry.contents if entry else []
		directories = ['.', '..'] + contents
		for directory in directories:
			yield fuse.Direntry(directory)

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
		logger.debug('open(%s, %d)' % (path, flags))

		# Caching
		cache_file = self.get_cache_file(path)
		if not os.access(cache_file, os.F_OK | os.R_OK | os.W_OK):
			os.makedirs('/'.join(cache_file.split('/')[:-1]), mode=0700)
			logger.debug('copied %s to cache at %s' % (path, cache_file))
			shutil.copy(root + path, cache_file)
			os.chmod(cache_file, 0600)
		else:
			logger.debug('cache hit %s for %s' % (cache_file, path))

		humanflags = ('r' if flags & os.O_RDONLY else '') + ('w' if flags & os.O_WRONLY else '') + ('a' if flags & os.O_APPEND else '')
		if not humanflags:
			humanflags = 'r'

		filehandle = os.fdopen(os.open(cache_file, flags), humanflags)

		descriptor_cache.set(path, filehandle)
		return filehandle

	def release(self, path, flags, f=None):
		logger.debug('release(%s, %r, %r)' % (path, flags, f))
		f = descriptor_cache.get(path)
		f.close()

		# Update changed cached attributes immediately
		entry = tree_cache.get(path)
		stat = MutableStatResult(os.stat(self.get_cache_file(path)))
		entry.stat.st_size = stat.st_size
		tree_cache.set(path, entry)

		logger.debug('updated st_size to %d for %s' % (tree_cache.get(path).stat.st_size, path))

		background.do('sync', path=path, cache_file=self.get_cache_file(path))

	def read(self, path, length, offset, filehandle):
		logger.debug('read(%s, %d, %d, %r)' % (path, length, offset, filehandle))
		f = descriptor_cache.get(path)
		f.seek(offset)
		return f.read(length)

	def write(self, path, data, offset, filehandle):
		logger.debug('write(%s, %d bytes, %d, %r)' % (path, len(data), offset, filehandle))
		f = descriptor_cache.get(path)
		f.seek(offset)
		f.write(data)

		# Update changed cached attributes immediately (unflushed)
		entry = tree_cache.get(path)
		stat = MutableStatResult(os.stat(self.get_cache_file(path)))
		entry.stat.st_size = stat.st_size
		tree_cache.set(path, entry)

		logger.debug('updated st_size to %d for %s' % (tree_cache.get(path).stat.st_size, path))

		return len(data)

	def mknod(self, path, mode, dev):
		logger.debug('mknod(%s, %r, %r)' % (path, mode, dev))
		node = os.mknod(root + path, mode, dev) # Source filesystem

		# Update attributes in the tree cache
		entry = fuse.Direntry(path.split('/')[-1])
		entry.type = stat.S_IFREG
		entry.stat = MutableStatResult(os.stat(root + path))
		tree_cache.set(path, entry)

		return node
	
	def truncate(self, path, offset):
		logger.debug('truncate(%s, %d)' % (path, offset))
		f = descriptor_cache.get(path)
		f.truncate(offset)

	def get_cache_file(self, path):
		key = hashlib.sha1(path).hexdigest()
		return cache_path + key[:2] + '/' + key[2:]


class BackgroundWorker:
	"""Does stuff"""

	tasks = None

	def __init__(self, threads=1):
		self.tasks = Queue()
		for t in range(threads):
			t = Thread(target=self.start)
			t.daemon = True
			t.start()

	def start(self):
		while True:
			task = self.tasks.get()	
			logger.debug('Doing %r' % task)

			try:
				getattr(self, 'task_' + task.get('task', None))(**task.get('args', {}))
				logger.debug('Done %r' % task)
			except AttributeError:
				logger.error('Unknown task %r' % task)
			finally:
				self.tasks.task_done()

	def do(self, task, **kwargs):
		self.tasks.put({'task': task, 'args': kwargs})

	def task_sync(self, path, cache_file):
		'''Copies content, stat info'''
		root_file = root + path
		logger.debug('Syncing %s to %s' % (cache_file, root_file))
		shutil.copy(cache_file, root_file)
		os.chmod(root_file, tree_cache.get(path).stat.st_mode)

if __name__ == '__main__':
	logger.info('Starting misster with arguments: %s' % ' '.join(sys.argv[1:]))

	# Parse arguments
	m = Misster()
	m.parser.add_option('-c', dest='cache_path', help='local file cache directory')
	m.parser.add_option('-r', dest='rootpoint', help='source mount root')
	m.parser.add_option('-t', dest='threads', help='number of background worker threads', default=1)
	m.parse(errex=True)

	cache_path = m.cmdline[0].cache_path
	root = m.cmdline[0].rootpoint
	threads = int(m.cmdline[0].threads, 10)

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
		entry.stat = MutableStatResult(os.stat(root + path))
		tree_cache.set(path.rstrip('/') or '/', entry)

		for f in files:
			entry = fuse.Direntry(f)
			entry.type = stat.S_IFREG
			entry.stat = MutableStatResult(os.stat(root + path + f))
			tree_cache.set(path + f, entry)

	logger.info('Cached %d tree elements' % len(tree_cache.storage))

	print('Starting %d background workers...' % threads)

	background = BackgroundWorker(threads)

	print('Mounting...')

	m.main()
