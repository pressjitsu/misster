"""A glusterfs mount mirror with caching
capabilities and pretty logic.

miss + gluster = misster
"""

import fuse
import sys, stat, time, logging, os, errno, signal
import hashlib, shutil, subprocess
from threading import Lock, Thread
from Queue import Queue
	
fuse.fuse_python_api = (0, 2)

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
		"""Get an entry from cache."""
		with self.lock:
			return self.storage.get(key, default)
	
	def set(self, key, value):
		"""Set cache entry."""
		with self.lock:
			self.storage[key] = value
	
	def remove(self, key):
		"""Remove a cache entry."""
		with self.lock:
			if self.storage.get(key, False):
				del self.storage[key]
	
	def storage(self):
		"""Unsynchronized storage dictionary."""
		return self.storage


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
			try:
				os.makedirs('/'.join(cache_file.split('/')[:-1]), mode=0700)
			except OSError as e: # Suppress existing directories
				if e.errno != errno.EEXIST:
					raise
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

		# Update changed cached attributes immediately
		entry = tree_cache.get(path)
		stat = MutableStatResult(os.stat(self.get_cache_file(path)))
		entry.stat.st_size = stat.st_size
		entry.stat.st_atime = time.time()
		tree_cache.set(path, entry)

		logger.debug('updated st_atime to %d for %s' % (tree_cache.get(path).stat.st_atime, path))

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
		entry.stat.st_mtime = time.time()
		tree_cache.set(path, entry)

		logger.debug('updated st_size to %d for %s' % (tree_cache.get(path).stat.st_size, path))
		logger.debug('updated st_mtime to %d for %s' % (tree_cache.get(path).stat.st_mtime, path))

		return len(data)

	def mknod(self, path, mode, dev):
		logger.debug('mknod(%s, %s, %r)' % (path, oct(mode), dev))

		# Create cache entry
		cache_file = self.get_cache_file(path)
		try:
			os.makedirs('/'.join(cache_file.split('/')[:-1]), mode=0700)
		except OSError as e: # Suppress existing directories
			if e.errno != errno.EEXIST:
				raise
		node = os.mknod(cache_file, 0600)

		# Parent
		parent = tree_cache.get(os.path.dirname(path))

		# Update attributes in the tree cache
		entry = fuse.Direntry(os.path.basename(path))
		entry.type = stat.S_IFREG
		entry.stat = MutableStatResult(os.stat(cache_file))
		entry.stat.st_mode = mode # Overwrite mode and ownership
		entry.stat.st_uid = self.GetContext().get('uid', entry.stat.st_uid)
		entry.stat.st_gid = self.GetContext().get('gid', entry.stat.st_gid)
		tree_cache.set(path, entry)

		# Update parent tree contents
		parent.contents.append(entry.name)

		# Sync to backend in background
		background.do('sync', path=path, cache_file=cache_file)

		return node
	
	def truncate(self, path, offset):
		logger.debug('truncate(%s, %d)' % (path, offset))
		f = descriptor_cache.get(path)
		f.truncate(offset)

	def unlink(self, path):
		logger.debug('unlink(%s)' % (path,))
		
		# Update tree cache and parent
		tree_cache.remove(path)
		tree_cache.get(os.path.dirname(path)).contents.remove(os.path.basename(path))

		# Remove cache file
		os.remove(self.get_cache_file(path))

		# Sync with backend
		background.do('sync', path=path, cache_file=self.get_cache_file(path))

	def mkdir(self, path, mode):
		logger.debug('mkdir(%s, %s)' % (path, oct(mode)))

		# Update attributes in the tree cache
		entry = fuse.Direntry(os.path.basename(path))
		entry.type = stat.S_IFDIR
		entry.contents = []
		entry.stat = MutableStatResult(os.stat(os.path.dirname(mountpoint + path))) # Inherit parent but change mode and time
		entry.stat.st_mode = mode # Overwrite mode and ownership
		entry.stat.st_uid = self.GetContext().get('uid', entry.stat.st_uid)
		entry.stat.st_gid = self.GetContext().get('gid', entry.stat.st_gid)
		entry.stat.st_atime = entry.stat.st_mtime = entry.stat.st_ctime = time.time()

		tree_cache.set(path, entry)

		# Update parent tree
		parent = tree_cache.get(os.path.dirname(path))
		parent.contents.append(entry.name)

		# Offload backend creation to background
		background.do('syncdir', path=path)

	def rmdir(self, path):
		logger.debug('rmdir(%s)' % (path))

		if tree_cache.get(path).contents:
			return -errno.ENOTEMPTY # Directory is not empty

		# Update tree cache and parent
		tree_cache.remove(path)
		tree_cache.get(os.path.dirname(path)).contents.remove(os.path.basename(path))

		# Sync with backend
		background.do('syncdir', path=path, remove=True)

	def chmod(self, path, mode):
		logger.debug('chmod(%s, %s)' % (path, oct(mode)))

		# Update tree
		tree_cache.get(path).stat.st_mode = mode

		# Sync with backend
		background.do('syncmod', path=path)

	@classmethod
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
		"""Copies content, stat info"""
		root_file = root + path
		logger.debug('Syncing %s to %s' % (cache_file, root_file))

		if not os.path.exists(cache_file):
			os.remove(root_file)
		else:
			shutil.copy2(cache_file, root_file)
			os.chmod(root_file, tree_cache.get(path).stat.st_mode & 0777)

	def task_syncdir(self, path, remove=False):
		logger.debug('Syncing %s' % (path,))

		root_dir = root + path

		if remove:
			os.rmdir(root_dir)
		else:
			os.mkdir(root_dir, os.stat(mountpoint + path).st_mode & 0777)

	def task_syncmod(self, path):
		logger.debug('Syncing %s' % (path,))

		os.chmod(root + path, os.stat(mountpoint + path).st_mode & 0777)


class CacheClearer(Thread):
	"""Makes sure cache diskspace usage has not overflown"""
	daemon = True

	def run(self):
		while True:
			# Get diskspace usage
			du = int(subprocess.check_output(['du', '-bs', cache_path], shell=False).split()[0])
			logger.debug('Cache directory size %d/%d bytes' % (du, cache_limit))
			if cache_limit < du:
				down = int(cache_limit * 0.8)
				logger.debug('...cleaning down to %d bytes' % down)

				removed = 0
				# Sort through the items to find least recently accessed or modified
				for path in filter(lambda s: tree_cache.get(s).type == stat.S_IFREG, sorted(tree_cache.storage, key=lambda s: max(tree_cache.get(s).stat.st_mtime, tree_cache.get(s).stat.st_atime))):
					cache_file = Misster.get_cache_file(path)
					if not os.path.exists(cache_file):
						continue
					os.remove(cache_file)
					removed = removed + tree_cache.get(path).stat.st_size
					if removed > (du - down):
						break
				logger.debug('Removed %d bytes from cache' % removed)
			time.sleep(60) # ...every 60 seconds
	

if __name__ == '__main__':
	# Parse arguments
	m = Misster()
	m.parser.add_option('-c', dest='cache_path', help='local file cache directory')
	m.parser.add_option('-r', dest='rootpoint', help='source mount root')
	m.parser.add_option('-t', dest='threads', help='number of background worker threads', default='1')
	m.parser.add_option('-l', dest='log', help='log file', default='/tmp/misster.log')
	m.parser.add_option('-m', dest='limit', help='cache size limit in bytes', default='1000000000')
	m.parse(errex=True)

	cache_path = m.cmdline[0].cache_path
	root = m.cmdline[0].rootpoint
	threads = int(m.cmdline[0].threads, 10)
	log_file = m.cmdline[0].log
	cache_limit = int(m.cmdline[0].limit, 10)
	mountpoint = m.fuse_args.mountpoint

	# Validate options and cleanup
	if not cache_path or not os.access(cache_path, os.F_OK | os.R_OK | os.W_OK | os.X_OK ):
		m.parser.error('Invalid cache path')
	cache_path = cache_path.rstrip('/') + '/'
	if not root or not os.access(root, os.F_OK | os.R_OK | os.W_OK | os.X_OK ):
		m.parser.error('Invalid source mount root')
	if not os.access(log_file, os.F_OK | os.R_OK | os.W_OK ):
		m.parser.error('Invalid log file')
	if cache_limit < 0:
		m.parser.error('Cache size limit should be 0 or more bytes')
	root = root.rstrip('/') + '/'

	# Logging
	logger = logging.getLogger('misster')
	handler = logging.FileHandler(log_file)
	formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s (%(module)s.%(funcName)s:%(lineno)d %(threadName)s)')
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	logger.setLevel(logging.DEBUG)

	logger.info('Starting misster with arguments: %s' % ' '.join(sys.argv[1:]))

	print('Warming tree cache up. Might take a while...')

	descriptor_cache = DescriptorCache()

	# Warm up
	tree_cache = TreeCache()
	logger.info('Warming up cache for %s' % root)
	
	# Walk the tree getting the stats
	for path, dirs, files in os.walk(root):
		path = path.replace(root, '/', 1).rstrip('/') + '/' # Strip the root relation
		entry = fuse.Direntry(os.path.basename(path))
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
	CacheClearer().start()

	print('Mounting...')

	h = signal.signal(signal.SIGINT, signal.SIG_DFL)
	m.main() # Allow clean Ctrl+C
	signal.signal(signal.SIGINT, h)
