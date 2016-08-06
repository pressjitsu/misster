Misster
=======

A caching FUSE filesystem for all types of slow mounts, like glusterfs, which still has no caching. Apart from caching, fancy logic like rule-based filters and hooks can be implemented pretty easily. Consider this a middleware filesystem.

Originally used with glusterfs (miss + gluster = misster).

Should be thread-safe.

*WARNING*: this is a development version, please backup your files. We do not guarantee data integrity, safety, or anything else for that matter. Seriously.

*CAUTION*: on 64-bit systems with Python compiled without LFS support this utility will break, since inodes will overflow. Launch gluster without LFS support in such cases using the `enable-ino32` flag.

Usage
-----

`python misster.py <mountpoint> -r <source> -c <cachedir>`

Cache directory size limit `-m BYTES`, default is 1G.

Add more background threading capacity by supplying the`-t` option (highly experimental, will probably mess up ordering of operations on the same file due to race conditions).

Add `-o allow_other,default_permissions` if intending to use across users (recommended).

Testing
-------

`python -m unittest -v tests`

Notes
-----

The source layer cannot be used concurrently by several entities. The caching layer will become unsynchronised if changes are done outside of misster's target mount.

Licence
-------

GPLv3 (http://www.gnu.org/licenses/gpl-3.0.txt) Copyright 2015 (c) Pressjitsu
