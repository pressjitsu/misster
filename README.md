Misster
=======

A caching FUSE filesystem for all types of slow mounts, like glusterfs, which still has no caching. Apart from caching, fancy logic like rule-based filters and hooks can be implemented pretty easily. Consider this a middleware filesystem.

Originally used with glusterfs (miss + gluster = misster).

Should be thread-safe.

Usage
-----

`python misster.py <mountpoint> -r <source> -c <cachedir>`

Add more background threading capacity by supplying the`-t` option.

Add `-o allow_other,default_permissions` if intending to use across users (recommended).

Testing
-------

`python -m unittest -v tests`

Licence
-------

GPLv3 (http://www.gnu.org/licenses/gpl-3.0.txt) Copyright 2015 (c) Pressjitsu
