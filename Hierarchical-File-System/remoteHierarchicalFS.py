#!/usr/bin/env python
import logging
import pickle, xmlrpclib
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from xmlrpclib import Binary
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

def put(key,value):
    a = pickle.dumps(value)  
    rpc.put(Binary(key),Binary(a),3000)   # put local_dict to server_ht

def retrieve(key):
    rv = rpc.get(Binary(key))
    rv = pickle.loads(rv["value"].data)
    return rv
    
class hierarchy:
    def __init__(self):
        self.file=[]
        self.directory=[]

class Memory(LoggingMixIn, Operations):
    def __init__(self):
        put('data',defaultdict(bytes))      #initiate hashtable data
        put('files',{})			    #initiate hashtable files
	put('dict2',{})			    #initiate hashtable dict2
	put('hierarchy_ht',hierarchy())     #initiate hashtable hierarchy_ht
        self.fd = 0
	now = time()
	a = retrieve('files')
	a['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now, st_mtime=now, st_atime=now,st_nlink=2)
	put('files', a)
	b = retrieve('hierarchy_ht')
	c = retrieve('dict2')
	c['/'] = b
	put('dict2', c)
        
    def chmod(self, path, mode):
        a = retrieve('files')
        a[path]['st_mode'] &= 0770000
	put('files',a)
	b = retrieve('files')
        b[path]['st_mode'] |= mode
        put('files',b)
	return 0

    def chown(self, path, uid, gid):
        a = retrieve('files')
        a[path]['st_uid'] = uid
	put('files',a)
        b = retrieve('files')
        b[path]['st_gid'] = gid
        put('files',b)
 
    def create(self, path, mode):
	a=retrieve('dict2')
	b=retrieve('files')
	c=retrieve('hierarchy_ht')
        if path.count('/')==1:
            newpath='/'
        else:			
	    index=path.rfind('/')
            newpath=path[:index]
        elements=path.split('/')
	if a.has_key(newpath):
	    a[newpath].file.append(elements[-1])
	else:
	    a[path]=c
	    a[newpath].file.append(elements[-1])
	    b[newpath]['st_nlink'] += 1
        put('dict2',a)
        b[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
	put('files',b)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        a = retrieve('files')
        if path not in a:
	    raise FuseOSError(ENOENT)
        return a[path]

    def getxattr(self, path, name, position=0):
        a=retrieve('files')
        a[path].retrieve('attrs', {})
        attrs=put('files',a) 
	try:
	    return attrs[name]
	except KeyError:
	    return '' # Should return ENOATTR

    def listxattr(self, path):
	a=retrieve('files')
        a[path].retrieve('attrs', {})
        attrs=put('files',a) 
	return attrs.keys()

    def mkdir(self, path, mode):
        if path.count('/')==1:
            newpath='/'
	elif path[-1]=='/':
	    path=path[:-1]	
        else:			
	    index=path.rfind('/')
	    newpath=path[:index]
	    
        elements=path.split('/')
	a = retrieve('dict2')
        a[newpath].directory.append(elements[-1])
	put('dict2',a)
	
        b=retrieve('hierarchy_ht')
	d=retrieve('dict2')
        d[path]=b
	put('dict2',d)
        c=retrieve('files')
	c[newpath]['st_nlink'] += 1
	put('files',c)
	e=retrieve('files')
	e[path]=dict(st_mode=(S_IFDIR | mode), st_nlink=2,
	                      st_size=0, st_ctime=time(), st_mtime=time(),
	                      st_atime=time())
	put('files',e)                
        
    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
	a = retrieve('data')
        return a[path][offset:offset + size]

    def readdir(self, path, fh):
	dirlist=['.', '..']+retrieve('dict2')[path].directory+retrieve('dict2')[path].file
        return dirlist

    def readlink(self, path):
	a = retrieve('data')
	return a[path]

    def removexattr(self, path, name):
	attrs=retrieve('files')[path].retrieve('attrs', {})
	try:
	    del attrs[name]
	except KeyError:
	    pass        # Should return ENOATTR

    def rename(self, old, new):
	a=retrieve('dict2')
	b=retrieve('files')
        if old.count('/')==1:
            oldpath='/'
        else:			
	    index=old.rfind('/')
            oldpath=old[:index]
	oldname=old.split('/')
        newname=new.split('/')
        l=len(new)	
        if oldname[-1] in a[oldpath].file:
            a[oldpath].file.remove(oldname[-1])
            a[oldpath].file.append(newname[-1])
	    b[new]=b.pop(old)
        elif oldname[-1] in a[oldpath].directory:
            a[oldpath].directory.remove(oldname[-1])
            a[oldpath].directory.append(newname[-1])
	for key in a.keys():
            if key.startswith(old):
                newpath=new+key[l:]
                b[newpath]=b.pop(key)
                a[newpath]=a.pop(key)
	put('files',b)
	put('dict2',a)
				
    def rmdir(self, path):
	if path.count('/')==1:
	    newpath='/'
	elif path[-1]=='/':
	    path=path[:-1]		
	else:			
	    index=path.rfind('/')
	    newpath=path[:index]
	elements=path.split('/')
	a = retrieve('dict2')
	try:
	    elements[-1] in a[newpath].file      #raise MyException
	except:
	    print "Can only remove directory."
	try:
	    a.has_key(path)                      #raise MyException
	except:
	    print "Directory must be empty." 
	a[newpath].directory.remove(elements[-1])
	put('dict2',a)
	b = retrieve('files')
        b.pop(path)
        b[newpath]['st_nlink'] -= 1
	put('files',b)

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        a = retrieve('files')
        attrs = a[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
	a=retrieve('files')
	b=retrieve('data')
	c=retrieve('dict2')
	d=retrieve('hierarchy_ht')
	
	a[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
	                          st_size=len(source))
	b[target] = source
	put('files',a)
	put('data',b)
	elements=target.split('/')	
	if target.count('/')==1:
	    newpath='/'
	else:
	    index=target.rfind('/')	    	    
	    newpath=target[:index]	
	c[newpath].file.append(elements[-1])
	c[target]=d
	put('dict2',c)
	
    def truncate(self, path, length, fh=None):
        a = retrieve('data')
        a[path] = a[path][:length]
        put('data',a)
        b = retrieve('files')
        b[path]['st_size'] = length
        put('files',b)

    def unlink(self, path):
	if path.count('/')==1:
            newpath='/'
        else:			
	    index=path.rfind('/')
            newpath=path[:index]
        elements=path.split('/')
	a=retrieve('dict2')
	a[newpath].file.remove(elements[-1])
	put('dict2',a)
	b=retrieve('files')
        b.pop(path)
	put('files',b)
		
    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        a = retrieve('files')
        a[path]['st_atime'] = atime
        a[path]['st_mtime'] = mtime
        put('files',a)

    def write(self, path, data, offset, fh):
        a = retrieve('data')
        b = retrieve('files')
        if path not in a:
	    a[path] = bytes()
        a[path] = a[path][:offset] + data
        b[path]['st_size'] = len(a[path])
        put('data',a)
        put('files',b)
        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)
    rpc = xmlrpclib.ServerProxy('http://localhost:51234/')
    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True, debug=True)