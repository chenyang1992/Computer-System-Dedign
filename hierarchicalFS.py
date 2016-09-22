#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class hierarchy:
    def __init__(self):
        self.file=[]
        self.directory=[]

class Memory(LoggingMixIn, Operations):
    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        self.trans=hierarchy()
        self.dict2={'/':self.trans}
        
    def chmod(self, path, mode):
        self.files[path]['st_mode'] &= 0770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

    def create(self, path, mode):
        if path.count('/')==1:
            newpath='/'
        else:			
	    index=path.rfind('/')
            newpath=path[:index]
        elements=path.split('/')
	if self.dict2.has_key(newpath):
	    self.dict2[newpath].file.append(elements[-1])
	else:
	    self.trans=hierarchy()
	    self.dict2[path]=self.trans
	    self.dict2[newpath].file.append(elements[-1])
	    self.files[newpath]['st_nlink'] += 1
        
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        if path not in self.files:
            raise FuseOSError(ENOENT)

        return self.files[path]

    def getxattr(self, path, name, position=0):
        attrs = self.files[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
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
        self.dict2[newpath].directory.append(elements[-1])
        self.trans=hierarchy()
        self.dict2[path]=self.trans
        self.files[newpath]['st_nlink'] += 1
        
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        dirlist=['.', '..']+self.dict2[path].directory+self.dict2[path].file
        return dirlist

    def readlink(self, path):
        return self.data[path]

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):   
        if old.count('/')==1:
            oldpath='/'
        else:			
	    index=old.rfind('/')
            oldpath=old[:index]	    
	oldname=old.split('/')
        newname=new.split('/')        
        if oldname[-1] in self.dict2[oldpath].file:
            self.dict2[oldpath].file.remove(oldname[-1])
            self.dict2[oldpath].file.append(newname[-1])
	    self.files[new]=self.files.pop(old)
        elif oldname[-1] in self.dict2[oldpath].directory:
            self.dict2[oldpath].directory.remove(oldname[-1])
            self.dict2[oldpath].directory.append(newname[-1])
	    l=len(new)
            for key in self.dict2.keys():
                if key.startswith(old):
                    newpath=new+key[l:]	
                    self.dict2[newpath]=self.dict2.pop(key)
		    self.files[newpath]=self.files.pop(key)				
				
    def rmdir(self, path):
        if path.count('/')==1:
            newpath='/'
        elif path[-1]=='/':
	    path=path[:-1]		
        else:			
	    index=path.rfind('/')
            newpath=path[:index]
        elements=path.split('/')
        try:
            elements[-1] in self.dict2[newpath].file
            #raise MyException
        except:
            print "Can only remove directory."
        try:
            self.dict2.has_key(path)
            #raise MyException
        except:
            print "Directory must be empty." 
            
        self.dict2[newpath].directory.remove(elements[-1])
        self.files.pop(path)
        self.files[newpath]['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):		
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                          st_size=len(source))
        self.data[target] = source 
	if target.count('/')==1:
	    newpath='/'
	else:
	    index=target.rfind('/')	    	    
	    newpath=target[:index]
	elements=target.split('/')	    
	self.dict2[newpath].file.append(elements[-1])
	self.trans=hierarchy()
	self.dict2[target]=self.trans		    
		
    def truncate(self, path, length, fh=None):
        self.data[path] = self.data[path][:length]
        self.files[path]['st_size'] = length

    def unlink(self, path):
	if path.count('/')==1:
            newpath='/'
        else:			
	    index=path.rfind('/')
            newpath=path[:index]
        elements=path.split('/')
	self.dict2[newpath].file.remove(elements[-1])		
        self.files.pop(path)
		
    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        self.data[path] = self.data[path][:offset] + data
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True, debug=True)