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
from pymongo import MongoClient
from datetime import datetime 
import copy
import timeit
import memcache

mc = memcache.Client(["127.0.0.1:11211"])
client = MongoClient()
db = client.yd

useCache=True
#useCache=False
#mc.delete("items")
#db.dy.delete_many({})
def putIntoCache(tempPath,Record,capacity):
    
    
    if mc.get("items")==None:   
        establishtime= datetime.now()
       
        mc.set("items",{tempPath:{"cachedata":Record[tempPath],"establishtime":establishtime}})

    elif len(mc.get("items"))<(capacity): 

        establishtime= datetime.now()
    
        result=mc.get("items")
        result[tempPath]={"cachedata":Record[tempPath],"establishtime":establishtime}
        mc.set("items",result)

    elif len(mc.get("items"))==(capacity):
 
        resultlist=[]
        for path in mc.get("items").keys():
            result=mc.get("items")[path]["establishtime"]
            resultlist.append(result)

        result1=min(resultlist)
        for path in mc.get("items").keys():
            if result1==mc.get("items")[path]["establishtime"]:

                result2=mc.get("items")
                del result2[path]
                mc.set("items",result2)

        establishtime= datetime.now()
       
        result=mc.get("items")
        result[tempPath]={"cachedata":Record[tempPath],"establishtime":establishtime}
        mc.set("items",result)
        
        print mc.get("items").keys()      

def getFromCache(path):

    if mc.get("items")[path]!=None:
        return mc.get("items")[path]["cachedata"].encode()

def put(key,value):
 
    tempvalue = copy.deepcopy(value)
    
    
    if key=="dfclass":

        db.dy.insert_one(
        {
            "key":key,
            "value":{
                "file": [],
                "directory": []
            }
        }
        ) 
    else:
        for key2 in tempvalue.keys():
            path = key2.replace(".","*")    
            values = tempvalue[key2]
            if key2 == path:
                pass
            else:
                tempvalue.pop(key2)
                tempvalue[path]=values

        if db.dy.find_one({'key':key}) is None:
           
            db.dy.insert_one(
            {
                "key": key,
                "value": tempvalue
            }
            )
        else:
            db.dy.update_one(
                 {"key": key},
                 {
                     "$set": {
                         "value" : tempvalue
                     },
                    "$currentDate": {"lastModified": True}
                 }
                ) 

def get(key):
    
    value=db.dy.find_one({'key':key})['value']
    if key == "dfclass":
        pass
    else:
        for key1 in value.keys():

            path = key1.replace("*",".")    
            values = value[key1]
            if key1 == path:
                pass
            else:
                value.pop(key1)
                value[path]=values
    return value
 
if not hasattr(__builtins__, 'bytes'):
    bytes = str
    
class df:
    def __init__(self):
        self.file=[]
        self.directory=[]

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'
    
    def __init__(self):
        
        if db.dy.find_one({'key':'dic'}) is None:
        #self.files = {}
        #self.data = defaultdict(bytes)
        #self.fd = 0
            mc.set("fd",0)
        #b={}
        #b['count']=0
        #put('fd',b)
            now = time()
        #self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
        #                       st_mtime=now, st_atime=now, st_nlink=2)

            put('files',{}) #chart 1
            a={} #defaultdict(bytes)
            put('data',a) #chart 2

            fileschart=get('files')
            fileschart['/']=dict(st_mode=(S_IFDIR | 0755), st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2)
            put('files',fileschart)  # initiate files

            put('dfclass',df()) #chart 3
        
            put('dic',{}) #chart 4
            dicchart=get('dic')
            a=get('dfclass')
            dicchart['/']=a
            put('dic',dicchart)
        else:
            pass
            #mc.get('fd')
            #get('dfclass')
            #get('dic')
            #get('files')
            #get('data')
      

    def chmod(self, path, mode):
        #self.files[path]['st_mode'] &= 0770000
        #self.files[path]['st_mode'] |= mode

        get1=get('files')
        get1[path]['st_mode'] &= 0770000
        put('files',get1)
        get2=get('files')
        get2[path]['st_mode'] |= mode
        put('files',get2)
        return 0

    def chown(self, path, uid, gid):
        #self.files[path]['st_uid'] = uid
        #self.files[path]['st_gid'] = gid
        get1=get('files')
        get1[path]['st_uid'] = uid
        put('files',get1)
        get2=get('files')
        get2[path]['st_gid'] = gid
        put('files',get2)

    def create(self, path, mode):
        x=path.split('/') 
        if len(x)>2:
            newpath=''    
            for s in x[1:-1]:
                newpath=newpath+'/'+s 
            #self.dic[newpath].file.append(x[-1])
            #self.e=df()
            #self.dic[path]=self.e
            #self.files[newpath]['st_nlink'] += 1
            get1=get('dic')
            get1[newpath]["file"].append(x[-1])
            put('dic',get1)
             
            e=get('dfclass')
            dicchart=get('dic')
            dicchart[path]=e
            put('dic',dicchart)
            get2=get('files')
            get2[newpath]['st_nlink'] += 1
            put('files',get2)

        elif len(x)==2:
            newpath='/'
            #self.dic['/'].file.append(x[-1])
            #self.b=df()
            #self.dic[path]=self.b
            #self.files['/']['st_nlink'] += 1
        
            get1=get('dic')
            get1[newpath]["file"].append(x[-1])
            put('dic',get1)
            b=get('dfclass')
            dicchart=get('dic')
            dicchart[path]=b
            put('dic',dicchart)
            get2=get('files')
            get2[newpath]['st_nlink'] += 1
            put('files',get2)

        #self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
        #                        st_size=0, st_ctime=time(), st_mtime=time(),
        #                        st_atime=time())
       
        fileschart=get('files')
        fileschart[path]=dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        put('files',fileschart) 
        #self.fd += 1
        result=mc.get("fd")
        result+=1
        mc.set("fd",result)
        return result
        #a=get('fd')
        #a['count'] += 1
        #put('fd',a)
        #return self.fd
        #return a['count']
        

    def getattr(self, path, fh=None):
        
        #if path not in self.files:
        #    raise FuseOSError(ENOENT)

        #return self.files[path]
        if path not in get('files'):
            raise FuseOSError(ENOENT)
        return get('files')[path]

    def getxattr(self, path, name, position=0):
        #attrs = self.files[path].get('attrs', {})
        get1=get('files')
        attrs=get1[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        #attrs = self.files[path].get('attrs', {})
        get1=get('files').keys()
        attrs=get1[path].get('attrs', {}) 
        return attrs.keys()

    def mkdir(self, path, mode):
        x=path.split('/') 
        if len(x)>2:
            newpath=''
            for s in x[1:-1]:
                newpath=newpath+'/'+s 
            #self.dic[newpath].directory.append(x[-1])
            #self.b=df()
            #self.dic[path]=mc.set("some_key", "Some value")self.b
            #self.files[newpath]['st_nlink'] += 1
            get1=get('dic')
            get1[newpath]["directory"].append(x[-1])
            put('dic',get1)
            
            b=get('dfclass')
            dicchart=get('dic')
            dicchart[path]=b
            put('dic',dicchart)

            get2=get('files')
            get2[newpath]['st_nlink'] += 1
            put('files',get2)

        elif len(x)==2:
            newpath='/'
            #self.dic['/'].directory.append(x[-1])
            #self.c=df()
            #self.dic[path]=self.c
            #self.files['/']['st_nlink'] += 1
            get1=get('dic')
            get1[newpath]["directory"].append(x[-1])
            put('dic',get1)
            
            b=get('dfclass')
            dicchart=get('dic')
            dicchart[path]=b
            put('dic',dicchart)
            get2=get('files')
            get2[newpath]['st_nlink'] += 1
            put('files',get2)
            
        #self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
        #                        st_size=0, st_ctime=time(), st_mtime=time(),
        #                        st_atime=time())
        fileschart=get('files')
        fileschart[path]=dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        put('files',fileschart) 

    def open(self, path, flags):
        #self.fd += 1
        #return self.fd
        #a=get('fd')
        #a['count'] += 1
        #put('fd',a)
        #return a['count']
        result=mc.get("fd")
        result+=1
        mc.set("fd",result)
        return result
        
    def read(self, path, size, offset, fh):
        get1 = get('data')
        if path not in get1.keys():
            get1[path]=defaultdict(bytes)
            put('data',get1)
        if useCache==True:
            start=datetime.now()
            if mc.get("items").has_key(path):
                
                for i in range(10000):
                    getFromCache(path)[offset:offset + size]
                print "**********************"
                print path+" file is in cache and time for getting this filedata from cache directly (running 10000 times) "
                print datetime.now()-start
                tempdx=datetime.now()-start
                # Open a file
                fo = open("result.txt", "wb")
                fo.write("cache hitted!"+"  Time(10000 instructions):"+str(tempdx));
                # Close opend file
                fo.close()
                print "**********************"
                Record=defaultdict(bytes)
                Record[path.encode('utf-8')]=getFromCache(path)[offset:offset + size]
                putIntoCache(path.encode('utf-8'),Record,5)              
                return getFromCache(path)[offset:offset + size]   

            else:
                #start=datetime.now()
                for i in range(10000):
                    get('data')[path][offset:offset + size].encode()
                print "**********************"
                print path+" file is not in cache and time for getting this filedata from MongoDB (running 10000 times) "
                print datetime.now()-start
                tempdx=datetime.now()-start
                # Open a file
                fo = open("result.txt", "wb")
                fo.write("cache not hitted! Read from mongoDB"+"  Time(10000 instructions):"+str(tempdx));

                # Close opend file
                fo.close()
                print "**********************"
                Record=defaultdict(bytes)
                Record[path.encode('utf-8')]=get('data')[path][offset:offset + size]
                putIntoCache(path.encode('utf-8'),Record,5)
                return get('data')[path][offset:offset + size].encode()
        else:
            start=datetime.now()
            for i in range(10000):
                get('data')[path][offset:offset + size].encode()
            print "**********************"
            print path+" file is fetched from mongodb directly and time for getting this filedata from MongoDB (running 10000 times) "
            print datetime.now()-start
            print "**********************"
            return get('data')[path][offset:offset + size].encode()
        #return get('data')[path][offset:offset + size].encode()
        #return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        print "mongoDB+++++++++++++++++++++++++"
        cursor = db.dy.find()
        for document in cursor:
            print(document)
        print "cache+++++++++++++++++++++++++"
        print mc.get("items")
     #dirlist=['.', '..']+self.dic[path].directory+self.dic[path].file
        
        dirlist=['.', '..']+get('dic')[path]["directory"]+get('dic')[path]["file"]
        return dirlist

    def readlink(self, path):
        #return self.data[path]
        return get('data')[path]

    def removexattr(self, path, name):
        #attrs = self.files[path].get('attrs', {})
        attrs=get('files')[path].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
         
        x=old.split('/') 
        if len(x)>2:
            newpath=''
            for s in x[1:-1]:
                newpath=newpath+'/'+s 
        elif len(x)==2:
            newpath='/'

        y=new.split('/')
        get1=get('dic')
        #if x[-1] in self.dic[newpath].directory:
        if x[-1] in get1[newpath]["directory"]:
            #self.dic[newpath].directory.remove(x[-1])
            #self.dic[newpath].directory.append(y[-1])
            
            get1[newpath]["directory"].remove(x[-1])
            get1[newpath]["directory"].append(y[-1])
            
            put('dic',get1)            

        elif x[-1] in get1[newpath]["file"]:
            #self.dic[newpath].file.remove(x[-1])
            #self.dic[newpath].file.append(y[-1])
         
            get1[newpath]["file"].remove(x[-1])
            get1[newpath]["file"].append(y[-1])

            put('dic',get1)

        
        for key in get1.keys():     
            q=key.split('/') 
            if len(q)>=len(x):
                if q[0:len(x)]==x[:]:             
                    q[len(x)-1]=y[-1]
                    newkey='/'.join(q)
                    #self.dic[newkey]=self.dic.pop(key)
                    #self.files[newkey]=self.files.pop(key)
                    get3=get('dic')
                    get3[newkey]=get3.pop(key)
                    put('dic',get3)
                   
                    get4=get('files')
                    get4[newkey]=get4.pop(key)
                    put('files',get4)
                   
        #self.files[new] = self.files.pop(old)
    def rmdir(self, path):
        x=path.split('/') 
        get1=get('dic') 
        if len(x)>2:
            newpath=''
            for s in x[1:-1]:
                newpath=newpath+'/'+s    
            #if x[-1] in self.dic[newpath].directory:
            if x[-1] in get1[newpath]["directory"]:
                get1[newpath]["directory"].remove(x[-1])
                put('dic',get1)
                #self.dic[newpath].directory.remove(x[-1])
            #elif x[-1] in self.dic[newpath].file:
            elif x[-1] in get1[newpath]["file"]:
                get1[newpath]["file"].remove(x[-1])
                put('dic',get1)
                #self.dic[newpath].file.remove(x[-1])
        elif len(x)==2:
            newpath='/'
            #if x[-1] in self.dic['/'].directory:
            if x[-1] in get1['/']["directory"]: 
                #self.dic['/'].directory.remove(x[-1])
                get1['/']["directory"].remove(x[-1])
                put('dic',get1)
            #elif x[-1] in self.dic['/'].file:
            elif x[-1] in get1['/']["file"]:
                #self.dic['/'].file.remove(x[-1])
                get1['/']["file"].remove(x[-1])
                put('dic',get1)
        #self.files.pop(path)
        #self.files[newpath]['st_nlink'] -= 1
        get2=get('files')
        get2.pop(path)
        get2[newpath]['st_nlink'] -= 1
        put('files',get2)

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        get1=get('files')
        attrs=get1[path].setdefault('attrs', {})
        #attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
          
        #self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
        #                          st_size=len(source))
        get1=get('files')
        get1[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))
        put('files',get1) 
        #self.data[target] = source
        get2=get('data')
        get2[target] = source
        put('data',get2)
  
        get3=get('dic')
        x=target.split('/') 
        if x[-1]=='':
            x.remove(x[-1])
        if len(x)>2:
            newpath=''    
            for s in x[1:-1]:
                newpath=newpath+'/'+s
            get3[newpath]["file"].append(x[-1])
            put('dic',get3) 
  
            i=get('dfclass')
            get1=get('dic')
            get1[target]=i
            put('dic',get1)
       
            #self.i=df()
            #self.dic[target]=self.i
   
        elif len(x)==2:
            newpath='/'
            get3[newpath]["file"].append(x[-1])
            put('dic',get3)
  
            j=get('dfclass')
            get2=get('dic')
            get2[target]=j
            put('dic',get2)
  
            #self.j=df()
            #self.dic[target]=self.j
                
    def truncate(self, path, length, fh=None):
        get1=get('data')
        if path not in get1.keys():
            get1[path]="".encode()
        get1[path]=get1[path][:length]
        put('data',get1)
        get2=get('files')
        get2[path]['st_size'] = length
        put('files',get2)
        #self.data[path] = self.data[path][:length]
        #self.files[path]['st_size'] = length

    def unlink(self, path):
        #self.files.pop(path)
        get1=get('files')
        get1.pop(path)
        put('files',get1)
        get2=get('dic')
        x=path.split('/')
 
        if len(x)>2:
            newpath=''
            for s in x[1:-1]:
                newpath=newpath+'/'+s          
            #self.dic[newpath].file.remove(x[-1])
            get2[newpath]["file"].remove(x[-1])
            put('dic',get2)
        elif len(x)==2:
            newpath='/'   
            #self.dic['/'].file.remove(x[-1])
            get2[newpath]["file"].remove(x[-1])
            put('dic',get2)
    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        #self.files[path]['st_atime'] = atime
        #self.files[path]['st_mtime'] = mtime
        get1=get('files')
        get1[path]['st_atime'] = atime
        put('files',get1)
        get2=get('files')
        get2[path]['st_mtime'] = mtime
        put('files',get2)

    def write(self, path, data, offset, fh):
        #self.data[path] = self.data[path][:offset] + data
        #self.files[path]['st_size'] = len(self.data[path])
        get1=get('data')
        if path not in get1.keys():
            get1[path]="".encode()
        get1[path]=get1[path][:offset] + data
        if useCache==True:
            Record=defaultdict(bytes)
            Record[path.encode('utf-8')]=get1[path][:offset] + data
            putIntoCache(path.encode('utf-8'),Record,5)
        put('data',get1)
        get2=get('files')
        get3=get('data')
        get2[path]['st_size']=len(get3[path])
        put('files',get2)
        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True, debug=True)
