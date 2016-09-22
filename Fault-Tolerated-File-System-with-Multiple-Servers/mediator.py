#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.02

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value and ttl associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => {"value": Binary, "ttl": 1000}
      print rv["value"].data => "value"
  put(base64 key, base64 value, int ttl)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"), 1000)
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
import hashlib

# Presents a HT interface
class SimpleHT:
  def __init__(self,Qr,Qw,meta_port,data1_port,data2_port,data3_port,data4_port,data5_port):
    #self.data = {}
    self.next_check = datetime.now() + timedelta(minutes = 5)
    self.Qr=Qr
    self.Qw=Qw
    self.meta_port=meta_port
    self.data1_port=data1_port
    self.data2_port=data2_port
    self.data3_port=data3_port
    self.data4_port=data4_port
    self.data5_port=data5_port

  def count(self):
    # Remove expired entries
    self.next_check = datetime.now() - timedelta(minutes = 5)
    self.check()
    return len(self.data)

  def restart0(self):

    rpc0 = xmlrpclib.Server("http://127.0.0.1:"+self.meta_port,allow_none=True)
    rpc1 = xmlrpclib.Server("http://127.0.0.1:"+self.data1_port,allow_none=True)
    rpc2 = xmlrpclib.Server("http://127.0.0.1:"+self.data2_port,allow_none=True)
    rpc3 = xmlrpclib.Server("http://127.0.0.1:"+self.data3_port,allow_none=True)
    rpc4 = xmlrpclib.Server("http://127.0.0.1:"+self.data4_port,allow_none=True)
    rpc5 = xmlrpclib.Server("http://127.0.0.1:"+self.data5_port,allow_none=True)
    
    data1=rpc1.print_content()
    data2=rpc2.print_content()
    data3=rpc3.print_content()
    data4=rpc4.print_content()
    data5=rpc5.print_content()
   
    if data1 !={}:
    
      for key in data1.keys():
         self.get(Binary(key))
    if data2 !={}:
    
      for key in data2.keys():
         self.get(Binary(key))
    if data3 !={}:
     
      for key in data3.keys():
         self.get(Binary(key))
    if data4 !={}:
     
      for key in data4.keys():
         self.get(Binary(key))
    if data5 !={}:
  
      for key in data5.keys():
         self.get(Binary(key))
    return 1

  # Retrieve something from the HT
  def get(self, key):
    # Remove expired entries
    rpc0 = xmlrpclib.Server("http://127.0.0.1:"+self.meta_port,allow_none=True)
    rpc1 = xmlrpclib.Server("http://127.0.0.1:"+self.data1_port,allow_none=True)
    rpc2 = xmlrpclib.Server("http://127.0.0.1:"+self.data2_port,allow_none=True)
    rpc3 = xmlrpclib.Server("http://127.0.0.1:"+self.data3_port,allow_none=True)
    rpc4 = xmlrpclib.Server("http://127.0.0.1:"+self.data4_port,allow_none=True)
    rpc5 = xmlrpclib.Server("http://127.0.0.1:"+self.data5_port,allow_none=True)
    
    if key.data.split("&&")[-1] == 'meta' or key.data.split("&&")[-1] == 'list_nodes':     
        res0 = rpc0.get(Binary(key.data))
        return res0
       
    else:
       
        try:
          res1 = rpc1.get(Binary(key.data))
          data1 = pickle.loads(res1["value"].data)
        except:
          
          data1="blank1"
          
        try:
          res2 = rpc2.get(Binary(key.data))
          data2 = pickle.loads(res2["value"].data)
        except:
          
          data2="blank2"
       
        try:
          res3 = rpc3.get(Binary(key.data))
          data3 = pickle.loads(res3["value"].data)
        except:
          
          data3="blank3"
       
        try:
          res4 = rpc4.get(Binary(key.data))
          data4 = pickle.loads(res4["value"].data)
        except:
         
          data4="blank4"
       
        try:
          res5 = rpc5.get(Binary(key.data))
          data5 = pickle.loads(res5["value"].data)
        except:
          
          data5="blank5" 

        hashdata1 = hashlib.md5()
        hashdata2 = hashlib.md5()
        hashdata3 = hashlib.md5()
        hashdata4 = hashlib.md5()
        hashdata5 = hashlib.md5()
        hashdata1.update(data1)
        hashdata2.update(data2)
        hashdata3.update(data3)
        hashdata4.update(data4)
        hashdata5.update(data5)
        list0=[hashdata1.hexdigest(),hashdata2.hexdigest(),hashdata3.hexdigest(),hashdata4.hexdigest(),hashdata5.hexdigest()]
        dic={}
        for hashlist in list0:
          if hashlist not in dic.keys():
            dic[hashlist]=1
          else:
            dic[hashlist]=dic[hashlist]+1
        dic1={1:data1,2:data2,3:data3,4:data4,5:data5}
        newdic=sorted(dic.iteritems(), key=lambda d:d[1], reverse = True)
       # print newdic
        newvalue=dic1[list0.index(newdic[0][0])+1]   #correct data
        print "the number of agreed data:"
        print newdic[0][1]

        if newdic[0][1]>=int(self.Qr):
            if newdic[0][1]<int(self.Qw):
              try:
                rpc1.put(Binary(key.data), Binary(pickle.dumps(newvalue)), 6000)
                res5 = rpc1.get(Binary(key.data))
              except:
                pass
              try:
                rpc2.put(Binary(key.data), Binary(pickle.dumps(newvalue)), 6000)
                res5 = rpc2.get(Binary(key.data))
              except:
                pass
              try:
                rpc3.put(Binary(key.data), Binary(pickle.dumps(newvalue)), 6000)
                res5 = rpc3.get(Binary(key.data))
              except:
                pass
              try:
                rpc4.put(Binary(key.data), Binary(pickle.dumps(newvalue)), 6000)
                res5 = rpc4.get(Binary(key.data))
              except:
                pass
              try:
                rpc5.put(Binary(key.data), Binary(pickle.dumps(newvalue)), 6000)
                res5 = rpc5.get(Binary(key.data))
              except:
                pass
            return res5

        else:
            print "Request denied. Reason: data errors are not tolerated, system cannot recover or correct it."
            return "error"


  # Insert something into the HT
  def put(self, key, value, ttl):
    # Remove expired entries
    self.check()
    
    rpc0 = xmlrpclib.Server("http://127.0.0.1:"+self.meta_port,allow_none=True)
    rpc1 = xmlrpclib.Server("http://127.0.0.1:"+self.data1_port,allow_none=True)
    rpc2 = xmlrpclib.Server("http://127.0.0.1:"+self.data2_port,allow_none=True)
    rpc3 = xmlrpclib.Server("http://127.0.0.1:"+self.data3_port,allow_none=True)
    rpc4 = xmlrpclib.Server("http://127.0.0.1:"+self.data4_port,allow_none=True)
    rpc5 = xmlrpclib.Server("http://127.0.0.1:"+self.data5_port,allow_none=True)
    
    end = datetime.now() + timedelta(seconds = ttl)
    if key.data.split("&&")[-1] == 'meta' or key.data.split("&&")[-1] == 'list_nodes':
      rpc0.put(Binary(key.data), Binary(value.data), 6000)
      return True
    else:   
        rpc1.put(Binary(key.data), Binary(value.data), 6000)  
        rpc2.put(Binary(key.data), Binary(value.data), 6000)
        rpc3.put(Binary(key.data), Binary(value.data), 6000)
        rpc4.put(Binary(key.data), Binary(value.data), 6000)
        rpc5.put(Binary(key.data), Binary(value.data), 6000)
        
        return True   

  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

  # Remove expired entries
  def check(self):
    now = datetime.now()
    if self.next_check > now:
      return
    self.next_check = datetime.now() + timedelta(minutes = 5)
    to_remove = []
    for key, value in self.data.items():
      if value[1] < now:
        to_remove.append(key)
    for key in to_remove:
      del self.data[key]
       
def main():
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  ol={}
  for k,v in optlist:
    ol[k] = v
  Qr=sys.argv[1]
  Qw=sys.argv[2]
  meta_port=sys.argv[3]
  data1_port=sys.argv[4]
  data2_port=sys.argv[5]
  data3_port=sys.argv[6]
  data4_port=sys.argv[7]
  data5_port=sys.argv[8]
  port = 51234
  if "--port" in ol:
    port = int(ol["--port"])  
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  serve(port,Qr,Qw,meta_port,data1_port,data2_port,data3_port,data4_port,data5_port)

# Start the xmlrpc server
def serve(port,Qr,Qw,meta_port,data1_port,data2_port,data3_port,data4_port,data5_port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('',port))
  file_server.register_introspection_functions()
  sht = SimpleHT(Qr,Qw,meta_port,data1_port,data2_port,data3_port,data4_port,data5_port)
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.restart0)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(51234, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
  main()
