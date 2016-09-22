The main point of hierarchicalFS.py is to store the information of each level in fuse, and key is the paths of each files and directories.

After I have built multi-level fuse, this step need me to make this multi-level fuse interacts with an xmlrpc HT, which is remoteHierarchicalFS.py. I store all the data and structure of fuse in server. And when I need to use them, I retrieve them from server to local and do further command.
