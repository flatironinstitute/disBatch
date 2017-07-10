
import kvsstcp, os

class DisBatcher(object):
    def __init__(self, tasksname, kvsserver=None):
        if kvsserver:
            self.kvs = kvsstcp.KVSClient(kvsserver)
        else:
            self.kvs = kvsstcp.KVSClient(os.environ['KVSSTCP_HOST'], os.environ['KVSSTCP_PORT'])
        self.kvs.put('task source name', tasksname, False)
        self.taskkey = tasksname + ' task'
        self.donetask = tasksname + ' done!'
        self.resultkey = tasksname + ' result %d'
        self.taskCount = 0
        self.tx2rc = {}

    def done(self):
        self.kvs.put(self.taskkey, self.donetask, False)

    def submit(self, c):
        self.kvs.put(self.taskkey, c, False)
        tc = self.taskCount
        self.taskCount += 1
        return tc

    def syncTasks(self, taskd):
        tx2rc = {}
        for tx in taskd:
            if tx not in self.tx2rc:
                r = self.kvs.get(self.resultkey%tx, False)
                lx, rc = r.split('\t')
                self.tx2rc[tx] = int(rc)
            tx2rc[tx] = self.tx2rc[tx]
        return tx2rc
