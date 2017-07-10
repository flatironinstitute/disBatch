# This library can be used to submit tasks to disBatch.py --taskserver or --taskcommand

import kvsstcp, os

class DisBatcher(object):
    def __init__(self, tasksname='DisBatcher', kvsserver=None):
        self.kvs = kvsstcp.KVSClient(kvsserver)
        self.kvs.put('task source name', tasksname, False)
        self.taskkey = tasksname + ' task'
        self.donetask = tasksname + ' done!'
        self.resultkey = tasksname + ' result %d'
        self.taskCount = 0
        self.tx2rc = {}

    def done(self):
        self.kvs.put(self.taskkey, self.donetask, False)

    def submit(self, c):
        '''Add a task to the disBatch queue, returning its streamIndex (which may map to multiple taskIds).'''
        self.kvs.put(self.taskkey, c, False)
        self.taskCount += 1
        return self.taskCount

    def syncTasks(self, taskd):
        '''Wait for specified taskIds (not streamIndex) to complete and collect results, returning a dict from taskd keys to returncodes.'''
        tx2rc = {}
        for tx in taskd:
            if tx not in self.tx2rc:
                r = self.kvs.get(self.resultkey%tx, False)
                lags, taskId, streamIndex, repIndex, host, pid, returncode, time, start, end, outbytes, errbytes, cmd = r.split('\t', 12)
                # do something with the rest of these results?
                self.tx2rc[tx] = int(returncode)
            tx2rc[tx] = self.tx2rc[tx]
        return tx2rc
