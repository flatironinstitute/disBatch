#!/usr/bin/env python
from collections import defaultdict as DD
try:
    from cPickle import dumps as PDS
except ImportError:
    from pickle import dumps as PDS
from functools import partial
import errno
import gc
import logging
import os
import resource
import select
import socket
import sys
import threading

try:
    from .kvscommon import *
except:
    from kvscommon import *

logger = logging.getLogger('kvs')

# There are some cyclic references in in asyncio, handlers, waiters, etc., so I'm re-enabling this:
#gc.disable()

_DISCONNECTED = frozenset((errno.ECONNRESET, errno.ENOTCONN, errno.ESHUTDOWN, errno.ECONNABORTED, errno.EPIPE, errno.EBADF))
_BUFSIZ = 8192

# Concepts:
#
# Every connection is represented by a dispatcher.
#
# Every dispatcher is registered with a handler, which in effect runs
# the KVS server loop.
#
# The handler runs an infinite loop that mostly sits on a poll of some
# sort waiting for one or more events associated with registered
# connections (identified by their file descriptor).
#
# When an event occurs the dispatcher associated with the connection
# is used to process the event.
#
# The listening socket is treated just like any other connection and
# has its own dispatcher. An "event" on this connection triggers an
# accept that leads to the creation of a new dispatcher
# (KVSRequestDispatcher) to handle exchanges with the client.
#
# This approach has the very important benefit that it is single threaded.

class Handler(object):
    '''Based on asyncore, but with a simpler, stricter per-thread interface that allows better performance.'''
    def __init__(self):
        self.disps = dict()
        self.current = None
        self.running = True

    def register(self, disp):
        self.disps[disp.fd] = disp

    def unregister(self, disp):
        del self.disps[disp.fd]

    def run(self):
        while self.running:
            try:
                self.poll()
            except IOError as e:
                if e.errno == errno.EINTR:
                    continue
                raise
        for d in list(self.disps.values()):
            try:
                d.close()
            except Exception as e:
                logger.info('%r reported %r on close in handler.', d, e)
        self.close()

    def writable(self, disp):
        "Equivalent to setting mask | OUT, but safe to be called from other (non-current) handlers."
        if disp.mask & self.OUT: return
        disp.mask |= self.OUT
        # write can be called from other threads
        if self.current is not disp:
            self.modify(disp)

    def close(self):
        self.running = False

class PollHandler(Handler):
    def __init__(self):
        self.IN, self.OUT, self.EOF = select.POLLIN, select.POLLOUT, select.POLLHUP
        self.poller = select.poll()
        Handler.__init__(self)

    def register(self, disp):
        Handler.register(self, disp)
        self.poller.register(disp.fd, disp.mask)

    def unregister(self, disp):
        self.poller.unregister(disp.fd)
        Handler.unregister(self, disp)

    def modify(self, disp):
        self.poller.modify(disp.fd, disp.mask)

    def poll(self):
        ev = self.poller.poll()
        for (f, e) in ev:
            d = self.current = self.disps[f]
            oldm = d.mask
            if e & self.EOF:
                d.handle_close()
                continue
            if e & self.IN:
                d.handle_read()
            if d.mask & self.OUT:
                d.handle_write()
            self.current = None
            if d.mask != oldm and not (d.mask & self.EOF):
                self.modify(d)

    def stop(self, disp):
        Handler.close(self)

class EPollHandler(PollHandler):
    def __init__(self):
        self.IN, self.OUT, self.EOF = select.EPOLLIN, select.EPOLLOUT, select.EPOLLHUP
        self.poller = select.epoll()
        Handler.__init__(self)

    def close(self):
        self.poller.close()
        Handler.close(self)

class KQueueHandler(Handler):
    def __init__(self):
        self.IN, self.OUT, self.EOF = 1, 2, 4
        self.kqueue = select.kqueue()
        Handler.__init__(self)

    def register(self, disp):
        Handler.register(self, disp)
        disp.curmask = 0
        self.modify(disp)

    def unregister(self, disp):
        disp.mask = 0
        self.modify(disp)
        Handler.unregister(self, disp)

    def modify(self, disp):
        c = []
        if disp.mask & self.IN:
            if not (disp.curmask & self.IN):
                c.append(select.kevent(disp.fd, select.KQ_FILTER_READ, select.KQ_EV_ADD))
        elif disp.curmask & self.IN:
            c.append(select.kevent(disp.fd, select.KQ_FILTER_READ, select.KQ_EV_DELETE))
        if disp.mask & self.OUT:
            if not (disp.curmask & self.OUT):
                c.append(select.kevent(disp.fd, select.KQ_FILTER_WRITE, select.KQ_EV_ADD))
        elif disp.curmask & self.OUT:
            c.append(select.kevent(disp.fd, select.KQ_FILTER_WRITE, select.KQ_EV_DELETE))
        if c: self.kqueue.control(c, 0)
        disp.curmask = disp.mask

    def poll(self):
        try:
            ev = self.kqueue.control(None, 1024)
        except OSError as e:
            if e.errno == errno.EBADF:
                self.running = False
                return
            raise
        for e in ev:
            d = self.current = self.disps[e.ident]
            if e.filter == select.KQ_FILTER_READ:
                d.handle_read()
            elif e.filter == select.KQ_FILTER_WRITE:
                d.handle_write()
            self.current = None
            if self.running: self.modify(d)

    def close(self):
        self.kqueue.close()
        Handler.close(self)

    def stop(self, disp):
        self.close()

class Dispatcher(object):
    def __init__(self, sock, handler, mask=0):
        self.sock = sock
        self.fd = sock.fileno()
        self.mask = mask
        sock.setblocking(0)
        self.handler = handler

    def open(self):
        self.handler.register(self)

    def close(self):
        self.mask = self.handler.EOF
        self.handler.unregister(self)
        try:
            self.sock.close()
        except socket.error:
            pass

    def accept(self):
        try:
            return self.sock.accept()
        except socket.error as e:
            if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                return
            if e.errno in _DISCONNECTED or e.errno == errno.EINVAL:
                self.handle_close()
                return
            raise

    def send(self, data):
        try:
            return self.sock.send(data)
        except socket.error as e:
            if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                return 0
            if e.errno in _DISCONNECTED:
                self.handle_close()
                return 0
            raise

    def recv(self, siz):
        try:
            data = self.sock.recv(siz)
            if not data:
                self.handle_close()
            return data
        except socket.error as e:
            if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                return b''
            if e.errno in _DISCONNECTED:
                self.handle_close()
                return b''
            raise

    def recv_into(self, buf):
        try:
            n = self.sock.recv_into(buf)
            if n == 0:
                self.handle_close()
            return n
        except socket.error as e:
            if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                return b''
            if e.errno in _DISCONNECTED:
                self.handle_close()
                return b''
            raise

    def shutdown(self):
        try:
            self.mask |= self.handler.IN
            self.sock.shutdown(socket.SHUT_RDWR)
        except socket.error as e:
            if e.errno not in _DISCONNECTED: raise

    def handle_close(self):
        self.close()

class StreamDispatcher(Dispatcher):
    '''Based on asyncore.dispatcher_with_send, works with EventHandler.
    Also allows input of known-size blocks.'''
    def __init__(self, sock, handler):
        super(StreamDispatcher, self).__init__(sock, handler)
        self.out_buf = []
        self.in_buf = memoryview(bytearray(_BUFSIZ))
        self.in_off = 0
        self.read_size = 0
        self.read_handler = None

    def write(self, *data):
        for d in data:
            self.out_buf.append(memoryview(d))
        self.handler.writable(self)

    def handle_write(self):
        while self.out_buf:
            buf = self.out_buf[0]
            r = self.send(buf[:1048576])
            if r < len(buf):
                if r: self.out_buf[0] = buf[r:]
                return
            self.out_buf.pop(0)
        self.mask &= ~self.handler.OUT

    def next_read(self, size, f):
        self.read_size = size
        if size > len(self.in_buf):
            buf = memoryview(bytearray(max(size, _BUFSIZ)))
            buf[:self.in_off] = self.in_buf[:self.in_off]
            self.in_buf = buf
        self.read_handler = f
        self.mask |= self.handler.IN

    def handle_read(self):
        if self.in_off < len(self.in_buf):
            self.in_off += self.recv_into(self.in_buf[self.in_off:])
        while True:
            handler = self.read_handler
            z = self.read_size
            if not handler or self.in_off < z:
                return
            i = self.in_buf[:z]
            self.in_buf = self.in_buf[z:]
            self.in_off -= z
            self.read_handler = None
            self.mask &= ~self.handler.IN
            handler(i)

class KVSRequestDispatcher(StreamDispatcher):
    def __init__(self, pair, server, handler):
        sock, self.addr = pair
        self.server = server
        # Keep track of any currently waiting get:
        self.waiter = None
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        super(KVSRequestDispatcher, self).__init__(sock, handler)
        logger.info('Accepted connect from %r', self.addr)
        self.next_op()
        self.open()

    def handle_close(self):
        self.cancel_waiter()
        logger.info('Closing connection from %r', self.addr)
        self.close()

    def error(self, msg):
        logger.error('Error from %r: %s' % (self.addr, msg))
        self.close()

    def cancel_waiter(self):
        if self.waiter:
            self.server.kvs.cancel_wait(self.waiter)
            self.waiter = None

    def next_op(self):
        self.next_read(4, self.handle_op)

    def next_lendata(self, handler):
        # wait for variable-length data prefixed by AsciiLenFormat
        def handle_len(l):
            l = l.tobytes()
            try:
                n = int(l)
            except ValueError:
                n = -1
            if n < 0:
                self.error("invalid data len: '%s'" % l)
                return
            self.next_read(n, handler)
        self.next_read(AsciiLenChars, handle_len)

    def handle_op(self, op):
        op = op.tobytes()
        if b'clos' == op:
            self.shutdown()
        elif b'down' == op:
            logger.info('Calling server shutdown')
            self.server.shutdown()
        elif b'dump' == op:
            d = self.server.kvs.dump()
            self.write(AsciiLenFormat(len(d)), d)
            self.next_op()
        elif op in [b'get_', b'mkey', b'put_', b'view']:
            self.next_lendata(partial(self.handle_opkey, op))
        else:
            self.error("Unknown op: '%r'" % op)

    def handle_opkey(self, op, key):
        key = key.tobytes()
        #DEBUGOFF            logger.debug('(%s) %s key "%s"', whoAmI, reqtxt, key)
        if b'mkey' == op:
            self.next_lendata(partial(self.handle_mkey, key))
        elif b'put_' == op:
            self.next_read(4, lambda encoding:
                self.next_lendata(partial(self.handle_put, key, encoding)))
        else: # 'get_' or 'view'
            # Cancel waiting for any previous get/view operation (since client wouldn't be able to distinguish the async response)
            self.cancel_waiter()
            self.waiter = KVSWaiter(op, key, self.handle_got)
            self.server.kvs.wait(self.waiter)
            # But keep listening for another op (like 'clos') to cancel this one
            self.next_op()

    def handle_mkey(self, key, val):
        #DEBUGOFF                logger.debug('(%s) val: %s', whoAmI, repr(val))
        self.server.kvs.monkey(key, val)
        self.next_op()

    def handle_put(self, key, encoding, val):
        # TODO: bytearray val?
        #DEBUGOFF                logger.debug('(%s) val: %s', whoAmI, repr(val))
        self.server.kvs.put(key, (encoding, val))
        self.next_op()

    def handle_got(self, encval):
        (encoding, val) = encval
        self.write(encoding, AsciiLenFormat(len(val)), val)
        self.waiter = None

class KVSWaiter:
    def __init__(self, op, key, handler):
        if op == b'get_': op = b'get'
        self.op = op
        self.delete = op == b'get'
        self.key = key
        self.handler = handler

class KVS(object):
    '''Get/Put/View implements a client-server key value store. If no
    value is associated with a given key, clients will block on get or
    view until a value is available. Multiple values may be associated
    with any given key.

    This is, by design, a very simple, lightweight service that only
    depends on standard Python modules.

    '''
 
    def __init__(self, getIndex=0, viewIndex=-1):
        self.getIndex, self.viewIndex = getIndex, viewIndex #TODO: Add sanity checks?
        self.key2mon = DD(lambda:DD(set)) # Maps a normal key to keys that monitor it.
        self.monkeys = set()              # List of monitor keys.
        # store and waiters are mutually exclusive, and could be kept in the same place
        self.store = DD(list)
        self.waiters = DD(list)
        self.opCounts = {b'get': 0, b'put': 0, b'view': 0, b'wait': 0}
        self.ac, self.rc = 0, 0

    def _doMonkeys(self, op, k):
        # Don't monitor operations on monitor keys.
        if k in self.monkeys: return
        #DEBUGOFF        logger.debug('doMonkeys: %s %s %s', op, k, repr(self.key2mon[True][op] | self.key2mon[k][op]))
        for p in (True, k):
            for mk in self.key2mon[p][op]:
                self.put(mk, (b'ASTR', repr((op, k))))
        
    def dump(self):
        '''Utility function that returns a snapshot of the KV store.'''
        def vrep(v):
            t = v[0].tobytes()
            # Omit or truncate some values, in which cases add the original length as a third value
            if v == b'JSON' or t == b'HTML': return (t, v[1].tobytes())
            if t != b'ASTR': return (t, None, len(v[1]))
            if v[1][:6].tobytes().lower() == '<html>': return (t, v[1].tobytes()) # for backwards compatibility only
            if len(v[1]) > 50: return (t, v[1][:24].tobytes() + '...' + v[1][-23:].tobytes(), len(v[1]))
            return (t, v[1].tobytes())

        return PDS(([self.opCounts[b'get'], self.opCounts[b'put'], self.opCounts[b'view'], self.opCounts[b'wait'], self.ac, self.rc], [(k, len(v)) for k, v in self.waiters.items() if v], [(k, len(vv), vrep(vv[-1])) for k, vv in self.store.items() if vv]))

    def wait(self, waiter):
        '''Atomically (remove and) return a value associated with key k. If
        none, block.'''
        #DEBUGOFF        logger.debug('wait: %s, %s', repr(waiter.key), repr(waiter.op))
        self._doMonkeys(waiter.op, waiter.key)
        vv = self.store.get(waiter.key)
        if vv:
            if waiter.delete:
                v = vv.pop(self.getIndex)
                if not vv: self.store.pop(waiter.key)
            else:
                v = vv[self.viewIndex]
            self.opCounts[waiter.op] += 1
            #DEBUGOFF                logger.debug('_gv (%s): %s => %s (%d)', waiter.op, waiter.key, repr(v[0]), len(v[1]))
            waiter.handler(v)
        else:
            self.waiters[waiter.key].append(waiter)
            self.opCounts[b'wait'] += 1
            self._doMonkeys(b'wait', waiter.key)
            #DEBUGOFF                logger.debug('(%s) %s acquiring', repr(waiter), repr(s))
            self.ac += 1

    def cancel_wait(self, waiter):
        ww = self.waiters.get(waiter.key)
        if ww:
            try:
                ww.remove(waiter)
            except ValueError:
                pass
            if not ww: self.waiters.pop(waiter.key)

    def monkey(self, mkey, v):
        '''Make Mkey a monitor key. Value encodes what events to monitor and
        for which key:

                Key:Events

        Whenever a listed event occurs for "Key", a put will be done
        to "Mkey" with the value "<event> <key>".  If 'Key' is empty,
        the events listed will be monitored for all keys.  'Events' is
        some subset of 'g', 'p', 'v' and 'w' (get, put, view and
        wait). Monitoring of any event *not* listed is turned off for
        the specified key.

        '''
        #DEBUGOFF        logger.debug('monkey: %s %s', mkey, v)
        if b':' not in v: return #TODO: Add some sort of error handling?
        self.monkeys.add(mkey)
        k, events = v.rsplit(b':', 1)
        if not k: k = True
        for e, op  in [(b'g', b'get'), (b'p', b'put'), (b'v', b'view'), (b'w', b'wait')]:
            if e in events:
                self.key2mon[k][op].add(mkey)
            else:
                try: self.key2mon[k][op].remove(mkey)
                except KeyError: pass
        #DEBUGOFF        logger.debug('monkey: %s', repr(self.key2mon))

    def put(self, k, v):
        '''Add value v to those associated with the key k.'''
        #DEBUGOFF        logger.debug('put: %s, %s', repr(k), repr(v))
        self.opCounts[b'put'] += 1
        ww = self.waiters.get(k) # No waiters is probably most common, so optimize for
                                 # that. ww will be None if no waiters have been
                                 # registered for key k.
        consumed = False
        if ww:
            while ww:
                waiter = ww.pop(0)
                #DEBUGOFF                    logger.debug('%s releasing', repr(waiter))
                self.rc += 1
                self.opCounts[waiter.op] += 1
                waiter.handler(v)
                if waiter.delete:
                    consumed = True
                    break
            if not ww: self.waiters.pop(k)

        if not consumed: self.store[k].append(v)
        self._doMonkeys(b'put', k)

class KVSServer(threading.Thread, Dispatcher):
    def __init__(self, host=None, port=0):
        if not host: host = socket.gethostname()

        self.kvs = KVS()

        snof, hnof = resource.getrlimit(resource.RLIMIT_NOFILE)
        hnof = min(hnof, 1000000) # don't need unreasonably many
        if snof < hnof:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (hnof, hnof))
                logger.info('Raised max open files from %d to %d', snof, hnof)
            except:
                logger.info('Failed to raise max open files from %d to %d; continuing anyway', snof, hnof)
                pass

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setblocking(1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        logger.info('Setting queue size to 4000')
        self.sock.listen(4000)
        self.cinfo = self.sock.getsockname()

        if hasattr(select, 'epoll'):
            self.handler = EPollHandler()
        elif hasattr(select, 'kqueue'):
            self.handler = KQueueHandler()
        else:
            self.handler = PollHandler()
        Dispatcher.__init__(self, self.sock, self.handler, self.handler.IN)
        self.open()

        threading.Thread.__init__(self, name='KVSServerThread', target=self.handler.run)
        self.start()

    def handle_read(self):
        pair = self.accept()
        if pair:
            KVSRequestDispatcher(pair, self, self.handler)

    def handle_close(self):
        logger.info('Server shutting down')
        self.close()
        self.handler.close()

    def shutdown(self):
        if self.handler.running:
            super(KVSServer, self).shutdown()
            self.handler.stop(self)

    def env(self, env = os.environ.copy()):
        '''Add the KVSSTCP environment variables to the given environment.'''
        env['KVSSTCP_HOST'] = self.cinfo[0]
        env['KVSSTCP_PORT'] = str(self.cinfo[1])
        return env

if '__main__' == __name__:
    import argparse
    argp = argparse.ArgumentParser(description='Start key-value storage server.')
    argp.add_argument('-H', '--host', default='', help='Host interface (default is hostname).')
    argp.add_argument('-p', '--port', type=int, default=0, help='Port (default is 0 --- let the OS choose).')
    argp.add_argument('-a', '--addrfile', default=None,  metavar='AddressFile', type=argparse.FileType('w'), help='Write address to this file.')
    argp.add_argument('-e', '--execcmd', default=None,  metavar='COMMAND SEQUENCE', help='Execute command with augmented environment.')
    argp.add_argument('-l', '--logfile', default=None,  metavar='KVSSLogfile', type=argparse.FileType('w'), help='Log file for key-value storage server.')
    args = argp.parse_args()

    # TODO: figure out where this should really go.
    lconf = {'format': '%(asctime)s %(levelname)-8s %(name)-15s: %(message)s', 'level': logging.DEBUG}
    if args.logfile:
        args.logfile.close()
        lconf['filename'] = args.logfile.name
    logging.basicConfig(**lconf)

    t = KVSServer(args.host, args.port)
    addr = '%s:%d'%t.cinfo
    logger.info('Server running at %s.', addr)
    if args.addrfile:
        args.addrfile.write(addr)
        args.addrfile.close()

    try:
        if args.execcmd:
            import subprocess
            logger.info('Launching: %r, env %r', args.execcmd, t.env())
            subprocess.check_call(args.execcmd, shell=True, env=t.env())
        else:
            while t.isAlive():
                t.join(60)
    finally:
        t.shutdown()
    t.join()
