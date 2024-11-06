#!/usr/bin/env python
from __future__ import print_function
import errno
import os
import socket
import sys
import time
try:
    from cPickle import dumps as PDS, loads as PLS
except ImportError:
    from pickle import dumps as PDS, loads as PLS

try:
    from .kvscommon import *
except:
    from kvscommon import *

class KVSClient(object):
    '''KVS convenience wrapper that includes pickling by default.'''
    def __init__(self, host=None, port=None, retry=0):
        '''Establish connection to a key value storage server at an address
        given by host, port or "host:port"

        If retry > 0, retry the connection this many times if it fails.
        '''
        if not host:
            host = os.environ.get('KVSSTCP_HOST', None)
            # TODO: Silently overrides user supplied value, if there is one.
            port = os.environ.get('KVSSTCP_PORT', None)

        if not host: raise Exception('Missing host')

        if not port:
            if type(host) is tuple:
                host, port = host
            elif ':' in host:
                host, port = host.rsplit(':', 1)
            else:
                raise Exception('Missing port')

        self.addr = (host, int(port))
        self.socket = None
        self.connect(retry)

    def clone(self):
        '''Create a new connection to the same server as this one.'''
        return KVSClient(self.addr)

    # Low-level network operations
    def _close(self):
        if not self.socket: return
        try:
            self._real_socket().close()
        except socket.error:
            pass
        self.socket = None

    def _recvValue(self, doPickle=False):
        l = int(recvall(self.socket, AsciiLenChars))
        payload = recvall(self.socket, l)
        if doPickle: payload = PLS(payload)
        return payload

    def _sendLenAndBytes(self, payload):
        if type(payload) is not bytes:
            payload = bytes(payload, 'utf-8')
        self.socket.sendall(AsciiLenFormat(len(payload)))
        self.socket.sendall(payload)

    class SocketWaiting:
        '''Used as placeholder socket when there's an incomplete get/view call
        that must be retried.  The real socket and outstanding op are stashed.'''
        def __init__(self, socket, op):
            self.socket = socket
            self.op = op

        def __nonzero__(self):
            return True

        def __bool__(self):
            return True

        def __getattr__(self, attr):
            '''Disallow any other operations on a waiting socket.'''
            raise Exception("Previous %s timed out: you must retreive the previously requested '%s' value first." % self.op)

    def _real_socket(self):
        '''Get the real socket, even if we have an outstanding SocketWaiting.'''
        try:
            # for SocketWaiting
            return self.socket.socket
        except AttributeError:
            return self.socket

    def _get_view(self, op, k, encoding, timeout=None):
        try:
            # check if we're waiting for something
            waiting = self.socket.op
        except AttributeError:
            waiting = None
        if waiting == (op, k):
            # continue previous timedout wait
            self.socket = self.socket.socket
        else:
            # new wait
            self.socket.sendall(op)
            self._sendLenAndBytes(k)
        if timeout is None:
            coding = recvall(self.socket, 4)
        else:
            self.socket.settimeout(timeout)
            try:
                c = self.socket.recv(1)
            except socket.timeout:
                self.socket = self.SocketWaiting(self.socket, (op, k))
                return
            except socket.error as e:
                if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                    self.socket = self.SocketWaiting(self.socket, (op, k))
                    return
                else:
                    raise
            finally:
                self._real_socket().settimeout(None)
            if not c:
                raise socket.error("Connection closed")
            coding = c + recvall(self.socket, 3)
        v = self._recvValue(encoding is True and coding == b'PYPK')
        return v if type(encoding) == bool else (coding, v)


    def connect(self, retry=0):
        '''Reconnect, if necessary.  Can be used after an explicit close.'''
        if self.socket: return
        rep = 0
        while 1:
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.socket.connect(self.addr)
                return
            except socket.error as msg:
                self._close()
                if rep >= retry: raise
                print('kvs socket error: %s, retrying' % msg, file=sys.stderr)
            # exponential backoff
            time.sleep(2 ** rep)
            rep += 1

    def close(self):
        '''Close the connection to the KVS storage server. Does a socket shutdown as well.'''
        if not self.socket: return
        try:
            self.socket.sendall(b'clos')
            self.socket.shutdown(socket.SHUT_RDWR)
        except socket.error as e:
            # this is the client --- cannot assume logging is available.
            print('Ignoring exception during client close: "%s"'%e, file=sys.stderr)
        self._close()

    def dump(self):
        '''Returns a snapshot of the KV store and its statistics.'''
        self.socket.sendall(b'dump')
        return self._recvValue(True)

    def get(self, key, encoding=True):
        '''Retrieve and remove a value from the store.  If there is no value
        associated with this key, block until one is added by another client
        (with put).

        If encoding is True, and the value was pickled, then the value will be
        unpickled before being returned.  If encoding is False, just return the
        raw value.  For anything else, return (encoding, value).
        '''
        return self._get_view(b'get_', key, encoding)

    def _get_nb(self, key, encoding=True, timeout=None):
        '''Non-blocking get.

        If timeout is not None, this will only wait for timeout seconds before
        returning None.  In this case, you MUST call this function again in the
        future until it returns a value before doing any other operation,
        otherwise the value may be lost.'''
        return self._get_view(b'get_', key, encoding, timeout)

    def view(self, key, encoding=True):
        '''Retrieve, but do not remove, a value from the store.  See 'get'.'''
        return self._get_view(b'view', key, encoding)

    def _view_nb(self, key, encoding=True, timeout=None):
        '''Non-blocking view.  See '_get_nb' and 'view'.'''
        return self._get_view(b'view', key, encoding, timeout)

    def put(self, key, value, encoding=True):
        '''Add a value to the key.  If encoding is True, pickle the value and
        encode as PYPK.  If False, convert to string and store as ASTR.
        Otherwise, encoding must be a 4 character string, and value must be a
        string.'''
        if encoding is True:
            value = PDS(value)
            encoding = b'PYPK'
        elif encoding is False:
            # TODO: Is this silent stringification too clever by half?
            # Maybe, since unicode strings will end up as "u'\\u...'". perhaps utf8-encode strings, and fail on other types?
            if type(value) is not str and type(value) is not bytes: value = repr(value)
            encoding = b'ASTR'
        else:
            if type(encoding) is not bytes:
                if type(encoding) is not str: encoding = repr(encoding)
                encoding = bytes(encoding, 'utf-8')
            if len(encoding) != 4:
                raise TypeError('Invalid encoding: %s'%encoding)

        self.socket.sendall(b'put_')
        self._sendLenAndBytes(key)
        self.socket.sendall(encoding)
        self._sendLenAndBytes(value)

    def monkey(self, mkey, value):
        '''Make mkey a monitor key. Value encodes what events to monitor and
        for which key:

                Key:Events

        Whenever a listed event occurs for "Key", a put will be done
        to "Mkey" with the value "<event> <key>".  If 'Key' is empty,
        the events listed will be monitored for all keys.  'Events' is
        some subset of 'g', 'p', 'v' and 'w' (get, put, view and
        wait). Monitoring of any event *not* listed is turned off for
        the specified key.
        '''
        self.socket.sendall(b'mkey')
        self._sendLenAndBytes(mkey)
        self._sendLenAndBytes(value)

    def shutdown(self):
        '''Tell the KVS server to shutdown (and run the close() method for this client).'''
        try:
            self._real_socket().sendall(b'down')
        finally:
            self._close()

def addKVSServerArgument(argp, name = 'kvsserver'):
    '''Add an argument to the given ArgumentParser that accepts the address of a running KVSServer, defaulting to $KVSSTCP_HOST:$KVSSTCP_PORT.'''
    host = os.environ.get('KVSSTCP_HOST')
    port = os.environ.get('KVSSTCP_PORT') if host else None
    argp.add_argument(name, metavar='host:port', nargs='?' if port else None, default=host+':'+port if port else None, help='KVS server address.')

if '__main__' == __name__:
    import argparse

    class OpAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            items = getattr(namespace, 'ops', [])
            op = self.option_strings[1][2:]
            if op in ('get', 'view', 'put'):
                encoding = getattr(namespace, 'encoding', False)
                values.append(encoding)
                if encoding is True and op == 'put':
                    values[1] = eval(values[1], {})
                if op in ('get', 'view'):
                    op = '_'+op+'_nb'
                    values.append(getattr(namespace, 'timeout', None))
            values.insert(0, op)
            items.append(values)
            namespace.ops = items

    argp = argparse.ArgumentParser(description='Command-line client to key-value storage server.')
    argp.add_argument('-R', '--retry', default=0, type=int, metavar='COUNT', help='Number of times to retry on connect failure [0]')
    argp.add_argument('-P', '--pickle', dest='encoding', action='store_true', help='(Un-)Pickle values to/from python expressions')
    argp.add_argument('-A', '--no-pickle', dest='encoding', action='store_false', help="Don't (un-)pickle values (default)")
    argp.add_argument('-E', '--encoding', dest='encoding', type=str, metavar='CODE', help='Explicitly set/get encoding (4-character string, ignored on get) [ASTR or PYPK with -P]')
    argp.add_argument('-T', '--timeout', type=float, metavar='SECS', nargs='?', help='Timeout waiting for get/view')
    argp.add_argument('-d', '--dump', action=OpAction, nargs=0, help='Dump the current state')
    argp.add_argument('-g', '--get', action=OpAction, nargs=1, metavar='KEY', help='Retrieve and remove a value')
    argp.add_argument('-v', '--view', action=OpAction, nargs=1, metavar='KEY', help='Retrieve a value')
    argp.add_argument('-p', '--put', action=OpAction, nargs=2, metavar=('KEY','VALUE'), help='Put a value')
    argp.add_argument('-m', '--monkey', action=OpAction, nargs=2, metavar=('MKEY','KEY:EVENTS'), help='Create or update a monitor for the key and events')
    argp.add_argument('-S', '--shutdown', action=OpAction, nargs=0, help='Tell the server to shutdown')
    argp.add_argument('-s', '--sleep', action=OpAction, nargs=1, type=float, metavar='SECS', help='Pause for a time')
    addKVSServerArgument(argp, 'server')
    args = argp.parse_args()

    kvs = KVSClient(args.server, retry = args.retry)

    if hasattr(args, 'ops') and args.ops:
        for cmd in args.ops:
            op = cmd.pop(0)
            if op == 'sleep':
                time.sleep(*cmd)
            else:
                try:
                    r = getattr(kvs, op)(*cmd)
                    if r is not None: print(r)
                except Exception as e:
                    print(e, file=sys.stderr)
    else:
        print("Nothing to do.")
    kvs.close()
