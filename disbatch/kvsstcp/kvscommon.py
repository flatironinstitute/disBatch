import os
import socket

# The line protocol is very simple:
#
#  4 byte operation ('clos', 'dump', 'get_', 'mkey', 'put_', 'view')
#
# 'clos': No additional argument
#
# 'dump': No additional argument
#
# 'get_': One key argument, expects a value argument in reply.
#
# 'mkey' (monitor key): Two key arguments. The second may have a ': ...'
#     suffix indicating events to be monitored.
#
# 'put_': One key argument followed by one value argument.
# 
# 'view': One key argument, expects a value argument in reply.
#
# Key representation:
#     10 bytes: A 10 character string (ascii, not null terminated) with the base 10
#               representation of the byte length of the key string
#     length bytes: the key string
#
# Value representatin:
#     4 bytes: coding scheme.
#     10 bytes: A 10 character string (ascii, not null terminated) with the base 10
#               representation of the byte length of the argument
#     length bytes: the string representing the key
#
# Notes:
#
# 1) Coding schemes for values is a work in progress.
#

AsciiLenChars = 10 

def AsciiLenFormat(n):
    assert(n <= 9999999999)
    return str(n).encode('ascii').rjust(AsciiLenChars)

if hasattr(socket, "MSG_WAITALL") and os.uname()[0] != 'Darwin':
    # MSG_WAITALL on OSX ends up blocking if the tcp buffer is not big enough for the entire message: don't use it
    def recvall(s, n):
        if s is None:
            raise socket.error('socket is None, cannot receive')
        if not n: return b''
        r = s.recv(n, socket.MSG_WAITALL)
        if len(r) < n: raise socket.error('Connection dropped')
        return r
else:
    def recvall(s, n):
        '''Wrapper to deal with partial recvs when we know there are N bytes to be had.'''
        if s is None:
            raise socket.error('socket is None, cannot receive')
        d = b''
        while n:
            b = s.recv(n)
            if not b: raise socket.error('Connection dropped')
            d += b
            n -= len(b)
        return d
