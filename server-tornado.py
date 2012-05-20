import sys
import errno
import functools
import socket
# this uses old version of ioloop because it works way faster then the new one...
from lib.tornado import ioloop, iostream
import sys
import gc
import commands

# constants
CR_LF = "\r\n"
PHASE_START   = 'START'
PHASE_DATA    = 'DATA'
PHASE_CONNECT = 'CONNECT'
CHAR_STAR     = '*'
CHAR_DOLLAR   = '$'
RET_OK = '+OK'
CMD_GET = 'GET'
CMD_SET = 'SET'
    
    
def _utf8(v):
    if isinstance(v, unicode):
        return v.encode('utf-8')
    else:
        return v

class Connection(object):
    def __init__(self, stream):
        self.stream = stream
        self.phase = PHASE_CONNECT
        self.read()
        self.buf = ''

    def send(self, s):
        if not self.stream.closed():
            self.stream.write(s + CR_LF)            
        
    def sendok(self):
        self.send(RET_OK)    

    def sendmultival(self, v):
        buf = []
        buf.append('*%i' % len(v))
        for i in v:
            if i:
                lv = _utf8(i)
                try:
                    ln = len(lv)
                except:
                    ln = len(str(lv))
                buf.append('$%i' % ln)
                buf.append(i)
            else:
                buf.append('$-1')
        self.send(CR_LF.join(buf))
    
    def sendval(self, v):
        if v:
            v = _utf8(v)
            try:
                ln = len(v)
            except:
                ln = len(str(v))
            self.send('$%i' % ln)
            self.send(v)
        else:
            self.send('$-1')
    
    def parse_commands(self, args):
        # print "cmd args", args
        self.sendok()
        
    def read(self):
        self.stream.read_until(CR_LF, self.read_until_callback)

    def read_until_callback(self, data):
        self.eol_callback(data)

    def parse_connect_line(self, line):        
        k = line[0]
        v = line[1:].split('\r\n')[0]        
        if k == CHAR_STAR:
            self.phase = PHASE_START
            self.args = {}
            self.received_arg_length = 0
            self.num_args = int(v)
            self.buf = ''
        else:
            raise Exception('commands out of order')

    def parse_start_line(self, line):        
        if line[0] == CHAR_DOLLAR:
            self.received_arg_length = self.received_arg_length + 1
            self.phase = PHASE_DATA
            self.wait_for_data_length = int(line[1:].split('\r\n')[0])
        else:
            raise Exception('commands out of order - 1')
    
    def parse_data_line(self, line):
        self.buf = self.buf + line
        if len(self.buf)-2 == self.wait_for_data_length:
            self.args[self.received_arg_length] = self.buf[:-2]
            self.phase = PHASE_START
            self.buf = ''

            # did we receive everything ???
            if self.received_arg_length == self.num_args:
                self.parse_commands(self.args)
                self.args = {}
                self.phase = PHASE_CONNECT
                self.received_arg_length = 0
                self.num_args = 0

    def eol_callback(self, line):
        if self.phase == PHASE_CONNECT:
            self.parse_connect_line(line)           
        elif self.phase == PHASE_START:
            self.parse_start_line(line)
        elif self.phase == PHASE_DATA:
            self.parse_data_line(line)
        else:
            raise Exception('parser error')

        # read again until we receive everything            
        if not self.stream.closed():  
            if not self.stream._read_callback:              
                self.read()
        else:
            self.debug( "closed stream" )
            raise Exception("closed stream")            


connections = {}
conncnt = 1

def connection_ready(sock, fd, events):
    while True:
        try:
            connection, address = sock.accept()
        except socket.error, e:
            if e[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise
            return
        connection.setblocking(0)
        stream = iostream.IOStream(connection, max_buffer_size=1048576)
        conn = Connection(stream)
        connections[len(connections)+1] = conn


def gc_connections():
    # print "%s connections hanging" % len(connections)
    dele = []
    for i in connections:
        if connections[i].stream.closed():
            del connections[i].stream._read_callback
            dele.append(i)
    for d in dele:
        del connections[d]            
        pass

    
if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setblocking(0)
    sock.bind(('', 6380))
    sock.listen(128)

    io_loop = ioloop.IOLoop.instance()
    callback = functools.partial(connection_ready, sock)
    io_loop.add_handler(sock.fileno(), callback, io_loop.READ)

    try:
        gcn = ioloop.PeriodicCallback(gc_connections, 5000, io_loop)
        gcn.start()
        io_loop.start()
    except KeyboardInterrupt:
        io_loop.stop()
        print "exited cleanly"
