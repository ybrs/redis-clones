import socket
import signal
import weakref
import errno
import logging
import pyev

logging.basicConfig(level=logging.DEBUG)

STOPSIGNALS = (signal.SIGINT, signal.SIGTERM)
NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)

CR_LF = "\r\n"
PHASE_START   = 'START'
PHASE_DATA    = 'DATA'
PHASE_CONNECT = 'CONNECT'
CHAR_STAR     = '*'
CHAR_DOLLAR   = '$'
RET_OK = '+OK'
CMD_GET = 'GET'
CMD_SET = 'SET'
    

class Connection(object):

    def __init__(self, sock, address, loop):
        self.sock = sock
        self.address = address
        self.sock.setblocking(0)
        self.buf = ""
        self.watcher = pyev.Io(self.sock._sock, pyev.EV_READ, loop, self.io_cb)
        self.watcher.start()
        # logging.debug("{0}: ready".format(self))
        self.phase = PHASE_CONNECT
        self.data = ''

    def reset(self, events):
        self.watcher.stop()
        self.watcher.set(self.sock, events)
        self.watcher.start()

    def handle_error(self, msg, level=logging.ERROR, exc_info=True):
        logging.log(level, "{0}: {1} --> closing".format(self, msg),
                    exc_info=exc_info)
        self.close()


    def parse_commands(self, args):
        # print "cmd args", args
        self.sendok()

    def parse_connect_line(self, line):        
        k = line[0]
        v = line[1:].split('\r\n')[0]
        #print ">>>", k, v
        if k == CHAR_STAR:
            self.phase = PHASE_START
            self.args = {}
            self.received_arg_length = 0
            self.num_args = int(v)
            self.buf = ''
        else:
            raise Exception('commands out of order')

    def parse_start_line(self, line):        
        #print "parse_start_line", line
        if line[0] == CHAR_DOLLAR:
            self.received_arg_length = self.received_arg_length + 1
            self.phase = PHASE_DATA
            self.wait_for_data_length = int(line[1:].split('\r\n')[0])
            # print ">>> expected data len", self.wait_for_data_length
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
        if not line:
            return

        if self.phase == PHASE_CONNECT:
            self.parse_connect_line(line)           
        elif self.phase == PHASE_START:
            self.parse_start_line(line)
        elif self.phase == PHASE_DATA:
            self.parse_data_line(line)
        else:
            raise Exception('parser error')

    def _consume(self, loc):
        result = self.data[:loc]
        self.data = self.data[loc:]
        return result


    def read_chunk(self, chunk):
        self.data += chunk                
        loc = self.data.find(CR_LF)        
        while (loc != -1):
            self.eol_callback(self._consume(loc + 2))            
            loc = self.data.find(CR_LF)

    def handle_read(self):
        try:
            buf = self.sock.recv(1024)
        except socket.error as err:
            if err.args[0] not in NONBLOCKING:
                self.handle_error("error reading from {0}".format(self.sock))
        if buf:
            self.read_chunk(buf)
            # self.reset(pyev.EV_READ | pyev.EV_WRITE)
        else:
            # self.handle_error("connection closed by peer", logging.DEBUG, False)
            pass

    def send(self, s):        
        self.sock.send(s + CR_LF)        
        self.reset(pyev.EV_READ | pyev.EV_WRITE)

    def sendok(self):        
        self.send(RET_OK)  

    def handle_write(self):
        try:
            sent = self.sock.send(self.buf)
        except socket.error as err:
            if err.args[0] not in NONBLOCKING:
                self.handle_error("error writing to {0}".format(self.sock))
        else :
            self.buf = self.buf[sent:]
            if not self.buf:
                self.reset(pyev.EV_READ)

    def io_cb(self, watcher, revents):
        if revents & pyev.EV_READ:
            self.handle_read()
        else:
            self.handle_write()

    def close(self):
        self.sock.close()
        self.watcher.stop()
        self.watcher = None
        #logging.debug("{0}: closed".format(self))


class Server(object):

    def __init__(self, address):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(address)
        self.sock.setblocking(0)
        self.address = self.sock.getsockname()
        self.loop = pyev.default_loop()
        self.watchers = [pyev.Signal(sig, self.loop, self.signal_cb)
                         for sig in STOPSIGNALS]
        self.watchers.append(pyev.Io(self.sock._sock, pyev.EV_READ, self.loop,
                                     self.io_cb))
        self.conns = weakref.WeakValueDictionary()

    def handle_error(self, msg, level=logging.ERROR, exc_info=True):
        logging.log(level, "{0}: {1} --> stopping".format(self, msg),
                    exc_info=exc_info)
        self.stop()

    def signal_cb(self, watcher, revents):
        self.stop()

    def io_cb(self, watcher, revents):
        try:
            while True:
                try:
                    sock, address = self.sock.accept()
                except socket.error as err:
                    if err.args[0] in NONBLOCKING:
                        break
                    else:
                        raise
                else:
                    self.conns[address] = Connection(sock, address, self.loop)
        except Exception:
            self.handle_error("error accepting a connection")

    def start(self):
        self.sock.listen(socket.SOMAXCONN)
        for watcher in self.watchers:
            watcher.start()
        #logging.debug("{0}: started on {0.address}".format(self))
        self.loop.start()

    def stop(self):
        self.loop.stop(pyev.EVBREAK_ALL)
        self.sock.close()
        while self.watchers:
            self.watchers.pop().stop()
        for conn in self.conns.values():
            conn.close()
        #logging.debug("{0}: stopped".format(self))


if __name__ == "__main__":
    server = Server(("127.0.0.1", 6380))
    server.start()