import eventlet


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
        if not self.stream.closed:
            self.stream.write(s + CR_LF)
            self.stream.flush()       
        
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
        data = self.stream.readline()
        self.eol_callback(data)

    def read_until_callback(self, data):
        self.eol_callback(data)

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


def handle(fd):
    # print "client connected"
    conn = Connection(fd)
    while True:
        # pass through every non-eof line
        data = fd.readline()
        if not data:
            break
        conn.eol_callback(data)
    # print "client disconnected"

#from eventlet import api,hubs
#hubs.use_hub("pyevent")

print "server socket listening on port 6380"
server = eventlet.listen(('0.0.0.0', 6380))
pool = eventlet.GreenPool()
while True:
    try:
        new_sock, address = server.accept()        
        # print "accepted", address
        pool.spawn_n(handle, new_sock.makefile('rw'))
    except (SystemExit, KeyboardInterrupt):
        break