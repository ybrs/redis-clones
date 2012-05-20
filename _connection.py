CR_LF = "\r\n"
PHASE_START   = 1
PHASE_DATA    = 2
PHASE_CONNECT = 3


CHAR_STAR = '*'
CHAR_DOLLAR = '$'
RET_OK = '+OK'

CMD_GET = 'GET'
CMD_SET = 'SET'

class Connection(object):

    def __init__(self, fd, _fileno):
        self.data = ''
        self.fd = fd
        self.write_buffer = ''
        
        self.parser = Parser(self)
        self.fileno = _fileno

    def _consume(self, loc):
        result = self.data[:loc]
        self.data = self.data[loc:]
        return result

    def read_callback(self, chunk):
        self.data += chunk                
        loc = self.data.find(CR_LF)        
        while (loc != -1):
            self.parser.eol_callback(self._consume(loc + 2))            
            loc = self.data.find(CR_LF)


    def write(self, data):        
        self.fd.send(data)
        



class Parser(object):
    
    def __init__(self, socket):        
        self.phase = PHASE_CONNECT
        self.socket = socket
        self.buf = ''
        
    def send(self, s):        
        self.socket.write(s)
        self.socket.write(CR_LF)
    
    def sendok(self):        
        self.send(RET_OK)  

    def read(self):
        data = self.socket.readline()
        self.eol_callback(data)

    def parse_commands(self, args):
        # print "cmd args", args
        self.sendok()

    def parse_connect_line(self, line):
        #print "parse_connect_line", line
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
    
    def parse_data_line(self,  line):
        #print "parse_data_line", line
        self.buf = self.buf + line
        #print "parse_data_line", "buf: (", len(self.buf), ")", self.buf

        if len(self.buf)-2 == self.wait_for_data_length:
            # print "parse_data_line all buffered...."
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
        # line = data[0:len(data)-2] # get rid of the last crlf
        # print "phase", self.phase
        if self.phase == PHASE_CONNECT:
            self.parse_connect_line(line)           
        elif self.phase == PHASE_START:
            self.parse_start_line(line)
        elif self.phase == PHASE_DATA:
            self.parse_data_line(line)
        else:
            raise Exception('parser error')
