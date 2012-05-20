DEF CR_LF = "\r\n"
DEF PHASE_START   = 1
DEF PHASE_DATA    = 2
DEF PHASE_CONNECT = 3


DEF CHAR_STAR = '*'
DEF CHAR_DOLLAR = '$'
DEF RET_OK = '+OK'

CMD_GET = 'GET'
CMD_SET = 'SET'

cdef class Connection(object):

    cdef object data
    cdef public object fd
    cdef object write_buffer
    cdef Parser parser
    cdef public int fileno


    def __init__(self, fd, _fileno):
        self.data = ''
        self.fd = fd
        self.write_buffer = ''
        
        self.parser = Parser(self)
        self.fileno = _fileno

    cdef char* _consume(self, int loc):
        result = self.data[:loc]
        self.data = self.data[loc:]
        return result

    def read_callback(self, char *chunk):
        self.data += chunk                
        loc = self.data.find(CR_LF)        
        while (loc != -1):
            self.parser.eol_callback(self._consume(loc + 2))            
            loc = self.data.find(CR_LF)


    cdef public void write(self, char *data):        
        self.fd.send(data)
        



cdef class Parser(object):
    
    cdef int phase
    cdef Connection socket
    cdef object args
    cdef int received_arg_length
    cdef int num_args
    cdef object buf     
    cdef int wait_for_data_length

    def __init__(self, Connection socket):        
        self.phase = PHASE_CONNECT
        self.socket = socket
        self.buf = ''
        
    cdef void send(self, char *s):        
        self.socket.write(s)
        self.socket.write(CR_LF)
    
    cdef void sendok(self):        
        self.send(RET_OK)  

    cdef void read(self):
        data = self.socket.readline()
        self.eol_callback(data)

    cdef parse_commands(self, object args):
        # print "cmd args", args
        self.sendok()

    cdef parse_connect_line(self, char *line):
        #print "parse_connect_line", line
        cdef char k = line[0]
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

    cdef parse_start_line(self, char *line):        
        #print "parse_start_line", line
        if line[0] == CHAR_DOLLAR:
            self.received_arg_length = self.received_arg_length + 1
            self.phase = PHASE_DATA
            self.wait_for_data_length = int(line[1:].split('\r\n')[0])
            # print ">>> expected data len", self.wait_for_data_length
        else:
            raise Exception('commands out of order - 1')
    
    cdef parse_data_line(self, char *line):
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
