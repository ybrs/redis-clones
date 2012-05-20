CXXFLAGS=-O2 -g -Wall -pthread -lrt
CFLAGS = -O2 -g -Wall -pthread -lrt --std=gnu99

all: server_epoll

server_epoll: server_epoll.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

