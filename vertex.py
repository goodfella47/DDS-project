from socket import socket
from socket import error
from socket import AF_INET
from socket import SOCK_STREAM
from socket import SOCK_DGRAM
from socket import SHUT_RDWR
from socket import SHUT_WR
from threading import Thread
from threading import Lock
import math


def vertex(ID):
    v = vertex_attr(ID)


class vertex_attr:
    # vertex attributes class
    def __init__(self, ID):
        self.shut_down_flag = False
        self.got_parent = False
        self.got_grandparent = False
        file_path = f'input_vertex_{ID}.txt'
        # assigns graph_size, master_udp, master_ip, udp_listen, tcp_listen, parent_tcp, parent_ip, children attributes
        self.decodeFile(file_path)  # get vertex attributes from file
        self.ID = ID
        self.color = ID
        self.root = False
        self.eight_color = True
        self.colors3 = ['done', '011', '100', '101', '110', '111']
        self.x = self.colors3.pop()
        self.state = 'send_my_color'

        if self.parent_ip == 'None':
            self.root = True
            self.color = ''.zfill(len(self.ID))
            self.parent_color = 'None'

        # ALGORITHM STARTS HERE
        self.beginAlg()

    def decodeFile(self, file_path):
        with open(file_path) as file:
            self.graph_size = int(next(file).strip('\n'))
            self.master_udp = int(next(file).strip('\n'))
            self.master_ip = next(file).strip('\n')
            self.udp_listen = int(next(file).strip('\n'))
            self.tcp_listen = int(next(file).strip('\n'))
            self.parent_tcp = next(file).strip('\n')
            if self.parent_tcp != 'None':
                self.parent_tcp = int(self.parent_tcp)
            self.parent_ip = next(file).strip('\n')
            self.children = []
            for line in file:
                if line.strip('\n') == '*':
                    break
                child_tcp = int(line.strip('\n'))
                child_ip = next(file).strip('\n')
                self.children.append((child_tcp, child_ip))

    def listenUDP(self):
        self.first_round = True
        sock_udp = socket(AF_INET, SOCK_DGRAM)
        sock_udp.bind(('', self.udp_listen))
        while True:
            if not self.shut_down_flag:
                data, addr = sock_udp.recvfrom(4096)
                if self.eight_color:
                    Thread(target=self.eightColoring, args=(data,)).start()
                else:
                    if self.state == 'send_my_color' and not self.first_round:
                        self.state = 'send_parent_color'
                    elif self.state == 'send_parent_color':
                        self.state = 'evaluate_3'
                    else:
                        self.state = 'send_my_color'
                    self.first_round = False
                    Thread(target=self.threeColoring, args=()).start()
            else:
                break
        sock_udp.close()

    def listenTCP(self):
        sock_tcp = socket(AF_INET, SOCK_STREAM)  # TCP: SOCK_STREAM
        sock_tcp.bind(('', self.tcp_listen))
        sock_tcp.listen()
        while True:
            conn, addr = sock_tcp.accept()
            data = conn.recv(4096)  # color received from parent
            if data.decode() == 'DIE':
                break
            # proceed to change my own color
            Thread(target=self.decodeMessage, args=(data,)).start()
        sock_tcp.close()

    def sendTCP(self, ip, port, message):
        sock_TCP = socket(AF_INET, SOCK_STREAM)
        sock_TCP.connect((ip, port))
        sock_TCP.send(str(message).encode())
        with open(f'output_vertex_{self.ID}.txt', "a") as file:
            file.write(f'{message}_{port}\n')
        sock_TCP.shutdown(SHUT_RDWR)
        sock_TCP.close()

    def sendUDP(self, message):
        sock_UDP = socket(AF_INET, SOCK_DGRAM)
        sock_UDP.sendto(str(message).encode(), (self.master_ip, self.master_udp))
        sock_UDP.close()

    def decodeMessage(self, data):
        if self.eight_color:
            self.threadLock.acquire()
            color = data.decode()
            self.parent_color = color
            arg = f'next_{self.ID}'
            self.threadLock.release()
            Thread(target=self.sendUDP, args=(arg,)).start()

        else:
            color, who = data.decode().split('_')
            if who == 'parent':
                self.threadLock.acquire()
                self.parent_color = color
                self.threadLock.release()
            else:
                self.threadLock.acquire()
                self.grandparent_color = color
                self.threadLock.release()
            arg = f'next_{self.ID}'
            Thread(target=self.sendUDP, args=(arg,)).start()

    def sendToChildren(self, color_to_send):
        if self.children:  # if Im a leaf -> break
            threads = []
            for port, ip in self.children:
                threads.append(Thread(target=self.sendTCP, args=(ip, port, color_to_send,)))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

    def assignNewColor(self, parent_color):
        L = len(self.color)  # current color length
        new_L = 1 + math.floor(math.log2(L - 1))  # new color length - 1
        for i in range(len(self.color)):
            if self.color[::-1][i] != parent_color[::-1][i]:  # find the different bit
                color = f'{i:0{new_L}b}{self.color[::-1][i]}'  # assign new color
                self.threadLock.acquire()
                self.color = color
                self.threadLock.release()
                break

    def beginAlg(self):
        # starts listening to master and neighbors
        threads = []
        self.threadLock = Lock()
        if not self.root:
            threads.append(Thread(target=self.listenTCP, args=()))  # start listening to my parent
        threads.append(Thread(target=self.listenUDP, args=()))  # start listening to master
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def eightColoring(self, data):
        if int(data.decode()) % 2 != 0:
            color = self.color
            self.sendToChildren(color)
            if self.root:
                arg = f'next_{self.ID}'
                Thread(target=self.sendUDP, args=(arg,)).start()
        else:
            if self.root:
                self.threadLock.acquire()
                self.color = ''.zfill(2 + math.floor(math.log2(len(self.color) - 1)))
                self.threadLock.release()
            else:
                self.assignNewColor(self.parent_color)
            if len(self.color) < 4:
                self.threadLock.acquire()
                self.state = 'send_my_color'
                self.first_round = True
                self.eight_color = False
                self.threadLock.release()
            arg = f'next_{self.ID}'
            Thread(target=self.sendUDP, args=(arg,)).start()

    def threeColoring(self):
        if self.state == 'send_my_color':
            color = self.color + '_parent'
            self.sendToChildren(color)
            if self.root:
                arg = f'next_{self.ID}'
                Thread(target=self.sendUDP, args=(arg,)).start()

        elif self.state == 'send_parent_color':
            color = self.parent_color + '_grandparent'
            self.sendToChildren(color)
            if self.root:
                arg = f'next_{self.ID}'
                Thread(target=self.sendUDP, args=(arg,)).start()

        elif self.state == 'evaluate_3':
            if self.root:
                self.shiftDown()
            else:
                if self.parent_color != self.x:
                    self.threadLock.acquire()
                    self.color = self.parent_color
                    self.threadLock.release()
                else:
                    self.shiftDown()
            self.threadLock.acquire()
            self.x = self.colors3.pop()
            self.threadLock.release()
            if self.x == 'done':
                self.threadLock.acquire()
                self.shut_down_flag = True
                self.threadLock.release()
                self.sendToChildren('DIE')
                with open(f'color_vertex_{self.ID}.txt', "w") as f:
                    f.write(self.color[1:])
                arg = f'done_{self.ID}'
                Thread(target=self.sendUDP, args=(arg,)).start()

            else:
                arg = f'next_{self.ID}'
                Thread(target=self.sendUDP, args=(arg,)).start()

    def shiftDown(self):
        colors = ['000', '001', '010']
        if self.color in colors:
            colors.remove(self.color)
        if self.root:
            self.threadLock.acquire()
            self.color = list(colors)[0]
            self.threadLock.release()
        else:
            if self.grandparent_color in colors:
                colors.remove(self.grandparent_color)
            self.threadLock.acquire()
            self.color = list(colors)[0]
            self.threadLock.release()