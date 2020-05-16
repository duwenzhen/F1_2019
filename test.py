import socket
import json

from f1_2019_telemetry.packets import unpack_udp_packet

udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
udp_socket.bind(('', 20777))

while True:
    udp_packet = udp_socket.recv(2048)
    packet = unpack_udp_packet(udp_packet)
    print("Received:", packet._to_dict())
    print()