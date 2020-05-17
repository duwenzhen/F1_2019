#! /usr/bin/env python3

"""This script captures F1 2019 telemetry packets (sent over UDP) and stores them into SQLite3 database files.

One database file will contain all packets from one session.

From UDP packet to database entry
---------------------------------

The data flow of UDP packets into the database is managed by 2 threads.

PacketReceiver thread:

  (1) The PacketReceiver thread does a select() to wait on incoming packets in the UDP socket.
  (2) When woken up with the notification that a UDP packet is available for reading, it is actually read from the socket.
  (3) The receiver thread calls the recorder_thread.record_packet() method with a TimedPacket containing
      the reception timestamp and the packet just read.
  (4) The recorder_thread.record_packet() method locks its packet queue, inserts the packet there,
      then unlocks the queue. Note that this method is only called from within the receiver thread!
  (5) repeat from (1).

PacketRecorder thread:

  (1) The PacketRecorder thread sleeps for a given period, then wakes up.
  (2) It locks its packet queue, moves the queue's packets to a local variable, empties the packet queue,
      then unlocks the packet queue.
  (3) The packets just moved out of the queue are passed to the 'process_incoming_packets' method.
  (4) The 'process_incoming_packets' method inspects the packet headers, and converts the packet data
      into SessionPacket instances that are suitable for inserting into the database.
      In the process, it collects packets from the same session. After collecting all
      available packets from the same session, it passed them on to the
      'process_incoming_same_session_packets' method.
  (5) The 'process_incoming_same_session_packets' method makes sure that the appropriate SQLite database file
      is opened (i.e., the one with matching sessionUID), then writes the packets into the 'packets' table.

By decoupling the packet capture and database writing in different threads, we minimize the risk of
dropping UDP packets. This risk is real because SQLite3 database commits can take a considerable time.
"""

import argparse
import sys
import time
import datetime
import socket
import sqlite3
import threading
import logging
import ctypes
import selectors
from influxdb import InfluxDBClient
import Game

from collections import namedtuple

from f1_2019_telemetry.cli.threading_utils import WaitConsoleThread, Barrier
from f1_2019_telemetry.packets import PacketHeader, PacketID, HeaderFieldsToPacketType, unpack_udp_packet, PacketLapData_V1, PacketMotionData_V1

# The type used by the PacketReceiverThread to represent incoming telemetry packets, with timestamp.
TimestampedPacket = namedtuple('TimestampedPacket', 'timestamp, packet')

# The type used by the PacketRecorderThread to represent incoming telemetry packets for storage in the SQLite3 database.
SessionPacket = namedtuple('SessionPacket', 'timestamp, packetFormat, gameMajorVersion, gameMinorVersion, packetVersion, packetId, sessionUID, sessionTime, frameIdentifier, playerCarIndex, packet')


class PacketRecorder:

    def __init__(self):
        self.game = Game.Game()
        self._open_database()

    def close(self):
        """Make sure that no database remains open."""
        if self.client is not None:
            self._close_database()

    def _open_database(self):
        self.client = InfluxDBClient(host='127.0.0.1', port=8086, username='admin', password='admin')
        #self.client.drop_database("F1_2019")
        #self.client.create_database("F1_2019")
        self.client.switch_database('F1_2019')
        logging.info("Opening influxdb")

    def _close_database(self):
        """Close SQLite3 database file."""
        logging.info("Closing influxdb")
        client = None

    '''
        MOTION        = 0
        SESSION       = 1
        LAP_DATA      = 2
        EVENT         = 3
        PARTICIPANTS  = 4  # 0.2 Hz (once every five seconds)
        CAR_SETUPS    = 5
        CAR_TELEMETRY = 6
        CAR_STATUS    = 7
    
    '''

    def formatLapJsonMessage(self, packet : PacketLapData_V1, time):
        json = []

        for i in range(20):
            dic = packet.header.fields
            dic["driver"] = i + 1
            json.append(
                {
                "measurement": "LapData",
                "tags": dic,
                "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": packet.lapData[i].fields
            }
            )
        return json




    def process_incoming_packets(self, timestamped_packets):
        """Process incoming packets by recording them into the correct database file.

        The incoming 'timestamped_packets' is a list of timestamped raw UDP packets.

        We process them to a variable 'same_session_packets', which is a list of consecutive
        packets having the same 'sessionUID' field. In this list, each packet is a 11-element tuple
        that can be inserted into the 'packets' table of the database.

        The 'same_session_packets' are then passed on to the '_process_same_session_packets'
        method that writes them into the appropriate database file.
        """

        t1 = time.monotonic()
        jsonMessages = {}
        for (timestamp, packet) in timestamped_packets:

            if len(packet) < ctypes.sizeof(PacketHeader):
                logging.error("Dropped bad packet of size {} (too short).".format(len(packet)))
                continue

            header = PacketHeader.from_buffer_copy(packet)

            packet_type_tuple = (header.packetFormat, header.packetVersion, header.packetId)

            packet_type = HeaderFieldsToPacketType.get(packet_type_tuple)
            if packet_type is None:
                logging.error("Dropped unrecognized packet (format, version, id) = {!r}.".format(packet_type_tuple))
                continue

            if len(packet) != ctypes.sizeof(packet_type):
                logging.error("Dropped packet with unexpected size; "
                              "(format, version, id) = {!r} packet, size = {}, expected {}.".format(
                                  packet_type_tuple, len(packet), ctypes.sizeof(packet_type)))
                continue

            unpacket = unpack_udp_packet(packet)

            if header.packetId == PacketID.MOTION:
                jsonMessages[PacketID.MOTION] = self.game.processMotion(unpacket, timestamp)
            elif header.packetId == PacketID.SESSION:
                jsonMessages[PacketID.SESSION] = self.game.processSession(unpacket, timestamp)
            elif header.packetId == PacketID.LAP_DATA:
                jsonMessages[PacketID.LAP_DATA] = self.game.processLap(unpacket, timestamp)
            elif header.packetId == PacketID.EVENT:
                jsonMessages[PacketID.EVENT] = self.game.processEvent(unpacket, timestamp)
            elif header.packetId == PacketID.PARTICIPANTS:
                jsonMessages[PacketID.PARTICIPANTS] = self.game.processParticipant(unpacket, timestamp)
            elif header.packetId == PacketID.CAR_SETUPS:
                jsonMessages[PacketID.CAR_STATUS] = self.game.processCarSetup(unpacket, timestamp)
            elif header.packetId == PacketID.CAR_STATUS:
                jsonMessages[PacketID.CAR_STATUS] = self.game.processCarStatus(unpacket, timestamp)
            elif header.packetId == PacketID.CAR_TELEMETRY:  # Log Event packets
                jsonMessages[PacketID.CAR_TELEMETRY] = self.game.processCarTelemetry(unpacket, timestamp)

        t = 0
        for k, v in jsonMessages.items():
            self.client.write_points(v)
            t = t + len(v)
        print("To insert messages number: " + str(t))

        t2 = time.monotonic()

        duration = (t2 - t1)

        logging.info("Recorded {} packets in {:.3f} ms.".format(len(timestamped_packets), duration * 1000.0))

    def no_packets_received(self, age: float) -> None:
        logging.info("No packets to record for")



class PacketRecorderThread(threading.Thread):
    """The PacketRecorderThread writes telemetry data to SQLite3 files."""

    def __init__(self, record_interval):
        super().__init__(name='recorder')
        self._record_interval = record_interval
        self._packets = []
        self._packets_lock = threading.Lock()
        self._socketpair = socket.socketpair()

    def close(self):
        for sock in self._socketpair:
            sock.close()

    def run(self):
        """Receive incoming packets and hand them over the the PacketRecorder.

        This method runs in its own thread.
        """

        selector = selectors.DefaultSelector()
        key_socketpair = selector.register(self._socketpair[0], selectors.EVENT_READ)

        recorder = PacketRecorder()

        packets = []

        logging.info("Recorder thread started.")

        quitflag = False
        inactivity_timer = datetime.datetime.utcnow()
        while not quitflag:

            # Calculate the timeout value that will bring us in sync with the next period.
            timeout = (-time.time()) % self._record_interval
            # If the timeout interval is too short, increase its length by 1 period.
            if timeout < 0.5 * self._record_interval:
                timeout += self._record_interval

            for (key, events) in selector.select(timeout):
                if key == key_socketpair:
                    quitflag = True

            # Swap packets, so the 'record_packet' method can be called uninhibited as soon as possible.
            with self._packets_lock:
                (packets, self._packets) = (self._packets, packets)

            if len(packets) != 0:
                inactivity_timer = packets[-1].timestamp
                recorder.process_incoming_packets(packets)
                packets.clear()
            else:
                t_now = datetime.datetime.utcnow()
                age = t_now - inactivity_timer
                recorder.no_packets_received(age)
                inactivity_timer = t_now

        recorder.close()

        selector.close()

        logging.info("Recorder thread stopped.")

    def request_quit(self):
        """Request termination of the PacketRecorderThread.

        Called from the main thread to request that we quit.
        """
        self._socketpair[1].send(b'\x00')

    def record_packet(self, timestamped_packet):
        """Called from the receiver thread for every UDP packet received."""
        with self._packets_lock:
            self._packets.append(timestamped_packet)


class PacketReceiverThread(threading.Thread):
    """The PacketReceiverThread receives incoming telemetry packets via the network and passes them to the PacketRecorderThread for storage."""

    def __init__(self, udp_port, recorder_thread):
        super().__init__(name='receiver')
        self._udp_port = udp_port
        self._recorder_thread = recorder_thread
        self._socketpair = socket.socketpair()

    def close(self):
        for sock in self._socketpair:
            sock.close()

    def run(self):
        """Receive incoming packets and hand them over to the PacketRecorderThread.

        This method runs in its own thread.
        """

        udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

        # Allow multiple receiving endpoints.
        if sys.platform in ['darwin']:
            udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        elif sys.platform in ['linux', 'win32']:
            udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Accept UDP packets from any host.
        address = ('', self._udp_port)
        udp_socket.bind(address)

        selector = selectors.DefaultSelector()

        key_udp_socket = selector.register(udp_socket, selectors.EVENT_READ)
        key_socketpair = selector.register(self._socketpair[0], selectors.EVENT_READ)

        logging.info("Receiver thread started, reading UDP packets from port {}.".format(self._udp_port))

        quitflag = False
        while not quitflag:
            for (key, events) in selector.select():
                timestamp = datetime.datetime.utcnow()
                if key == key_udp_socket:
                    # All telemetry UDP packets fit in 2048 bytes with room to spare.
                    packet = udp_socket.recv(2048)
                    timestamped_packet = TimestampedPacket(timestamp, packet)
                    self._recorder_thread.record_packet(timestamped_packet)
                elif key == key_socketpair:
                    quitflag = True

        selector.close()
        udp_socket.close()
        for sock in self._socketpair:
            sock.close()

        logging.info("Receiver thread stopped.")

    def request_quit(self):
        """Request termination of the PacketReceiverThread.

        Called from the main thread to request that we quit.
        """
        self._socketpair[1].send(b'\x00')


def main():
    """Record incoming telemetry data until the user presses enter."""

    # Configure logging.

    logging.basicConfig(level=logging.DEBUG, format="%(asctime)-23s | %(threadName)-10s | %(levelname)-5s | %(message)s")
    logging.Formatter.default_msec_format = '%s.%03d'

    # Parse command line arguments.

    parser = argparse.ArgumentParser(description="Record F1 2019 telemetry data to SQLite3 files.")

    parser.add_argument("-p", "--port", default=20777, type=int, help="UDP port to listen to (default: 20777)", dest='port')
    parser.add_argument("-i", "--interval", default=1.0, type=float, help="interval for writing incoming data to SQLite3 file, in seconds (default: 1.0)", dest='interval')

    args = parser.parse_args()

    # Start recorder thread first, then receiver thread.

    quit_barrier = Barrier()

    recorder_thread = PacketRecorderThread(args.interval)
    recorder_thread.start()

    receiver_thread = PacketReceiverThread(args.port, recorder_thread)
    receiver_thread.start()

    wait_console_thread = WaitConsoleThread(quit_barrier)
    wait_console_thread.start()

    # Recorder, receiver, and wait_console threads are now active. Run until we're asked to quit.

    quit_barrier.wait()

    # Stop threads.

    wait_console_thread.request_quit()
    wait_console_thread.join()
    wait_console_thread.close()

    receiver_thread.request_quit()
    receiver_thread.join()
    receiver_thread.close()

    recorder_thread.request_quit()
    recorder_thread.join()
    recorder_thread.close()

    # All done.

    logging.info("All done.")


if __name__ == "__main__":
    main()