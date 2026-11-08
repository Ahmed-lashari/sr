"""Selective Repeat receiver (Assignment IV).

One listening port serves two kinds of connections, identified by a first line:
  "DATA"  -> opened by the channel; carries "seq|payload\n" packets
  "ACK"   -> opened directly by the sender; receiver writes "ACK <seq>\n" on it
"""
import argparse, socket, threading, time

FIN = "__FIN__"


def process_packet(seq_num, rcv_base, N, buffer):
    """Assignment-II logic. Returns the ACK number or None. Mutates buffer."""
    if rcv_base <= seq_num < rcv_base + N:          # in-window
        if seq_num not in buffer:                   # keep one copy only
            buffer.append(seq_num)
        return seq_num
    if rcv_base - N <= seq_num < rcv_base:          # old / duplicate
        return seq_num                              # re-ACK, buffer untouched
    return None                                     # out of window: ignore


class Receiver:
    def __init__(self, port, N, linger=2.0):
        self.N, self.port, self.linger = N, port, linger
        self.rcv_base, self.buffer, self.payloads = 0, [], {}
        self.delivered = []
        self.lock = threading.Lock()
        self.ack_conn = None
        self.ack_ready = threading.Event()
        self.done = threading.Event()

    def deliver_in_order(self):
        """Slide rcv_base over the contiguous run of buffered packets."""
        while self.rcv_base in self.buffer:
            self.buffer.remove(self.rcv_base)
            data = self.payloads.pop(self.rcv_base)
            print(f"[RCV] deliver seq={self.rcv_base} data={data!r}", flush=True)
            if data == FIN:
                self.done.set()
            else:
                self.delivered.append(data)
            self.rcv_base += 1

    def handle_data(self, conn):
        f = conn.makefile("r")
        for line in f:
            line = line.strip()
            if "|" not in line:
                continue
            seq_s, payload = line.split("|", 1)
            seq = int(seq_s)
            with self.lock:
                ack = process_packet(seq, self.rcv_base, self.N, self.buffer)
                if ack is None:
                    print(f"[RCV] seq={seq} out of window (base={self.rcv_base}) ignored", flush=True)
                    continue
                if seq >= self.rcv_base:
                    self.payloads[seq] = payload
                    print(f"[RCV] got seq={seq}, buffer={sorted(self.buffer)}", flush=True)
                else:
                    print(f"[RCV] duplicate seq={seq}, re-ACK", flush=True)
                self.deliver_in_order()
                self.ack_ready.wait()
                self.ack_conn.sendall(f"ACK {ack}\n".encode())
                print(f"[RCV] sent ACK {ack} (rcv_base={self.rcv_base})", flush=True)

    def handle_client(self, conn):
        role = conn.makefile("r").readline().strip()
        if role == "ACK":
            self.ack_conn = conn
            self.ack_ready.set()
        elif role == "DATA":
            self.handle_data(conn)

    def run(self):
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", self.port)); srv.listen(5)
        print(f"[RCV] listening on {self.port}, N={self.N}", flush=True)
        threading.Thread(target=self._accept, args=(srv,), daemon=True).start()
        self.done.wait()
        time.sleep(self.linger)   # keep ACKing retransmitted FINs
        print(f"[RCV] finished. Delivered {len(self.delivered)} messages in order: {self.delivered}")

    def _accept(self, srv):
        while True:
            c, _ = srv.accept()
            threading.Thread(target=self.handle_client, args=(c,), daemon=True).start()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=9001)
    p.add_argument("-N", type=int, default=4)
    a = p.parse_args()
    Receiver(a.port, a.N).run()
