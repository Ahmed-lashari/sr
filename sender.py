"""Selective Repeat sender with a per-packet timer."""
import argparse, socket, threading

FIN = "__FIN__"


class Sender:
    def __init__(self, chan_port, rcv_port, N, timeout):
        self.N, self.timeout = N, timeout
        self.send_base = 0
        self.next_seq = 0
        self.acked = set()
        self.timers = {}
        self.packets = []
        self.cond = threading.Condition()
        self.retx = 0
        self.chan = socket.create_connection(("127.0.0.1", chan_port))
        self.ack_sock = socket.create_connection(("127.0.0.1", rcv_port))
        self.ack_sock.sendall(b"ACK\n")      # ACKs come straight back from receiver
        self.send_lock = threading.Lock()
        threading.Thread(target=self.ack_listener, daemon=True).start()

    def transmit(self, seq):
        with self.send_lock:
            self.chan.sendall(f"{seq}|{self.packets[seq]}\n".encode())

    def start_timer(self, seq):
        t = threading.Timer(self.timeout, self.on_timeout, args=(seq,))
        t.daemon = True
        self.timers[seq] = t
        t.start()

    def on_timeout(self, seq):
        with self.cond:
            if seq in self.acked or seq < self.send_base:
                return
            self.retx += 1
            print(f"[SND] TIMEOUT seq={seq} -> retransmit", flush=True)
            self.transmit(seq)
            self.start_timer(seq)

    def ack_listener(self):
        for line in self.ack_sock.makefile("r"):
            seq = int(line.split()[1])
            with self.cond:
                if self.send_base <= seq < self.send_base + self.N and seq not in self.acked:
                    self.acked.add(seq)
                    t = self.timers.pop(seq, None)
                    if t: t.cancel()
                    while self.send_base in self.acked:   # slide window
                        self.send_base += 1
                    print(f"[SND] ACK {seq}, send_base={self.send_base}", flush=True)
                    self.cond.notify_all()

    def send_all(self, messages):
        self.packets = list(messages) + [FIN]
        total = len(self.packets)
        with self.cond:
            while self.send_base < total:
                while self.next_seq < min(self.send_base + self.N, total):
                    s = self.next_seq
                    print(f"[SND] send seq={s} window=[{self.send_base},{self.send_base+self.N-1}]", flush=True)
                    self.transmit(s)
                    self.start_timer(s)
                    self.next_seq += 1
                self.cond.wait(0.1)
        print(f"[SND] all {total} packets ACKed, retransmissions={self.retx}", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--chan-port", type=int, default=9000)
    p.add_argument("--rcv-port", type=int, default=9001)
    p.add_argument("-N", type=int, default=4)
    p.add_argument("--timeout", type=float, default=0.6)
    p.add_argument("--count", type=int, default=15)
    a = p.parse_args()
    Sender(a.chan_port, a.rcv_port, a.N, a.timeout).send_all([f"msg{i}" for i in range(a.count)])
