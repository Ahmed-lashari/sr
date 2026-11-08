"""Unreliable channel: sender -> (drop/delay) -> receiver."""
import argparse, random, socket, threading


class Channel:
    def __init__(self, listen_port, rcv_port, loss, dmin, dmax, seed=None):
        self.listen_port, self.rcv_port = listen_port, rcv_port
        self.loss, self.dmin, self.dmax = loss, dmin, dmax
        self.rng = random.Random(seed)
        self.send_lock = threading.Lock()

    def forward(self, line):
        with self.send_lock:
            self.rcv_sock.sendall(line.encode())

    def run(self):
        # socket 1: towards the receiver
        self.rcv_sock = socket.create_connection(("127.0.0.1", self.rcv_port))
        self.rcv_sock.sendall(b"DATA\n")
        # socket 2: server socket for the sender
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", self.listen_port)); srv.listen(1)
        print(f"[CHN] listening on {self.listen_port} -> receiver {self.rcv_port}, loss={self.loss}", flush=True)
        conn, _ = srv.accept()
        for line in conn.makefile("r"):
            seq = line.split("|", 1)[0]
            if self.rng.random() < self.loss:
                print(f"[CHN] DROP  seq={seq}", flush=True)
                continue
            d = self.rng.uniform(self.dmin, self.dmax)   # random delay => reordering
            print(f"[CHN] delay seq={seq} by {d:.2f}s", flush=True)
            threading.Timer(d, self.forward, args=(line,)).start()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--rcv-port", type=int, default=9001)
    p.add_argument("--loss", type=float, default=0.3)
    p.add_argument("--dmin", type=float, default=0.0)
    p.add_argument("--dmax", type=float, default=0.3)
    p.add_argument("--seed", type=int, default=None)
    a = p.parse_args()
    Channel(a.port, a.rcv_port, a.loss, a.dmin, a.dmax, a.seed).run()
