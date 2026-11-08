# Assignment IV – Full Selective Repeat Simulation

## Architecture

```
sender.py --(data, TCP)--> channel.py --(drop/delay)--> receiver.py
    ^                                                      |
    +------------------ ACKs (direct TCP) -----------------+
```

Run (three terminals, in this order):

```
python receiver.py -N 4
python channel.py --loss 0.3 --dmax 0.3 [--seed 1]
python sender.py -N 4 --timeout 0.6 --count 15
```

Video Demo Link: ![https://youtu.be/6geZxhP8BDQ]
Messages are newline-delimited text: `seq|payload` for data, `ACK <seq>` for ACKs.

## Receiver (`receiver.py`)

A single server port accepts two connection types, identified by a first line: `DATA` (opened by the channel) and `ACK` (opened directly by the sender). ACKs therefore travel straight to the sender, as specified, and only the data direction is unreliable.
`process_packet(seq, rcv_base, N, buffer)` is the Assignment II function:

- **In-window** (`rcv_base <= seq < rcv_base+N`): add to buffer (once) and return `seq`.
- **Old/duplicate** (`rcv_base-N <= seq < rcv_base`): return `seq` (re-ACK), buffer unchanged. This is essential: if an ACK is lost, the sender keeps retransmitting until it gets a re-ACK.
- **Otherwise**: return `None`, no ACK.

After each packet, `deliver_in_order()` delivers the contiguous run starting at `rcv_base`, removes those packets from the buffer and advances `rcv_base`. Delivery is immediate, rather than waiting for the buffer to fill as in Assignment II. Waiting would stall the final partial window, and standard SR delivers as soon as the gap closes. A mutex serialises state changes, since each connection runs in its own thread.

## Unreliable channel (`channel.py`)

It has two sockets: a server socket facing the sender and a client socket facing the receiver. For every packet it draws a random number: with probability `loss` the packet is dropped; otherwise it is forwarded after a random delay in `[dmin, dmax]` via `threading.Timer`. Because delays are independent, packets can overtake one another, so **reordering** is simulated as well as loss. A lock protects the forwarding socket from concurrent timer threads.

## Sender (`sender.py`)

`Sender` keeps `send_base`, `next_seq`, a set of ACKed sequence numbers and **one timer per outstanding packet** (`threading.Timer`). SR needs per-packet timers because only the unACKed packet is retransmitted, unlike Go-Back-N.

- **Sending:** while `next_seq < send_base + N`, transmit and start that packet's timer.
- **ACK thread:** an ACK inside the window is recorded and its timer cancelled. If it equals `send_base`, the window slides past all consecutively ACKed packets and the main loop is woken via a `Condition`.
- **Timeout:** retransmit only that packet and restart its timer.
- **Termination:** a final `__FIN__` packet is sent through the same reliable mechanism. The receiver stops after delivering it, with a 2 s linger so that retransmitted FINs still get ACKed if the last ACK was lost.

The timeout (0.6 s) is set above the maximum channel delay (0.3 s) plus processing time, so most timeouts come from real loss, not slow packets.

## Design notes / limitations

- Sequence numbers are unbounded integers (no wrap-around). The receiver window equals the sender window `N`; with modulo numbering the sequence space would have to be at least `2N`.
- TCP is used between components because the assignment specifies streams; loss exists only at the application level, inside the channel.
- Test: 15 messages, N=4, 30% loss, random delay 0–0.3 s. All messages were delivered in order and exactly once; the sender needed 13 retransmissions.
