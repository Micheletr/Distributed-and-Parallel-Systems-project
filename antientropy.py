#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
Anti-Entropy Push-Pull — implementazione di un nodo P2P con ZeroMQ
═══════════════════════════════════════════════════════════════════════════════
"""

import zmq
import time
import json
import os
import random
import argparse
import threading
import logging

# ─── CONFIGURAZIONE RETE ──────────────────────────────────────────────────────

GRID_SIZE   = 3           
BASE_PORT   = 5550        
ROUND_INTERVAL = 2.0      
FANOUT      = 1           
REQUEST_TIMEOUT_MS = 800  
PACKET_LOSS_RATE = 0.2    

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [nodo-%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

# ─────────────────────────────────────────────────────────────────────────────
# STRUTTURA DATI: KEY-VALUE STORE
# ─────────────────────────────────────────────────────────────────────────────

class KVStore:
    def __init__(self, initial_data: dict = None):
        self.data = initial_data if initial_data is not None else {}

    def to_dict(self) -> dict:
        return {"data": self.data}

    @staticmethod
    def from_dict(d: dict) -> "KVStore":
        return KVStore(initial_data=d.get("data", {}))

    def merge_with(self, remote_store: "KVStore") -> bool:
        updated = False
        for key, remote_record in remote_store.data.items():
            local_record = self.data.get(key)
            if local_record is None or remote_record["timestamp"] > local_record["timestamp"]:
                self.data[key] = remote_record
                updated = True
        return updated

# ─────────────────────────────────────────────────────────────────────────────
# OVERLAY NETWORK — CALCOLO VICINI
# ─────────────────────────────────────────────────────────────────────────────

def get_neighbors(node_id: int) -> list[int]:
    row, col = divmod(node_id, GRID_SIZE)
    neighbors = []
    if row > 0:              neighbors.append((row - 1) * GRID_SIZE + col)
    if row < GRID_SIZE - 1:  neighbors.append((row + 1) * GRID_SIZE + col)
    if col > 0:              neighbors.append(row * GRID_SIZE + col - 1)
    if col < GRID_SIZE - 1:  neighbors.append(row * GRID_SIZE + col + 1)
    return neighbors

def node_port(node_id: int) -> int:
    return BASE_PORT + node_id

def node_address(node_id: int) -> str:
    return f"tcp://127.0.0.1:{node_port(node_id)}"

# ─────────────────────────────────────────────────────────────────────────────
# PROTOCOLLO
# ─────────────────────────────────────────────────────────────────────────────

MSG_PUSH_PULL_REQ = "PUSH_PULL_REQ"
MSG_PUSH_PULL_REP = "PUSH_PULL_REP"

def encode(msg_type: str, store: KVStore) -> bytes:
    payload = {"type": msg_type, "store": store.to_dict()}
    return json.dumps(payload).encode()

def decode(raw: bytes) -> tuple[str, KVStore]:
    payload = json.loads(raw.decode())
    return payload["type"], KVStore.from_dict(payload["store"])

# ─────────────────────────────────────────────────────────────────────────────
# CLASSE NODO
# ─────────────────────────────────────────────────────────────────────────────

class KVStoreNode:
    def __init__(self, node_id: int, initial_store: KVStore):
        self.node_id   = node_id
        self.neighbors = get_neighbors(node_id)
        self.log       = logging.getLogger(str(node_id))

        self._lock  = threading.Lock()
        self._store = initial_store

        self._round        = 0
        self._msgs_sent    = 0
        self._msgs_recv    = 0

        self._ctx = zmq.Context()
        self._stop_event = threading.Event()  # Aggiunto per evitare il crash del loop

    @property
    def store(self) -> KVStore:
        with self._lock:
            safe_data = {k: v.copy() for k, v in self._store.data.items()}
            return KVStore(initial_data=safe_data)

    def _merge(self, remote_store: KVStore) -> bool:
        with self._lock:
            updated = self._store.merge_with(remote_store)
            if updated:
                # Log pulito anche qui
                stato_pulito = {k: v["value"] for k, v in self._store.data.items()}
                self.log.info("AGGIORNAMENTO STORE: %s", stato_pulito)
            return updated

    def _server_thread(self):
        sock = self._ctx.socket(zmq.REP)
        sock.bind(f"tcp://0.0.0.0:{node_port(self.node_id)}")
        self.log.info("Server in ascolto su porta %d", node_port(self.node_id))

        while True:
            try:
                raw = sock.recv()
                msg_type, remote_store = decode(raw)
                self._msgs_recv += 1

                if msg_type == MSG_PUSH_PULL_REQ:
                    self._merge(remote_store)
                    sock.send(encode(MSG_PUSH_PULL_REP, self.store))
                    self._msgs_sent += 1
                else:
                    sock.send(encode(MSG_PUSH_PULL_REP, self.store))

            except zmq.ZMQError as e:
                self.log.error("Errore ZMQ nel server: %s", e)
                break
            except Exception as e:
                self.log.warning("Errore nel server: %s", e)
                try:
                    sock.send(encode(MSG_PUSH_PULL_REP, self.store))
                except Exception:
                    pass

    def _client_api_thread(self):
        sock = self._ctx.socket(zmq.REP)
        client_port = node_port(self.node_id) + 1000
        sock.bind(f"tcp://0.0.0.0:{client_port}")
        
        while True:
            try:
                msg = sock.recv_json()
                
                if msg.get("action") == "PUT":
                    key = msg["key"]
                    value = msg["value"]
                    
                    with self._lock:
                        self._store.data[key] = {"value": value, "timestamp": time.time()}
                        self.log.info("[PUT ESTERNA] Inserita chiave '%s' = '%s'", key, value)
                        
                    sock.send_string("OK - Dato inserito con successo")
                
                elif msg.get("action") == "CRASH":
                    self.log.critical("[CRASH] Ricevuto comando di terminazione forzata.")
                    sock.send_string("ACK - Nodo in distruzione")
                    os._exit(1)
                    
            except zmq.ZMQError:
                break

    def _do_push_pull(self, neighbor_id: int):
        if random.random() < PACKET_LOSS_RATE:
            self.log.warning("[PACKET LOSS] Scartato pacchetto verso nodo %d", neighbor_id)
            return
        
        sock = self._ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, REQUEST_TIMEOUT_MS)
        sock.connect(node_address(neighbor_id))

        try:
            sock.send(encode(MSG_PUSH_PULL_REQ, self.store))
            self._msgs_sent += 1

            raw = sock.recv()
            msg_type, remote_store = decode(raw)
            self._msgs_recv += 1

            if msg_type == MSG_PUSH_PULL_REP:
                self._merge(remote_store)

        except zmq.Again:
            self.log.warning("Timeout: vicino %d non raggiungibile (ci riproveremo)", neighbor_id)
        except Exception as e:
            self.log.error("Errore durante push-pull con nodo %d: %s", neighbor_id, e)
        finally:
            sock.close()

    def run(self):
        server = threading.Thread(target=self._server_thread, daemon=True)
        server.start()

        client_api = threading.Thread(target=self._client_api_thread, daemon=True)
        client_api.start()

        # Estraiamo i dati puliti (solo valore, niente timestamp) in modo thread-safe
        stato_iniziale = {k: v["value"] for k, v in self.store.data.items()}
        self.log.info(
            "Nodo avviato | posizione: %s | stato iniziale: %s",
            divmod(self.node_id, GRID_SIZE),
            stato_iniziale
        )

        time.sleep(1.5)

        try:
            while not self._stop_event.is_set():
                self._round += 1
                vicini = get_neighbors(self.node_id)
                vicini_scelti = random.sample(vicini, min(FANOUT, len(vicini)))
                
                # Creiamo la vista pulita per il log di questo round
                stato_pulito = {k: v["value"] for k, v in self.store.data.items()}
                
                # Rimessi a livello INFO, ma formattati puliti
                self.log.info("── round %d  |  contatto: %s  |  stato: %s", self._round, vicini_scelti, stato_pulito)
                
                for v_id in vicini_scelti:
                    self._do_push_pull(v_id)
                
                self.log.info("   msg inviati: %d  |  msg ricevuti: %d", self._msgs_sent, self._msgs_recv)
                time.sleep(ROUND_INTERVAL)
                
        except KeyboardInterrupt:
            pass
        finally:
            stato_finale = {k: v["value"] for k, v in self.store.data.items()}
            self.log.info(
                "Terminazione | round completati: %d | stato finale: %s",
                self._round, stato_finale
            )
            self._ctx.term()

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Nodo anti-entropy push-pull")
    parser.add_argument("--id", type=int, default=0, help="id del nodo")
    parser.add_argument("--seed-value", type=str, default=None, help="valore iniziale")
    parser.add_argument("--demo", action="store_true", help="avvia tutti i nodi")
    
    parser.add_argument(
        "--put", nargs=3, metavar=("NODE_ID", "KEY", "VALUE"),
        help="Invia un nuovo dato a un nodo. Es: --put 0 nome Michele"
    )
    parser.add_argument(
        "--kill", type=int, metavar="NODE_ID",
        help="Provoca il crash istantaneo di un nodo. Es: --kill 4"
    )
    
    args = parser.parse_args()

    if args.put:
        target_id, key, value = int(args.put[0]), args.put[1], args.put[2]
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REQ)
        sock.connect(f"tcp://127.0.0.1:{BASE_PORT + target_id + 1000}")
        sock.send_json({"action": "PUT", "key": key, "value": value})
        print(f"Risposta dal nodo {target_id}: {sock.recv_string()}")
        return
    
    if args.kill is not None:
        target_id = args.kill
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REQ)
        sock.connect(f"tcp://127.0.0.1:{BASE_PORT + target_id + 1000}")
        sock.send_json({"action": "CRASH"})
        print(f"Risposta dal nodo {target_id}: {sock.recv_string()}")
        return
    
    if args.demo:
        _run_demo()
        return

    if args.seed_value:
        initial_data = {"chiave_iniziale": {"value": args.seed_value, "timestamp": time.time()}}
        store = KVStore(initial_data=initial_data)
    else:
        store = KVStore()

    node = KVStoreNode(node_id=args.id, initial_store=store)
    node.run()

def _run_demo():
    import subprocess
    import sys

    n = GRID_SIZE * GRID_SIZE
    seed_id = n // 2  
    procs = []

    print(f"\n Demo anti-entropy push-pull — griglia {GRID_SIZE}×{GRID_SIZE} ({n} nodi)")
    print(f" Nodo seme: id={seed_id}  →  posizione {divmod(seed_id, GRID_SIZE)}")
    print(f" Round interval: {ROUND_INTERVAL}s  |  fanout: {FANOUT}")
    print(f" Premi Ctrl+C per terminare tutti i nodi.\n")
    print("─" * 60)

    for i in range(n):
        cmd = [sys.executable, __file__, "--id", str(i)]
        if i == seed_id:
            cmd += ["--seed-value", "broadcast_v1"]
        p = subprocess.Popen(cmd)
        procs.append(p)
        time.sleep(0.05)  

    try:
        for p in procs:
            p.wait()
    except KeyboardInterrupt:
        print("\n\nTerminazione di tutti i nodi...")
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait()
        print("Demo terminata.")

if __name__ == "__main__":
    main()