import asyncio
import math
import queue
import socket
import threading
import time
from datetime import datetime
import tkinter as tk
from tkinter import ttk
import blickfeld_qb2

# ==========================================
# --- CONFIGURATION ---
# ==========================================
TARGET_ZONE = "Security Zone 1"
COOLDOWN_SECONDS = 5
ALARM_HOLD_SECONDS = 1.5
UI_REFRESH_RATE_SEC = 0.2

LIDAR_IP = "192.168.26.26"
API_KEY = "2ee812bc2e745dddb8i1cmJwrEaz8ehy"
PROBE_PORT = 50051  # Probes the sensor's hardware gRPC port for clean wire RTT

class GrpcTrueWireDashboard:
    def __init__(self, root):
        self.root = root
        self.root.title("Blickfeld: gRPC Edge-AI Monitor (Empirical TCP Probe + Advanced Telemetry)")
        self.root.geometry("980x650")
        self.root.configure(bg="#1e1e2e")

        self.packet_queue = queue.Queue()
        self.last_log_time = 0
        self.alarm_active_until = 0
        self.last_ui_paint = 0
        self.cached_subjects = []
        self.tree_items = {}

        self.live_wire_latency_ms = 0.000 
        self.target_fps = None
        self.last_proc_time = 0.0

        # --- ADVANCED BENCHMARKING STATE ---
        self.bench_active = False
        self.bench_end_time = 0.0
        self.bench_tot_latencies = []
        self.bench_net_latencies = []
        self.bench_bytes_received = 0
        self.bench_frame_count = 0
        self.bench_skipped_frames = 0
        self.last_sensor_epoch = 0.0

        self.root.rowconfigure(2, weight=1)
        self.root.rowconfigure(3, weight=1)
        self.root.columnconfigure(0, weight=1)

        self._build_ui()

        threading.Thread(target=self._grpc_objects_producer, daemon=True).start()
        threading.Thread(target=self._wire_latency_probe, daemon=True).start()

        self.root.after(100, self._ui_consumer_tick)

    def _build_ui(self):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background="#252538", foreground="white", fieldbackground="#252538", rowheight=26)
        style.configure("Treeview.Heading", background="#32324d", foreground="white", relief="flat")

        ctrl_frame = tk.Frame(self.root, bg="#252538", pady=8, padx=15)
        ctrl_frame.grid(row=0, column=0, sticky="ew", padx=15, pady=(10, 0))
        
        tk.Label(ctrl_frame, text="⏱ Dur (s):", font=("Arial", 10, "bold"), bg="#252538", fg="white").pack(side=tk.LEFT, padx=2)
        self.dur_entry = tk.Entry(ctrl_frame, width=4, font=("Consolas", 11, "bold"), bg="#181825", fg="#89b4fa", insertbackground="white")
        self.dur_entry.insert(0, "10")
        self.dur_entry.pack(side=tk.LEFT, padx=2)

        self.start_bench_btn = tk.Button(ctrl_frame, text="▶ START BENCHMARK", font=("Arial", 10, "bold"), bg="#89b4fa", fg="#11111b", command=self._start_timed_benchmark, relief="flat")
        self.start_bench_btn.pack(side=tk.LEFT, padx=10)

        tk.Label(ctrl_frame, text="⚙️ Client Rate Limit (FPS):", font=("Arial", 10, "bold"), bg="#252538", fg="white").pack(side=tk.LEFT, padx=(10, 2))
        self.fps_entry = tk.Entry(ctrl_frame, width=5, font=("Consolas", 11, "bold"), bg="#181825", fg="#89b4fa", insertbackground="white")
        self.fps_entry.pack(side=tk.LEFT, padx=5)
        
        self.wire_status_lbl = tk.Label(ctrl_frame, text="📡 Wire Speed: Probing...", font=("Consolas", 10, "bold"), bg="#252538", fg="#f9e2af")
        self.wire_status_lbl.pack(side=tk.RIGHT, padx=10)

        self.status_frame = tk.Frame(self.root, bg="#a6e3a1", height=60)
        self.status_frame.grid(row=1, column=0, sticky="ew", padx=15, pady=10)
        self.status_frame.grid_propagate(False)
        self.status_label = tk.Label(self.status_frame, text="SYSTEM SECURED - NO INTRUSIONS", font=("Arial", 15, "bold"), bg="#a6e3a1", fg="#11111b")
        self.status_label.pack(expand=True)

        subjects_frame = tk.LabelFrame(self.root, text=f" gRPC Tracked Subjects inside '{TARGET_ZONE}' ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        subjects_frame.grid(row=2, column=0, sticky="nsew", padx=15, pady=5)
        subjects_frame.rowconfigure(0, weight=1)
        subjects_frame.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(subjects_frame, columns=("objectID", "objType", "pos", "speed"), show="headings")
        self.tree.heading("objectID", text="objectID")
        self.tree.heading("objType", text="objType")
        self.tree.heading("pos", text="pos [X, Y, Z]")
        self.tree.heading("speed", text="speed (mph)")
        self.tree.column("objectID", anchor="center", width=100)
        self.tree.column("objType", anchor="center", width=180)
        self.tree.column("pos", anchor="center", width=180)
        self.tree.column("speed", anchor="center", width=100)
        self.tree.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

        tree_scroll = ttk.Scrollbar(subjects_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_scroll.set)
        tree_scroll.grid(row=0, column=1, sticky="ns")

        perf_frame = tk.LabelFrame(self.root, text=" Telemetry & Advanced Network Log ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        perf_frame.grid(row=3, column=0, sticky="nsew", padx=15, pady=10)
        perf_frame.rowconfigure(0, weight=1)
        perf_frame.columnconfigure(0, weight=1)

        self.log_widget = tk.Text(perf_frame, bg="#181825", fg="#bac2de", font=("Consolas", 10), state="disabled", wrap="word")
        self.log_widget.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        scroll = ttk.Scrollbar(perf_frame, orient="vertical", command=self.log_widget.yview)
        self.log_widget.configure(yscrollcommand=scroll.set)
        scroll.grid(row=0, column=1, sticky="ns")

    def log_message(self, msg):
        self.log_widget.configure(state="normal")
        self.log_widget.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}\n")
        self.log_widget.see(tk.END)
        self.log_widget.configure(state="disabled")

    def _start_timed_benchmark(self):
        if self.bench_active: return
        try: dur = float(self.dur_entry.get())
        except ValueError: return

        self.bench_active = True
        self.bench_end_time = time.time() + dur
        self.bench_tot_latencies, self.bench_net_latencies = [], []
        self.bench_bytes_received, self.bench_frame_count, self.bench_skipped_frames = 0, 0, 0
        self.last_sensor_epoch = 0.0
        
        self.start_bench_btn.configure(state="disabled", text="⏳ RECORDING...", bg="#f38ba8")
        self.log_message(f"\n=== STARTING {dur}s ADVANCED gRPC EDGE-AI BENCHMARK ===")

    def _calc_jitter(self, latencies):
        if len(latencies) < 2: return 0.0
        avg = sum(latencies) / len(latencies)
        variance = sum((x - avg) ** 2 for x in latencies) / len(latencies)
        return math.sqrt(variance)

    def _wire_latency_probe(self):
        while True:
            try:
                start_t = time.perf_counter()
                with socket.create_connection((LIDAR_IP, PROBE_PORT), timeout=1.0): pass
                rtt_ms = (time.perf_counter() - start_t) * 1000
                self.live_wire_latency_ms = rtt_ms / 2.0
                self.wire_status_lbl.configure(text=f"📡 Live Wire Speed: {self.live_wire_latency_ms:.3f} ms", fg="#a6e3a1")
            except Exception:
                self.wire_status_lbl.configure(text="📡 Wire Speed: Probe Timeout", fg="#f38ba8")
            time.sleep(1.0)

    def _grpc_objects_producer(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            token_factory = blickfeld_qb2.TokenFactory(application_key_secret=API_KEY)
            with blickfeld_qb2.Channel(fqdn_or_ip=LIDAR_IP, token=token_factory) as channel:
                service = blickfeld_qb2.percept_processing.services.Objects(channel)
                self.packet_queue.put(("LOG", "ONLINE: Connected to gRPC Edge Objects stream"))
                for response in service.stream():
                    now = time.time()
                    if self.target_fps and self.target_fps > 0:
                        if (now - self.last_proc_time) < (1.0 / self.target_fps): continue
                    self.last_proc_time = now
                    payload_dict = response.to_dict()
                    byte_size = len(str(payload_dict).encode('utf-8'))
                    self.packet_queue.put(("OBJECTS", now, payload_dict, byte_size))
        except Exception as e:
            self.packet_queue.put(("LOG", f"gRPC ERROR: {str(e)}"))

    def _ui_consumer_tick(self):
        val = self.fps_entry.get().strip()
        self.target_fps = float(val) if val and val.replace('.', '', 1).isdigit() else None

        latest_packet = None
        now = time.time()

        while not self.packet_queue.empty():
            item = self.packet_queue.get_nowait()
            if item[0] == "LOG": self.log_message(item[1])
            elif item[0] == "OBJECTS": latest_packet = item

        if latest_packet:
            _, wire_arrival_time, frame_data, byte_size = latest_packet
            raw_ts = (frame_data.get("timestamp") or frame_data.get("frame", {}).get("timestamp") or 0.0)

            sensor_epoch = 0.0
            if isinstance(raw_ts, str):
                try: sensor_epoch = float(raw_ts) / 1e9 if float(raw_ts) > 1e16 else float(raw_ts)
                except Exception: pass
            elif isinstance(raw_ts, (int, float)):
                sensor_epoch = raw_ts / 1e9 if raw_ts > 1e16 else (raw_ts / 1e3 if raw_ts > 1e10 else raw_ts)

            # --- ADVANCED BENCHMARK RECORDING ---
            if self.bench_active:
                if now <= self.bench_end_time:
                    if sensor_epoch > 0:
                        tot_ms = abs(wire_arrival_time - sensor_epoch) * 1000
                        self.bench_tot_latencies.append(tot_ms)
                        self.bench_net_latencies.append(self.live_wire_latency_ms)
                        self.bench_bytes_received += byte_size
                        self.bench_frame_count += 1
                        
                        if self.last_sensor_epoch > 0 and (sensor_epoch - self.last_sensor_epoch) > 0.180:
                            self.bench_skipped_frames += 1
                        self.last_sensor_epoch = sensor_epoch
                else:
                    self.bench_active = False
                    self.start_bench_btn.configure(state="normal", text="▶ START BENCHMARK", bg="#89b4fa")
                    dur_actual = float(self.dur_entry.get())
                    
                    if len(self.bench_tot_latencies) > 0:
                        avg_tot = sum(self.bench_tot_latencies) / len(self.bench_tot_latencies)
                        tot_jitter = self._calc_jitter(self.bench_tot_latencies)
                        net_jitter = self._calc_jitter(self.bench_net_latencies)
                        fps = self.bench_frame_count / dur_actual
                        kbps = (self.bench_bytes_received / 1024) / dur_actual
                        loss_rate = (self.bench_skipped_frames / (self.bench_frame_count + self.bench_skipped_frames)) * 100

                        summary = (
                            f"\n=== ADVANCED gRPC AI TELEMETRY ({self.bench_frame_count} Frames over {dur_actual}s) ===\n"
                            f" ├── Avg Total Latency : {avg_tot:.3f} ms\n"
                            f" ├── Jitter (Total System Turnaround Standard Deviation σ) : ±{tot_jitter:.3f} ms\n"
                            f" ├── Jitter (Ethernet Wire Transit Standard Deviation σ)   : ±{net_jitter:.3f} ms\n"
                            f" ├── Throughput (Protobuf Binary Bandwidth & Render Rate)  : {kbps:.2f} KB/s ({fps:.1f} FPS)\n"
                            f" └── Packet Loss (Skipped Optical Timestamp Gaps >180ms)   : {self.bench_skipped_frames} frames ({loss_rate:.1f}% loss)\n"
                            f"========================================================================================"
                        )
                        self.log_message(summary)

            if sensor_epoch > 0:
                total_ms = abs(wire_arrival_time - sensor_epoch) * 1000
                net_ms = self.live_wire_latency_ms
                hw_compute_ms = max(0.0, total_ms - net_ms)
                lat_str = f"HW/AI: {hw_compute_ms:.2f}ms | Net: {net_ms:.3f}ms | Total: {total_ms:.2f}ms"
            else: lat_str = "UNKNOWN"
                
            raw_objs = frame_data.get("objects", {})
            obj_map = raw_objs.get("objects", {}) if isinstance(raw_objs, dict) and "objects" in raw_objs else raw_objs
            incoming_intrusions = []

            if isinstance(obj_map, dict):
                for obj_id, obj in obj_map.items():
                    if not isinstance(obj, dict): continue
                    if obj.get("intruding", {}).get("value") is True or obj.get("intruding", {}).get("state") is True:
                        pos = obj.get("pose", {}).get("position") or obj.get("center_of_mass") or {}
                        cx, cy, cz = round(pos.get("x", 0.0), 2), round(pos.get("y", 0.0), 2), round(pos.get("z", 0.0), 2)
                        vel = obj.get("velocity", {})
                        speed_mph = math.sqrt(vel.get("x", 0)**2 + vel.get("y", 0)**2 + vel.get("z", 0)**2) * 2.23694
                        size_str = str(obj.get("properties", {}).get("size", "")).upper()
                        sdsm_type = "VRU (detVRUData)" if "MEDIUM" in size_str else ("VEHICLE (detVehData)" if "LARGE" in size_str else "OBSTACLE")

                        incoming_intrusions.append({
                            "objectID": str(obj_id), "objType": sdsm_type,
                            "pos": f"[{cx}, {cy}, {cz}]", "speed": round(speed_mph, 1)
                        })

            if len(incoming_intrusions) > 0:
                self.alarm_active_until = now + ALARM_HOLD_SECONDS
                self.cached_subjects = incoming_intrusions
                if (now - self.last_log_time) >= COOLDOWN_SECONDS:
                    self.last_log_time = now
                    sense_str = datetime.fromtimestamp(sensor_epoch).strftime('%H:%M:%S.%f')[:-3] if sensor_epoch > 0 else "N/A"
                    recv_str = datetime.fromtimestamp(wire_arrival_time).strftime('%H:%M:%S.%f')[:-3]
                    obj_ids = ", ".join([str(subj["objectID"]) for subj in incoming_intrusions])
                    self.log_message(f"SDSM INTRUSION | IDs: {obj_ids} | Sense: {sense_str} ➔ Recv: {recv_str} | {lat_str}")

        is_alarm = now < self.alarm_active_until
        if (now - self.last_ui_paint) >= UI_REFRESH_RATE_SEC:
            self.last_ui_paint = now
            target_bg = "#f38ba8" if is_alarm else "#a6e3a1"
            master_threat = "ALARM: INTRUSION DETECTED!" if is_alarm else "SYSTEM SECURED - NO INTRUSIONS"
            if self.status_label.cget("text") != master_threat:
                self.status_frame.configure(bg=target_bg)
                self.status_label.configure(bg=target_bg, text=master_threat)

            if is_alarm:
                for target in self.cached_subjects:
                    obj_id = str(target["objectID"])
                    vals = (target["objectID"], target["objType"], target["pos"], f"{target['speed']} mph")
                    if obj_id in self.tree_items: self.tree.item(self.tree_items[obj_id], values=vals)
                    else: self.tree_items[obj_id] = self.tree.insert("", 0, values=vals)
                    if len(self.tree_items) > 100:
                        last_item = self.tree.get_children()[-1]
                        last_id = self.tree.item(last_item)["values"][0]
                        self.tree.delete(last_item)
                        if str(last_id) in self.tree_items: del self.tree_items[str(last_id)]

        self.root.after(100, self._ui_consumer_tick)

if __name__ == "__main__":
    window = tk.Tk()
    app = GrpcTrueWireDashboard(window)
    window.mainloop()