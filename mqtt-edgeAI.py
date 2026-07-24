import json
import math
import queue
import threading
import time
from datetime import datetime
import tkinter as tk
from tkinter import ttk
import paho.mqtt.client as mqtt

# ==========================================
# --- CONFIGURATION ---
# ==========================================
COOLDOWN_SECONDS = 5
ALARM_HOLD_SECONDS = 1.5
UI_REFRESH_RATE_SEC = 0.2

MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC = "#"

class MqttDashboardApp:
    def __init__(self, root):
        self.root = root
        self.root.title("MQTT Edge AI Monitor")
        self.root.geometry("900x620")
        self.root.configure(bg="#1e1e2e")

        self.packet_queue = queue.Queue()
        self.last_log_time = 0
        self.alarm_active_until = 0
        self.last_ui_paint = 0
        self.cached_subjects = []
        self.tree_items = {}
        
        self.target_fps = None      
        self.last_proc_time = 0.0

        self.bench_active = False
        self.bench_end_time = 0.0
        self.bench_proc_latencies = []
        self.bench_net_latencies = []
        self.bench_tot_latencies = []
        self.bench_bytes_received = 0
        self.bench_frames_processed = 0
        self.bench_frames_throttled = 0

        self.root.rowconfigure(2, weight=1)
        self.root.rowconfigure(3, weight=1)
        self.root.columnconfigure(0, weight=1)

        self._build_ui()

        threading.Thread(target=self._mqtt_subscriber_thread, daemon=True).start()
        self.root.after(100, self._ui_consumer_tick)

    def _build_ui(self):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background="#252538", foreground="white", fieldbackground="#252538", rowheight=26)
        style.configure("Treeview.Heading", background="#32324d", foreground="white", relief="flat")

        ctrl_frame = tk.Frame(self.root, bg="#252538", pady=8, padx=15)
        ctrl_frame.grid(row=0, column=0, sticky="ew", padx=15, pady=(10, 0))
        
        tk.Label(ctrl_frame, text="Dur (s):", font=("Arial", 10, "bold"), bg="#252538", fg="white").pack(side=tk.LEFT, padx=2)
        self.dur_entry = tk.Entry(ctrl_frame, width=4, font=("Consolas", 11, "bold"), bg="#181825", fg="#89b4fa", insertbackground="white")
        self.dur_entry.insert(0, "10")
        self.dur_entry.pack(side=tk.LEFT, padx=2)

        self.start_bench_btn = tk.Button(ctrl_frame, text="START BENCHMARK", font=("Arial", 10, "bold"), bg="#89b4fa", fg="#11111b", command=self._start_timed_benchmark, relief="flat")
        self.start_bench_btn.pack(side=tk.LEFT, padx=10)

        tk.Label(ctrl_frame, text="Rate Limit (FPS):", font=("Arial", 10, "bold"), bg="#252538", fg="white").pack(side=tk.LEFT, padx=5)
        self.fps_entry = tk.Entry(ctrl_frame, width=5, font=("Consolas", 11, "bold"), bg="#181825", fg="#89b4fa", insertbackground="white")
        self.fps_entry.pack(side=tk.LEFT, padx=5)
        tk.Label(ctrl_frame, text="*(Blank = Firehose)*", font=("Arial", 9, "italic"), bg="#252538", fg="#a6adc8").pack(side=tk.LEFT, padx=5)

        self.status_frame = tk.Frame(self.root, bg="#a6e3a1", height=65)
        self.status_frame.grid(row=1, column=0, sticky="ew", padx=15, pady=10)
        self.status_frame.grid_propagate(False)
        self.status_label = tk.Label(self.status_frame, text="SYSTEM SECURED - NO INTRUSIONS", font=("Arial", 16, "bold"), bg="#a6e3a1", fg="#11111b")
        self.status_label.pack(expand=True)

        subjects_frame = tk.LabelFrame(self.root, text=" Tracked Subjects ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
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

        perf_frame = tk.LabelFrame(self.root, text=" Telemetry Log (Jitter, Throughput & Throttle Analysis) ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        perf_frame.grid(row=3, column=0, sticky="nsew", padx=15, pady=10)
        perf_frame.rowconfigure(0, weight=1)
        perf_frame.columnconfigure(0, weight=1)

        self.edge_log = tk.Text(perf_frame, bg="#181825", fg="#bac2de", font=("Consolas", 10), state="disabled", wrap="word")
        self.edge_log.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        scroll = ttk.Scrollbar(perf_frame, orient="vertical", command=self.edge_log.yview)
        self.edge_log.configure(yscrollcommand=scroll.set)
        scroll.grid(row=0, column=1, sticky="ns")

    def log_message(self, msg):
        self.edge_log.configure(state="normal")
        self.edge_log.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}\n")
        self.edge_log.see(tk.END)
        self.edge_log.configure(state="disabled")

    def _start_timed_benchmark(self):
        if self.bench_active: return
        try: dur = float(self.dur_entry.get())
        except ValueError: return

        self.bench_active = True
        self.bench_end_time = time.time() + dur
        self.bench_proc_latencies, self.bench_net_latencies, self.bench_tot_latencies = [], [], []
        self.bench_bytes_received, self.bench_frames_processed, self.bench_frames_throttled = 0, 0, 0
        self.start_bench_btn.configure(state="disabled", text="RECORDING...", bg="#f38ba8", disabledforeground="#11111b")
        self.log_message(f"\n=== STARTING {dur}s ADVANCED MQTT INTRUSION BENCHMARK ===")

    def _calc_jitter(self, latencies):
        if len(latencies) < 2: return 0.0
        avg = sum(latencies) / len(latencies)
        variance = sum((x - avg) ** 2 for x in latencies) / len(latencies)
        return math.sqrt(variance)

    def _mqtt_subscriber_thread(self):
        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                self.packet_queue.put(("LOG", f"ONLINE: Connected to local MQTT Broker on {MQTT_PORT}"))
                client.subscribe(MQTT_TOPIC)
            else: self.packet_queue.put(("LOG", f"ERROR: MQTT Connection failed with code {rc}"))

        def on_message(client, userdata, msg):
            try:
                now = time.time()
                byte_size = len(msg.payload)
                
                if self.target_fps is not None and self.target_fps > 0:
                    if (now - self.last_proc_time) < (1.0 / self.target_fps):
                        if self.bench_active and now <= self.bench_end_time:
                            self.bench_frames_throttled += 1
                        return
                        
                self.last_proc_time = now
                payload = json.loads(msg.payload.decode("utf-8"))
                self.packet_queue.put(("MQTT", now, payload, byte_size))
            except Exception: pass

        try:
            try: client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="MQTT_Dash")
            except AttributeError: client = mqtt.Client(client_id="MQTT_Dash")
            client.on_connect = on_connect
            client.on_message = on_message
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e: self.packet_queue.put(("LOG", f"MQTT ERROR: {str(e)}"))

    def _ui_consumer_tick(self):
        val = self.fps_entry.get().strip()
        self.target_fps = float(val) if val else None

        latest_mqtt = None
        now = time.time()

        while not self.packet_queue.empty():
            item = self.packet_queue.get_nowait()
            if item[0] == "LOG": self.log_message(item[1])
            elif item[0] == "MQTT": latest_mqtt = item

        if latest_mqtt:
            _, wire_arrival_time, payload, byte_size = latest_mqtt
            
            # PRIORITIZE OPTICAL HARDWARE TIMESTAMP
            raw_ts = (
                payload.get("optical_timestamp") or
                payload.get("timestamp") or
                payload.get("time") or
                0.0
            )

            raw_send_ts = payload.get("send_time", 0.0)

            sensor_epoch = 0.0
            if isinstance(raw_ts, str):
                try:
                    if "T" in raw_ts:
                        sensor_epoch = datetime.fromisoformat(raw_ts.replace("Z", "+00:00")).timestamp()
                    else:
                        val_num = float(raw_ts)
                        sensor_epoch = val_num / 1e9 if val_num > 1e16 else (val_num / 1e6 if val_num > 1e13 else (val_num / 1e3 if val_num > 1e10 else val_num))
                except Exception: pass
            elif isinstance(raw_ts, (int, float)):
                try:
                    val_num = float(raw_ts)
                    sensor_epoch = val_num / 1e9 if val_num > 1e16 else (val_num / 1e6 if val_num > 1e13 else (val_num / 1e3 if val_num > 1e10 else val_num))
                except Exception: pass

            send_epoch = 0.0
            if raw_send_ts:
                try:
                    if isinstance(raw_send_ts, str) and "T" in raw_send_ts:
                        send_epoch = datetime.fromisoformat(raw_send_ts.replace("Z", "+00:00")).timestamp()
                    else:
                        val_num = float(raw_send_ts)
                        send_epoch = val_num / 1e9 if val_num > 1e16 else (val_num / 1e6 if val_num > 1e13 else (val_num / 1e3 if val_num > 1e10 else val_num))
                except Exception: pass
            
            if send_epoch == 0.0 and sensor_epoch > 0: 
                send_epoch = sensor_epoch

            proc_ms = abs(send_epoch - sensor_epoch) * 1000
            net_ms = abs(wire_arrival_time - send_epoch) * 1000
            tot_ms = abs(wire_arrival_time - sensor_epoch) * 1000

            if self.bench_active and now <= self.bench_end_time:
                if sensor_epoch > 0:
                    self.bench_proc_latencies.append(proc_ms)
                    self.bench_net_latencies.append(net_ms)
                    self.bench_tot_latencies.append(tot_ms)
                    self.bench_bytes_received += byte_size
                    self.bench_frames_processed += 1

            if sensor_epoch > 0:
                lat_str = f"Sensing: {proc_ms:.1f}ms | Net: {net_ms:.1f}ms | Total: {tot_ms:.1f}ms"
            else: lat_str = "UNKNOWN"

            incoming_intrusions = []
            custom_subjects = payload.get("subjects") or []
            
            if isinstance(custom_subjects, list):
                for subj in custom_subjects:
                    if isinstance(subj, dict):
                        cx, cy, cz = round(subj.get("x", 0.0), 2), round(subj.get("y", 0.0), 2), round(subj.get("z", 0.0), 2)
                        raw_class = str(subj.get("classification", "")).upper()
                        sdsm_type = "VRU (detVRUData)" if "PERSON" in raw_class or "MEDIUM" in raw_class else ("VEHICLE (detVehData)" if "VEHICLE" in raw_class or "LARGE" in raw_class else "OBSTACLE")

                        incoming_intrusions.append({
                            "objectID": str(subj.get("cluster_id", "Unknown")), "objType": sdsm_type,
                            "pos": f"[{cx}, {cy}, {cz}]", "speed": round(float(subj.get("speed_mph", 0.0)), 1)
                        })

            if len(incoming_intrusions) > 0:
                self.alarm_active_until = now + ALARM_HOLD_SECONDS
                self.cached_subjects = incoming_intrusions
                if (now - self.last_log_time) >= COOLDOWN_SECONDS:
                    self.last_log_time = now
                    sense_str = datetime.fromtimestamp(sensor_epoch).strftime('%H:%M:%S.%f')[:-3] if sensor_epoch > 0 else "N/A"
                    recv_str = datetime.fromtimestamp(wire_arrival_time).strftime('%H:%M:%S.%f')[:-3]
                    obj_ids = ", ".join([str(subj["objectID"]) for subj in incoming_intrusions])
                    mode_str = f"Throttled ({self.target_fps} FPS)" if self.target_fps else "Unthrottled"
                    self.log_message(f"SDSM INTRUSION [{mode_str}] | IDs: {obj_ids} | Sense: {sense_str} -> Recv: {recv_str} | {lat_str}")

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

        if self.bench_active:
            now_time = time.time()
            if now_time <= self.bench_end_time:
                time_left = max(0.0, self.bench_end_time - now_time)
                self.start_bench_btn.configure(text=f"RECORDING ({time_left:.1f}s)")
            else:
                self.bench_active = False
                self.start_bench_btn.configure(state="normal", text="START BENCHMARK", bg="#89b4fa", fg="#11111b")
                dur_actual = float(self.dur_entry.get())
                
                if len(self.bench_tot_latencies) > 0:
                    avg_proc = sum(self.bench_proc_latencies) / len(self.bench_proc_latencies) if self.bench_proc_latencies else 0.0
                    avg_net = sum(self.bench_net_latencies) / len(self.bench_net_latencies) if self.bench_net_latencies else 0.0
                    avg_tot = sum(self.bench_tot_latencies) / len(self.bench_tot_latencies)

                    proc_jitter = self._calc_jitter(self.bench_proc_latencies)
                    net_jitter = self._calc_jitter(self.bench_net_latencies)
                    tot_jitter = self._calc_jitter(self.bench_tot_latencies)

                    fps = self.bench_frames_processed / dur_actual
                    kbps = (self.bench_bytes_received / 1024) / dur_actual
                    tot_attempts = self.bench_frames_processed + self.bench_frames_throttled
                    drop_rate = (self.bench_frames_throttled / tot_attempts) * 100 if tot_attempts > 0 else 0

                    summary = (
                        f"\n=== ADVANCED MQTT TELEMETRY ({self.bench_frames_processed} Frames over {dur_actual}s) ===\n"
                        f" ├── Avg Sensing / Middleware Latency : {avg_proc:.2f} ms (Jitter σ: ±{proc_jitter:.2f} ms)\n"
                        f" ├── Avg Network Wire Transit Latency  : {avg_net:.2f} ms (Jitter σ: ±{net_jitter:.2f} ms)\n"
                        f" ├── Avg Total End-to-End Latency      : {avg_tot:.2f} ms (Jitter σ: ±{tot_jitter:.2f} ms)\n"
                        f" ├── Throughput (JSON Text Bandwidth)  : {kbps:.2f} KB/s ({fps:.1f} FPS)\n"
                        f" └── Packet Loss (Throttled Discards)  : {self.bench_frames_throttled} frames ({drop_rate:.1f}% drop)\n"
                        f"=========================================================================================="
                    )
                    self.log_message(summary)

        self.root.after(100, self._ui_consumer_tick)

if __name__ == "__main__":
    window = tk.Tk()
    app = MqttDashboardApp(window)
    window.mainloop()