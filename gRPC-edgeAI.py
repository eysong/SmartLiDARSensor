import asyncio
import math
import queue
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

# Calibrated via SSH tcpdump network benchmark (Wire transit time in ms to 3 decimal places)
TCPDUMP_WIRE_LATENCY_MS = 0.200 

LIDAR_IP = "192.168.26.26"
API_KEY = "2ee812bc2e745dddb8i1cmJwrEaz8ehy"

class GrpcEdgeDashboardApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Blickfeld: gRPC Edge-AI Monitor (Microsecond Precision)")
        self.root.geometry("880x600")
        self.root.configure(bg="#1e1e2e")

        self.packet_queue = queue.Queue()
        self.last_log_time = 0
        self.alarm_active_until = 0
        self.last_ui_paint = 0
        self.cached_subjects = []
        self.tree_items = {}

        # Thread-safe rate limiting state
        self.target_fps = None      # None = Unthrottled (Receive all messages)
        self.last_proc_time = 0.0

        self.root.rowconfigure(2, weight=1)
        self.root.rowconfigure(3, weight=1)
        self.root.columnconfigure(0, weight=1)

        self._build_ui()

        self.grpc_thread = threading.Thread(target=self._grpc_objects_producer, daemon=True)
        self.grpc_thread.start()

        self.root.after(100, self._ui_consumer_tick)

    def _build_ui(self):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background="#252538", foreground="white", fieldbackground="#252538", rowheight=26)
        style.configure("Treeview.Heading", background="#32324d", foreground="white", relief="flat")

        # Control Bar for Dynamic Rate Limiting
        ctrl_frame = tk.Frame(self.root, bg="#252538", pady=8, padx=15)
        ctrl_frame.grid(row=0, column=0, sticky="ew", padx=15, pady=(10, 0))
        
        tk.Label(ctrl_frame, text="⚙️ Client-Side Rate Limit (FPS):", font=("Arial", 10, "bold"), bg="#252538", fg="white").pack(side=tk.LEFT, padx=5)
        self.fps_entry = tk.Entry(ctrl_frame, width=6, font=("Consolas", 11, "bold"), bg="#181825", fg="#89b4fa", insertbackground="white")
        self.fps_entry.insert(0, "")  # Default blank = unthrottled
        self.fps_entry.pack(side=tk.LEFT, padx=5)
        tk.Label(ctrl_frame, text="*(Leave blank for unthrottled hardware broadcast)*", font=("Arial", 9, "italic"), bg="#252538", fg="#a6adc8").pack(side=tk.LEFT, padx=10)

        # Status Banner
        self.status_frame = tk.Frame(self.root, bg="#a6e3a1", height=70)
        self.status_frame.grid(row=1, column=0, sticky="ew", padx=15, pady=10)
        self.status_frame.grid_propagate(False)
        self.status_label = tk.Label(self.status_frame, text="SYSTEM SECURED - NO INTRUSIONS", font=("Arial", 16, "bold"), bg="#a6e3a1", fg="#11111b")
        self.status_label.pack(expand=True)

        # Subjects Table
        subjects_frame = tk.LabelFrame(self.root, text=f" gRPC Tracked Subjects inside '{TARGET_ZONE}' ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        subjects_frame.grid(row=2, column=0, sticky="nsew", padx=15, pady=5)
        subjects_frame.rowconfigure(0, weight=1)
        subjects_frame.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(subjects_frame, columns=("objectID", "objType", "pos", "speed"), show="headings")
        self.tree.heading("objectID", text="objectID (Temp ID)")
        self.tree.heading("objType", text="objType & OptionalData")
        self.tree.heading("pos", text="pos (PositionOffsetXYZ)")
        self.tree.heading("speed", text="speed (Magnitude)")
        
        self.tree.column("objectID", anchor="center", width=100)
        self.tree.column("objType", anchor="center", width=180)
        self.tree.column("pos", anchor="center", width=180)
        self.tree.column("speed", anchor="center", width=100)
        self.tree.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

        tree_scroll = ttk.Scrollbar(subjects_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_scroll.set)
        tree_scroll.grid(row=0, column=1, sticky="ns")

        # Telemetry Log
        perf_frame = tk.LabelFrame(self.root, text=" gRPC Edge-AI Performance Log (Microsecond Resolution) ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
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
                    
                    if self.target_fps is not None and self.target_fps > 0:
                        if (now - self.last_proc_time) < (1.0 / self.target_fps):
                            continue
                            
                    self.last_proc_time = now
                    self.packet_queue.put(("OBJECTS", now, response.to_dict()))
        except Exception as e:
            self.packet_queue.put(("LOG", f"gRPC ERROR: {str(e)}"))

    def _ui_consumer_tick(self):
        val = self.fps_entry.get().strip()
        if val:
            try: self.target_fps = float(val)
            except ValueError: self.target_fps = None
        else: self.target_fps = None

        latest_packet = None
        now = time.time()

        while not self.packet_queue.empty():
            item = self.packet_queue.get_nowait()
            if item[0] == "LOG":
                self.log_message(item[1])
            elif item[0] == "OBJECTS":
                latest_packet = item

        if latest_packet:
            _, wire_arrival_time, frame_data = latest_packet
            
            raw_ts = (
                frame_data.get("timestamp") or
                frame_data.get("frame", {}).get("timestamp") or
                frame_data.get("objects", {}).get("timestamp") or
                frame_data.get("objects", {}).get("metadata", {}).get("timestamp") or
                0.0
            )

            sensor_epoch = 0.0
            if isinstance(raw_ts, str):
                try:
                    if "T" in raw_ts:
                        sensor_epoch = datetime.fromisoformat(raw_ts.replace("Z", "+00:00")).timestamp()
                    else:
                        val = float(raw_ts)
                        sensor_epoch = val / 1e9 if val > 1e16 else (val / 1e3 if val > 1e10 else val)
                except Exception: pass
            else:
                try:
                    val = float(raw_ts)
                    sensor_epoch = val / 1e9 if val > 1e16 else (val / 1e3 if val > 1e10 else val)
                except (ValueError, TypeError): pass

            # --- HIGH-PRECISION LATENCY BREAKDOWN (3 DECIMAL PLACES) ---
            if sensor_epoch > 0:
                total_ms = abs(wire_arrival_time - sensor_epoch) * 1000
                net_ms = TCPDUMP_WIRE_LATENCY_MS
                hw_compute_ms = max(0.0, total_ms - net_ms)
                
                # Upgraded to .3f for microsecond resolution
                lat_str = f"HW Scan/AI: {hw_compute_ms:.3f}ms | Net (tcpdump): {net_ms:.3f}ms | Total: {total_ms:.3f}ms"
            else:
                lat_str = "UNKNOWN"
                
            raw_objs = frame_data.get("objects", {})
            obj_map = raw_objs.get("objects", {}) if isinstance(raw_objs, dict) and "objects" in raw_objs else raw_objs
            incoming_intrusions = []

            if isinstance(obj_map, dict):
                for obj_id, obj in obj_map.items():
                    if not isinstance(obj, dict): continue
                    intruding = obj.get("intruding", {})
                    if intruding.get("value") is True or intruding.get("state") is True:
                        pos_data = (
                            obj.get("pose", {}).get("position") or
                            obj.get("center_of_mass") or
                            obj.get("kinematics", {}).get("position") or
                            obj.get("bounding_box", {}).get("center") or
                            {}
                        )
                        cx = round(pos_data.get("x", 0.0), 2)
                        cy = round(pos_data.get("y", 0.0), 2)
                        cz = round(pos_data.get("z", 0.0), 2)

                        vel = obj.get("velocity", {})
                        vx, vy, vz = vel.get("x", 0), vel.get("y", 0), vel.get("z", 0)
                        speed_mph = math.sqrt(vx**2 + vy**2 + vz**2) * 2.23694

                        props = obj.get("properties", {}) or obj.get("classification", {})
                        raw_size = str(props.get("size", "")).upper()
                        
                        if "MEDIUM" in raw_size: sdsm_type = "VRU (detVRUData)" 
                        elif "LARGE" in raw_size: sdsm_type = "VEHICLE (detVehData)" 
                        else: sdsm_type = "OBSTACLE (detObstData)"

                        incoming_intrusions.append({
                            "objectID": str(obj_id),
                            "objType": sdsm_type,
                            "pos": f"[{cx}, {cy}, {cz}]",
                            "speed": round(speed_mph, 1),
                        })

            if len(incoming_intrusions) > 0:
                self.alarm_active_until = now + ALARM_HOLD_SECONDS
                self.cached_subjects = incoming_intrusions
                if (now - self.last_log_time) >= COOLDOWN_SECONDS:
                    self.last_log_time = now
                    
                    sensor_time_str = datetime.fromtimestamp(sensor_epoch).strftime('%H:%M:%S.%f')[:-3] if sensor_epoch > 0 else "N/A"
                    recv_time_str = datetime.fromtimestamp(wire_arrival_time).strftime('%H:%M:%S.%f')[:-3]
                    
                    obj_ids = ", ".join([str(subj["objectID"]) for subj in incoming_intrusions])
                    mode_str = f"Throttled ({self.target_fps} FPS)" if self.target_fps else "Unthrottled (Firehose)"
                    self.log_message(f"SDSM INTRUSION [{mode_str}] | IDs: {obj_ids} | Sense: {sensor_time_str} ➔ Recv: {recv_time_str} | {lat_str}")

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
                    
                    if obj_id in self.tree_items:
                        self.tree.item(self.tree_items[obj_id], values=vals)
                    else:
                        item = self.tree.insert("", 0, values=vals)
                        self.tree_items[obj_id] = item
                        
                    if len(self.tree_items) > 100:
                        last_item = self.tree.get_children()[-1]
                        last_id = self.tree.item(last_item)["values"][0]
                        self.tree.delete(last_item)
                        if str(last_id) in self.tree_items:
                            del self.tree_items[str(last_id)]

        self.root.after(100, self._ui_consumer_tick)

if __name__ == "__main__":
    window = tk.Tk()
    app = GrpcEdgeDashboardApp(window)
    window.mainloop()