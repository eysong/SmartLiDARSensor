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

LIDAR_IP = "192.168.26.26"
API_KEY = "2ee812bc2e745dddb8i1cmJwrEaz8ehy"

class GrpcEdgeDashboardApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Blickfeld: gRPC Edge-AI Monitor")
        self.root.geometry("800x500")
        self.root.configure(bg="#1e1e2e")

        self.packet_queue = queue.Queue()
        self.last_log_time = 0
        self.alarm_active_until = 0
        self.last_ui_paint = 0
        self.cached_subjects = []

        self.root.rowconfigure(1, weight=1)
        self.root.rowconfigure(2, weight=1)
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

        # Status Banner
        self.status_frame = tk.Frame(self.root, bg="#a6e3a1", height=80)
        self.status_frame.grid(row=0, column=0, sticky="ew", padx=15, pady=10)
        self.status_frame.grid_propagate(False)
        self.status_label = tk.Label(self.status_frame, text="SYSTEM SECURED - NO INTRUSIONS", font=("Arial", 16, "bold"), bg="#a6e3a1", fg="#11111b")
        self.status_label.pack(expand=True)

        # Subjects Table
        subjects_frame = tk.LabelFrame(self.root, text=f" gRPC Tracked Subjects inside '{TARGET_ZONE}' ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        subjects_frame.grid(row=1, column=0, sticky="nsew", padx=15, pady=5)
        subjects_frame.rowconfigure(0, weight=1)
        subjects_frame.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(subjects_frame, columns=("id", "type", "speed"), show="headings")
        self.tree.heading("id", text="Cluster ID")
        self.tree.heading("type", text="Classification")
        self.tree.heading("speed", text="Travel Speed")
        self.tree.column("id", anchor="center")
        self.tree.column("type", anchor="center")
        self.tree.column("speed", anchor="center")
        self.tree.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

        # Telemetry Log
        perf_frame = tk.LabelFrame(self.root, text=" gRPC Edge-AI Performance Log ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        perf_frame.grid(row=2, column=0, sticky="nsew", padx=15, pady=10)
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
                    self.packet_queue.put(("OBJECTS", time.time(), response.to_dict()))
        except Exception as e:
            self.packet_queue.put(("LOG", f"gRPC ERROR: {str(e)}"))

    def _ui_consumer_tick(self):
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
            
            # 1. ROBUST TIMESTAMP EXTRACTION (Cascade through possible locations)
            raw_ts = (
                frame_data.get("timestamp") or
                frame_data.get("frame", {}).get("timestamp") or
                frame_data.get("objects", {}).get("timestamp") or
                frame_data.get("objects", {}).get("metadata", {}).get("timestamp") or
                0.0
            )

            # 2. STRING & NUMBER PARSER
            sensor_epoch = 0.0
            if isinstance(raw_ts, str):
                try:
                    if "T" in raw_ts:
                        sensor_epoch = datetime.fromisoformat(raw_ts.replace("Z", "+00:00")).timestamp()
                    else:
                        val = float(raw_ts)
                        sensor_epoch = val / 1e9 if val > 1e16 else (val / 1e3 if val > 1e10 else val)
                except Exception:
                    pass
            else:
                try:
                    val = float(raw_ts)
                    sensor_epoch = val / 1e9 if val > 1e16 else (val / 1e3 if val > 1e10 else val)
                except (ValueError, TypeError):
                    pass

            # 3. NTP-RELIANT LATENCY MATH
            if sensor_epoch > 0:
                edge_lat_ms = abs(wire_arrival_time - sensor_epoch) * 1000
                lat_str = f"{edge_lat_ms:.2f}ms"
            else:
                lat_str = "UNKNOWN"
                
            # (Keep the rest of your object parsing code below this...)
            raw_objs = frame_data.get("objects", {})
            obj_map = raw_objs.get("objects", {}) if isinstance(raw_objs, dict) and "objects" in raw_objs else raw_objs
            incoming_intrusions = []

            if isinstance(obj_map, dict):
                for obj_id, obj in obj_map.items():
                    if not isinstance(obj, dict): continue
                    intruding = obj.get("intruding", {})
                    if intruding.get("value") is True or intruding.get("state") is True:
                        props = obj.get("properties", {}) or obj.get("classification", {})
                        raw_size = str(props.get("size", "")).upper()
                        
                        friendly_type = "UNCLASSIFIED_MOTION"
                        if "MEDIUM" in raw_size: friendly_type = "PERSON"
                        elif "LARGE" in raw_size: friendly_type = "VEHICLE"
                        elif "SMALL" in raw_size: friendly_type = "ANIMAL_OR_DEBRIS"

                        vel = obj.get("velocity", {})
                        vx, vy, vz = vel.get("x", 0), vel.get("y", 0), vel.get("z", 0)
                        speed_mph = math.sqrt(vx**2 + vy**2 + vz**2) * 2.23694

                        incoming_intrusions.append({
                            "cluster_id": str(obj_id),
                            "classification": friendly_type,
                            "speed_mph": round(speed_mph, 1),
                        })

            if len(incoming_intrusions) > 0:
                self.alarm_active_until = now + ALARM_HOLD_SECONDS
                self.cached_subjects = incoming_intrusions
                if (now - self.last_log_time) >= COOLDOWN_SECONDS:
                    self.last_log_time = now
                    self.log_message(f"INTRUSION | gRPC Pipeline Latency: {lat_str} | Count: {len(incoming_intrusions)}")

        # UI Drawing
        is_alarm = now < self.alarm_active_until
        display_list = self.cached_subjects if is_alarm else []

        if (now - self.last_ui_paint) >= UI_REFRESH_RATE_SEC:
            self.last_ui_paint = now
            target_bg = "#f38ba8" if is_alarm else "#a6e3a1"
            master_threat = "ALARM: INTRUSION DETECTED!" if is_alarm else "SYSTEM SECURED - NO INTRUSIONS"
            
            if self.status_label.cget("text") != master_threat:
                self.status_frame.configure(bg=target_bg)
                self.status_label.configure(bg=target_bg, text=master_threat)

            for item in self.tree.get_children(): self.tree.delete(item)
            for target in display_list:
                self.tree.insert("", tk.END, values=(target["cluster_id"], target["classification"], f"{target['speed_mph']} mph"))

        self.root.after(100, self._ui_consumer_tick)

if __name__ == "__main__":
    window = tk.Tk()
    app = GrpcEdgeDashboardApp(window)
    window.mainloop()