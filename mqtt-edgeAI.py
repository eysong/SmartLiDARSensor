import json
import queue
import threading
import time
from datetime import datetime, timezone
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
        self.root.title("Blickfeld: MQTT Intrusion Monitor")
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

        self.mqtt_thread = threading.Thread(target=self._mqtt_subscriber_thread, daemon=True)
        self.mqtt_thread.start()

        self.root.after(100, self._ui_consumer_tick)

    def _build_ui(self):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background="#252538", foreground="white", fieldbackground="#252538", rowheight=26)
        style.configure("Treeview.Heading", background="#32324d", foreground="white", relief="flat")
        style.map("Treeview", background=[("selected", "#4c4f69")])

        # Status Banner
        self.status_frame = tk.Frame(self.root, bg="#a6e3a1", height=80)
        self.status_frame.grid(row=0, column=0, sticky="ew", padx=15, pady=10)
        self.status_frame.grid_propagate(False)
        self.status_label = tk.Label(self.status_frame, text="SYSTEM SECURED - NO INTRUSIONS", font=("Arial", 16, "bold"), bg="#a6e3a1", fg="#11111b")
        self.status_label.pack(expand=True)

        # Subjects Table
        subjects_frame = tk.LabelFrame(self.root, text=" Tracked Subjects ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
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

        # Log
        perf_frame = tk.LabelFrame(self.root, text=" Telemetry Log ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        perf_frame.grid(row=2, column=0, sticky="nsew", padx=15, pady=10)
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

    def _mqtt_subscriber_thread(self):
        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                self.packet_queue.put(("LOG", f"ONLINE: Connected to local MQTT Broker on {MQTT_PORT}"))
                client.subscribe(MQTT_TOPIC)
            else:
                self.packet_queue.put(("LOG", f"ERROR: MQTT Connection failed with code {rc}"))

        def on_message(client, userdata, msg):
            try:
                payload = json.loads(msg.payload.decode("utf-8"))
                self.packet_queue.put(("MQTT", time.time(), payload))
            except Exception:
                pass

        try:
            try:
                client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="MQTT_Dash")
            except AttributeError:
                client = mqtt.Client(client_id="MQTT_Dash")
            client.on_connect = on_connect
            client.on_message = on_message
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            self.packet_queue.put(("LOG", f"MQTT ERROR: {str(e)}"))

    def _ui_consumer_tick(self):
        latest_mqtt = None
        now = time.time()

        while not self.packet_queue.empty():
            item = self.packet_queue.get_nowait()
            if item[0] == "LOG":
                self.log_message(item[1])
            elif item[0] == "MQTT":
                latest_mqtt = item

        if latest_mqtt:
            _, wire_arrival_time, payload = latest_mqtt
            
            # Extract timestamp from your custom Node-RED JSON
            raw_ts = payload.get("timestamp") or payload.get("time") or 0.0
            sensor_epoch = 0.0
            
            if isinstance(raw_ts, str):
                try:
                    sensor_epoch = datetime.fromisoformat(raw_ts.replace("Z", "+00:00")).timestamp()
                except Exception:
                    pass
            else:
                try:
                    sensor_epoch = float(raw_ts) / 1e9 if float(raw_ts) > 1e16 else (float(raw_ts) / 1e3 if float(raw_ts) > 1e10 else float(raw_ts))
                except (ValueError, TypeError):
                    pass

            if sensor_epoch > 0:
                # Basic latency math (relies on NTP sync between sensor and laptop)
                edge_lat_ms = abs(wire_arrival_time - sensor_epoch) * 1000
                lat_str = f"{edge_lat_ms:.2f}ms"
            else:
                lat_str = "UNKNOWN"

            incoming_intrusions = []
            custom_subjects = payload.get("subjects") or []
            
            if isinstance(custom_subjects, list):
                for subj in custom_subjects:
                    if isinstance(subj, dict):
                        incoming_intrusions.append({
                            "cluster_id": str(subj.get("cluster_id", "Unknown")),
                            "classification": subj.get("classification", "UNCLASSIFIED_MOTION"),
                            "speed_mph": float(subj.get("speed_mph", 0.0)),
                        })

            if len(incoming_intrusions) > 0:
                self.alarm_active_until = now + ALARM_HOLD_SECONDS
                self.cached_subjects = incoming_intrusions
                if (now - self.last_log_time) >= COOLDOWN_SECONDS:
                    self.last_log_time = now
                    
                    # Convert both epochs to human-readable format
                    sensor_time_str = datetime.fromtimestamp(sensor_epoch).strftime('%H:%M:%S.%f')[:-3]
                    recv_time_str = datetime.fromtimestamp(wire_arrival_time).strftime('%H:%M:%S.%f')[:-3]
                    
                    self.log_message(f"INTRUSION | Sensor: {sensor_time_str} -> Laptop: {recv_time_str} | Latency: {lat_str}")

        # UI Updates
        is_alarm = now < self.alarm_active_until
        display_list = self.cached_subjects if is_alarm else []

        if (now - self.last_ui_paint) >= UI_REFRESH_RATE_SEC:
            self.last_ui_paint = now
            
            target_bg = "#f38ba8" if is_alarm else "#a6e3a1"
            master_threat = "ALARM: INTRUSION DETECTED!" if is_alarm else "SYSTEM SECURED - NO INTRUSIONS"
            if self.status_label.cget("text") != master_threat:
                self.status_frame.configure(bg=target_bg)
                self.status_label.configure(bg=target_bg, text=master_threat)

            rendered_ids = [self.tree.item(c)["values"][0] for c in self.tree.get_children() if self.tree.item(c)["values"]]
            new_ids = [x["cluster_id"] for x in display_list]
            
            if new_ids != rendered_ids:
                for item in self.tree.get_children():
                    self.tree.delete(item)
                for target in display_list:
                    self.tree.insert("", tk.END, values=(target["cluster_id"], target["classification"], f"{target['speed_mph']} mph"))

        self.root.after(100, self._ui_consumer_tick)

if __name__ == "__main__":
    window = tk.Tk()
    app = MqttDashboardApp(window)
    window.mainloop()