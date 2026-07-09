import json
import queue
import threading
import time
from datetime import datetime
import tkinter as tk
from tkinter import ttk
import paho.mqtt.client as mqtt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

# ==========================================
# --- CONFIGURATION ---
# ==========================================
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC = "blickfeld/raw_pointcloud"  # Change this to match your Node-RED/Push topic

class MqttRawDashboardApp:
    def __init__(self, root):
        self.root = root
        self.root.title("⚠️ WARNING: Raw Point Cloud via MQTT Benchmark")
        self.root.geometry("1100x600")
        self.root.configure(bg="#1e1e2e")

        self.packet_queue = queue.Queue()
        self.cached_xyz = ([], [], [])
        self.last_ui_paint = 0

        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=3)
        self.root.columnconfigure(1, weight=2)

        self._build_ui()

        self.mqtt_thread = threading.Thread(target=self._mqtt_subscriber_thread, daemon=True)
        self.mqtt_thread.start()

        self.root.after(100, self._ui_consumer_tick)

    def _build_ui(self):
        # 3D Matplotlib Canvas
        pc_frame = tk.LabelFrame(self.root, text=" Unpacked MQTT 3D Matrix Stream ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        pc_frame.grid(row=0, column=0, sticky="nsew", padx=15, pady=10)

        self.fig = Figure(figsize=(6, 5), dpi=90, facecolor="#1e1e2e")
        self.ax = self.fig.add_subplot(111, projection="3d", facecolor="#1e1e2e")
        self.ax.tick_params(colors="white", labelsize=7)
        self.canvas = FigureCanvasTkAgg(self.fig, master=pc_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # Performance Monitor Log
        bench_frame = tk.LabelFrame(self.root, text=" MQTT Buffer Load Diagnostics ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        bench_frame.grid(row=0, column=1, sticky="nsew", padx=(0, 15), pady=10)
        bench_frame.rowconfigure(0, weight=1)
        bench_frame.columnconfigure(0, weight=1)

        self.log_widget = tk.Text(bench_frame, bg="#181825", fg="#f38ba8", font=("Consolas", 10), state="disabled", wrap="word")
        self.log_widget.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        scroll = ttk.Scrollbar(bench_frame, orient="vertical", command=self.log_widget.yview)
        self.log_widget.configure(yscrollcommand=scroll.set)
        scroll.grid(row=0, column=1, sticky="ns")

    def log_message(self, msg):
        self.log_widget.configure(state="normal")
        self.log_widget.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}\n")
        self.log_widget.see(tk.END)
        self.log_widget.configure(state="disabled")

    def _mqtt_subscriber_thread(self):
        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                self.packet_queue.put(("LOG", f"CONNECTED: Listening to heavy stream on '{MQTT_TOPIC}'..."))
                client.subscribe(MQTT_TOPIC)
            else:
                self.packet_queue.put(("LOG", f"BROKER REFUSED CONNECTION: Code {rc}"))

        def on_message(client, userdata, msg):
            try:
                t_recv = time.time()
                payload = json.loads(msg.payload.decode("utf-8"))
                self.packet_queue.put(("PC_DATA", t_recv, payload))
            except Exception as e:
                # WE ADDED THIS PRINT STATEMENT
                print(f"⚠️ PYTHON PARSE ERROR: {e}")

        try:
            try:
                client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="MQTT_Raw_Heavy")
            except AttributeError:
                client = mqtt.Client(client_id="MQTT_Raw_Heavy")
            client.on_connect = on_connect
            client.on_message = on_message
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            self.packet_queue.put(("LOG", f"CRITICAL: {str(e)}"))

    def _ui_consumer_tick(self):
        latest_packet = None
        queue_backlog = self.packet_queue.qsize()

        while not self.packet_queue.empty():
            item = self.packet_queue.get_nowait()
            if item[0] == "LOG":
                self.log_message(item[1])
            elif item[0] == "PC_DATA":
                latest_packet = item

        if queue_backlog > 5:
            self.log_message(f"⚠️ WARNING: Broker packet backlog detected! Queue size: {queue_backlog} frames.")

        if latest_packet:
            _, recv_time, payload = latest_packet
            
            # Expecting payload to contain arrays of 'x', 'y', 'z' coordinates
            # Structured as: {"timestamp": 12345, "x": [...], "y": [...], "z": [...]}
            xs = payload.get("x", [])
            ys = payload.get("y", [])
            zs = payload.get("z", [])
            self.cached_xyz = (xs, ys, zs)

            # --- WITH THIS NEW SAFE VERSION ---
            raw_ts = payload.get("timestamp", 0.0)
            sensor_epoch = 0.0
            
            if isinstance(raw_ts, str):
                try:
                    # Handle both ISO strings and stringified numbers
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

            if sensor_epoch > 0:
                transit_ms = abs(recv_time - sensor_epoch) * 1000
                if transit_ms > 100:
                    self.log_message(f"🛑 CRITICAL DELAY: Payload transit bottleneck: {transit_ms:.1f}ms")

            # Graph painting limit
            now = time.time()
            if (now - self.last_ui_paint) >= 0.4 and len(xs) > 0:
                self.last_ui_paint = now
                self.ax.clear()
                self.ax.set_facecolor("#1e1e2e")
                self.ax.tick_params(colors="white", labelsize=7)

                # Heavy downsampling to prevent GUI lockup
                step = max(1, len(xs) // 1500)
                sub_x, sub_y, sub_z = xs[::step], ys[::step], zs[::step]

                self.ax.scatter(sub_x, sub_y, sub_z, c=sub_y, cmap="viridis", s=3, alpha=0.8, edgecolors="none")
                self.ax.view_init(elev=22, azim=-45)
                self.ax.set_title(f"MQTT Pipe: Rendering {len(sub_x)} of {len(xs)} points", color="white", fontsize=9)
                self.canvas.draw_idle()

        self.root.after(100, self._ui_consumer_tick)

if __name__ == "__main__":
    window = tk.Tk()
    app = MqttRawDashboardApp(window)
    window.mainloop()