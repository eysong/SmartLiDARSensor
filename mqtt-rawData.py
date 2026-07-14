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
MQTT_TOPIC = "blickfeld/raw_pointcloud"

class MqttRawBenchmarkApp:
    def __init__(self, root):
        self.root = root
        self.root.title("⚠️ WARNING: Raw Point Cloud via MQTT Benchmark")
        self.root.geometry("1100x600")
        self.root.configure(bg="#1e1e2e")

        self.packet_queue = queue.Queue()
        self.cached_xyz = ([], [], [])
        self.last_ui_paint = 0

        # Benchmark State Variables
        self.bench_active = False
        self.bench_end_time = 0
        
        # --- NEW: SPLIT-LATENCY TRACKING LISTS ---
        self.bench_proc_latencies = []
        self.bench_net_latencies = []
        self.bench_tot_latencies = []
        self.bench_points_count = []

        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=3)
        self.root.columnconfigure(1, weight=2)

        self._build_ui()

        self.mqtt_thread = threading.Thread(target=self._mqtt_subscriber_thread, daemon=True)
        self.mqtt_thread.start()

        self.root.after(100, self._ui_consumer_tick)

    def _build_ui(self):
        pc_frame = tk.LabelFrame(self.root, text=" Unpacked MQTT 3D Matrix Stream ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        pc_frame.grid(row=0, column=0, sticky="nsew", padx=15, pady=10)

        self.fig = Figure(figsize=(6, 5), dpi=90, facecolor="#1e1e2e")
        self.ax = self.fig.add_subplot(111, projection="3d", facecolor="#1e1e2e")
        self.ax.tick_params(colors="white", labelsize=7)
        self.canvas = FigureCanvasTkAgg(self.fig, master=pc_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        bench_frame = tk.LabelFrame(self.root, text=" Network Benchmarker ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        bench_frame.grid(row=0, column=1, sticky="nsew", padx=(0, 15), pady=10)
        bench_frame.rowconfigure(2, weight=1)
        bench_frame.columnconfigure(0, weight=1)

        # Control Bar
        ctrl_bar = tk.Frame(bench_frame, bg="#252538", pady=10, padx=10)
        ctrl_bar.grid(row=0, column=0, sticky="ew", padx=5, pady=5)

        tk.Label(ctrl_bar, text="Duration (sec):", font=("Arial", 10, "bold"), bg="#252538", fg="white").pack(side=tk.LEFT, padx=5)
        self.dur_entry = tk.Entry(ctrl_bar, width=6, font=("Consolas", 11, "bold"), bg="#181825", fg="#89b4fa", insertbackground="white")
        self.dur_entry.insert(0, "10")
        self.dur_entry.pack(side=tk.LEFT, padx=5)

        self.start_btn = tk.Button(ctrl_bar, text="▶ START BENCHMARK", font=("Arial", 10, "bold"), bg="#89b4fa", fg="#11111b", command=self._start_timed_benchmark, relief="flat")
        self.start_btn.pack(side=tk.RIGHT, padx=5)

        self.bench_status = tk.Label(bench_frame, text="Status: Waiting for MQTT stream...", font=("Arial", 10, "italic"), bg="#1e1e2e", fg="#a6adc8")
        self.bench_status.grid(row=1, column=0, sticky="w", padx=10, pady=2)

        self.log_widget = tk.Text(bench_frame, bg="#181825", fg="#f38ba8", font=("Consolas", 10), state="disabled", wrap="word")
        self.log_widget.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)
        scroll = ttk.Scrollbar(bench_frame, orient="vertical", command=self.log_widget.yview)
        self.log_widget.configure(yscrollcommand=scroll.set)
        scroll.grid(row=2, column=1, sticky="ns")

    def log_message(self, msg):
        self.log_widget.configure(state="normal")
        self.log_widget.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}\n")
        self.log_widget.see(tk.END)
        self.log_widget.configure(state="disabled")

    def _start_timed_benchmark(self):
        if self.bench_active:
            return
        try:
            dur = float(self.dur_entry.get())
        except ValueError:
            return

        self.bench_active = True
        self.bench_end_time = time.time() + dur
        # Reset split latency lists
        self.bench_proc_latencies, self.bench_net_latencies, self.bench_tot_latencies, self.bench_points_count = [], [], [], []
        self.start_btn.configure(state="disabled", text="⏳ RUNNING...", bg="#f38ba8")
        self.bench_status.configure(text=f"Status: Recording packets for {dur}s...", fg="#f9e2af")
        self.log_message(f"\n=== STARTING {dur}s MQTT NETWORK BENCHMARK ===")

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
            
            xs = payload.get("x", [])
            ys = payload.get("y", [])
            zs = payload.get("z", [])
            self.cached_xyz = (xs, ys, zs)

            # --- 1. EXTRACT SENSING TIME (sensor_epoch) ---
            raw_ts = payload.get("timestamp", 0.0)
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

            # --- 2. EXTRACT SENDING TIME (send_epoch) ---
            raw_send_ts = payload.get("send_time", 0.0)
            send_epoch = 0.0
            if raw_send_ts:
                if isinstance(raw_send_ts, str):
                    try: send_epoch = datetime.fromisoformat(raw_send_ts.replace("Z", "+00:00")).timestamp()
                    except Exception: pass
                else:
                    try: send_epoch = float(raw_send_ts) / 1e9 if float(raw_send_ts) > 1e16 else (float(raw_send_ts) / 1e3 if float(raw_send_ts) > 1e10 else float(raw_send_ts))
                    except (ValueError, TypeError): pass
            
            if send_epoch == 0.0 and sensor_epoch > 0:
                send_epoch = sensor_epoch

            if self.bench_active:
                if time.time() <= self.bench_end_time:
                    if sensor_epoch > 0:
                        # --- 3. SPLIT-LATENCY MATH ---
                        proc_ms = abs(send_epoch - sensor_epoch) * 1000         # Node-RED JS Unpacker & JSON Formatting Tax
                        net_ms = abs(recv_time - send_epoch) * 1000             # Wire / Broker Socket Queue Delay
                        tot_ms = abs(recv_time - sensor_epoch) * 1000           # Full Pipeline

                        self.bench_proc_latencies.append(proc_ms)
                        self.bench_net_latencies.append(net_ms)
                        self.bench_tot_latencies.append(tot_ms)
                        self.bench_points_count.append(len(xs))
                else:
                    self.bench_active = False
                    self.start_btn.configure(state="normal", text="▶ START BENCHMARK", bg="#89b4fa")
                    self.bench_status.configure(text="Status: Benchmark complete!", fg="#a6e3a1")

                    if len(self.bench_tot_latencies) > 0:
                        first_sensor_time = datetime.fromtimestamp(sensor_epoch).strftime('%H:%M:%S.%f')[:-3]
                        first_recv_time = datetime.fromtimestamp(recv_time).strftime('%H:%M:%S.%f')[:-3]
                        
                        avg_proc = sum(self.bench_proc_latencies) / len(self.bench_proc_latencies)
                        avg_net = sum(self.bench_net_latencies) / len(self.bench_net_latencies)
                        avg_tot = sum(self.bench_tot_latencies) / len(self.bench_tot_latencies)
                        max_tot = max(self.bench_tot_latencies)
                        
                        summary = (
                            f"\n=== BENCHMARK SYNC RESULTS ({len(self.bench_tot_latencies)} Frames) ===\n"
                            f" ├── Stream Start: measurementTime @ {first_sensor_time} -> Receipt Time @ {first_recv_time}\n"
                            f" ├── Avg Middleware CPU Tax : {avg_proc:.2f} ms (JS Unpacker Tax)\n"
                            f" ├── Avg Network/Buffer Tax : {avg_net:.2f} ms (Broker Wire Delay)\n"
                            f" ├── Total Pipeline Latency : {avg_tot:.2f} ms (Max: {max_tot:.2f} ms)\n"
                            f" └── pos (Points) Avg       : {sum(self.bench_points_count)/len(self.bench_points_count):.0f} points per frame\n"
                            f"========================================================="
                        )
                        self.log_message(summary)

            now = time.time()
            if (now - self.last_ui_paint) >= 0.4 and len(xs) > 0:
                self.last_ui_paint = now
                self.ax.clear()
                self.ax.set_facecolor("#1e1e2e")
                self.ax.tick_params(colors="white", labelsize=7)

                step = max(1, len(xs) // 1500)
                sub_x, sub_y, sub_z = xs[::step], ys[::step], zs[::step]

                self.ax.scatter(sub_x, sub_y, sub_z, c=sub_y, cmap="viridis", s=3, alpha=0.8, edgecolors="none")
                self.ax.view_init(elev=22, azim=-45)
                self.ax.set_title(f"MQTT Pipe: Rendering {len(sub_x)} of {len(xs)} points", color="white", fontsize=9)
                self.canvas.draw_idle()

        self.root.after(100, self._ui_consumer_tick)

if __name__ == "__main__":
    window = tk.Tk()
    app = MqttRawBenchmarkApp(window)
    window.mainloop()