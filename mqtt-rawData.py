import json
import math
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
        self.root.title("MQTT Raw Data Monitor")
        self.root.geometry("1100x600")
        self.root.configure(bg="#1e1e2e")

        self.packet_queue = queue.Queue()
        self.cached_xyz = ([], [], [])
        self.last_ui_paint = 0

        self.bench_active = False
        self.bench_end_time = 0
        
        self.bench_proc_latencies = []
        self.bench_net_latencies = []
        self.bench_tot_latencies = []
        self.bench_points_count = []
        
        self.bench_bytes_received = 0
        self.bench_backlog_drops = 0
        self.last_sensor_epoch = 0.0
        self.bench_first_sensor = 0.0
        self.bench_first_recv = 0.0

        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=3)
        self.root.columnconfigure(1, weight=2)

        self._build_ui()

        threading.Thread(target=self._mqtt_subscriber_thread, daemon=True).start()
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

        ctrl_bar = tk.Frame(bench_frame, bg="#252538", pady=10, padx=10)
        ctrl_bar.grid(row=0, column=0, sticky="ew", padx=5, pady=5)

        tk.Label(ctrl_bar, text="Duration (s):", font=("Arial", 10, "bold"), bg="#252538", fg="white").pack(side=tk.LEFT, padx=5)
        self.dur_entry = tk.Entry(ctrl_bar, width=6, font=("Consolas", 11, "bold"), bg="#181825", fg="#89b4fa", insertbackground="white")
        self.dur_entry.insert(0, "10")
        self.dur_entry.pack(side=tk.LEFT, padx=5)

        self.start_btn = tk.Button(ctrl_bar, text="START BENCHMARK", font=("Arial", 10, "bold"), bg="#89b4fa", fg="#11111b", command=self._start_timed_benchmark, relief="flat")
        self.start_btn.pack(side=tk.RIGHT, padx=5)

        self.bench_status = tk.Label(bench_frame, text="Status: Waiting for MQTT stream...", font=("Arial", 10, "italic"), bg="#1e1e2e", fg="#a6adc8")
        self.bench_status.grid(row=1, column=0, sticky="w", padx=10, pady=2)

        self.log_widget = tk.Text(bench_frame, bg="#181825", fg="#bac2de", font=("Consolas", 10), state="disabled", wrap="word")
        self.log_widget.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)
        scroll = ttk.Scrollbar(bench_frame, orient="vertical", command=self.log_widget.yview)
        self.log_widget.configure(yscrollcommand=scroll.set)
        scroll.grid(row=2, column=1, sticky="ns")

    def log_message(self, msg):
        self.log_widget.configure(state="normal")
        self.log_widget.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}\n")
        self.log_widget.see(tk.END)
        self.log_widget.configure(state="disabled")

    def _calc_jitter(self, latencies):
        if len(latencies) < 2: return 0.0
        avg = sum(latencies) / len(latencies)
        variance = sum((x - avg) ** 2 for x in latencies) / len(latencies)
        return math.sqrt(variance)

    def _start_timed_benchmark(self):
        if self.bench_active: return
        try: dur = float(self.dur_entry.get())
        except ValueError: return

        self.bench_active = True
        self.bench_end_time = time.time() + dur
        self.bench_proc_latencies, self.bench_net_latencies, self.bench_tot_latencies, self.bench_points_count = [], [], [], []
        self.bench_bytes_received, self.bench_backlog_drops = 0, 0
        self.last_sensor_epoch = 0.0
        self.bench_first_sensor = 0.0
        self.bench_first_recv = 0.0
        
        self.start_btn.configure(state="disabled", text="RUNNING...", bg="#f38ba8", disabledforeground="#11111b")
        self.bench_status.configure(text=f"Status: Recording packets for {dur}s...", fg="#f9e2af")
        self.log_message(f"\n=== STARTING {dur}s ADVANCED MQTT POINT CLOUD BENCHMARK ===")

    def _mqtt_subscriber_thread(self):
        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                self.packet_queue.put(("LOG", f"CONNECTED: Listening to stream on '{MQTT_TOPIC}'..."))
                client.subscribe(MQTT_TOPIC)
            else: self.packet_queue.put(("LOG", f"BROKER REFUSED CONNECTION: Code {rc}"))

        def on_message(client, userdata, msg):
            try:
                t_recv = time.time()
                byte_size = len(msg.payload)
                payload = json.loads(msg.payload.decode("utf-8"))
                self.packet_queue.put(("PC_DATA", t_recv, payload, byte_size))
            except Exception as e: print(f"PYTHON PARSE ERROR: {e}")

        try:
            try: client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="MQTT_Raw_Dash")
            except AttributeError: client = mqtt.Client(client_id="MQTT_Raw_Dash")
            client.on_connect = on_connect
            client.on_message = on_message
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e: self.packet_queue.put(("LOG", f"CRITICAL: {str(e)}"))

    def _ui_consumer_tick(self):
        latest_packet = None
        queue_backlog = self.packet_queue.qsize()

        while not self.packet_queue.empty():
            item = self.packet_queue.get_nowait()
            if item[0] == "LOG": self.log_message(item[1])
            elif item[0] == "PC_DATA": latest_packet = item

        if queue_backlog > 5:
            self.log_message(f"Broker packet backlog detected! Queue size: {queue_backlog} frames.")
            if self.bench_active and time.time() <= self.bench_end_time:
                self.bench_backlog_drops += max(0, queue_backlog - 1)

        if latest_packet:
            _, recv_time, payload, byte_size = latest_packet
            xs, ys, zs = payload.get("x", []), payload.get("y", []), payload.get("z", [])
            self.cached_xyz = (xs, ys, zs)

            raw_ts = (
                payload.get("timestamp") or
                payload.get("frame", {}).get("timestamp") or
                payload.get("objects", {}).get("timestamp") or
                payload.get("objects", {}).get("metadata", {}).get("timestamp") or
                0.0
            )

            sensor_epoch = 0.0
            if isinstance(raw_ts, str):
                try:
                    if "T" in raw_ts:
                        sensor_epoch = datetime.fromisoformat(raw_ts.replace("Z", "+00:00")).timestamp()
                    else:
                        val = float(raw_ts)
                        sensor_epoch = val / 1e9 if val > 1e16 else (val / 1e6 if val > 1e13 else (val / 1e3 if val > 1e10 else val))
                except Exception:
                    pass
            elif isinstance(raw_ts, (int, float)):
                try:
                    val = float(raw_ts)
                    sensor_epoch = val / 1e9 if val > 1e16 else (val / 1e6 if val > 1e13 else (val / 1e3 if val > 1e10 else val))
                except Exception:
                    pass

            raw_send_ts = payload.get("send_time", 0.0)
            send_epoch = 0.0
            if raw_send_ts:
                try: 
                    if isinstance(raw_send_ts, str) and "T" in raw_send_ts:
                        send_epoch = datetime.fromisoformat(raw_send_ts.replace("Z", "+00:00")).timestamp()
                    else:
                        val = float(raw_send_ts)
                        send_epoch = val / 1e9 if val > 1e16 else (val / 1e6 if val > 1e13 else (val / 1e3 if val > 1e10 else val))
                except Exception: 
                    pass
            
            if send_epoch == 0.0 and sensor_epoch > 0: send_epoch = sensor_epoch

            if self.bench_active and time.time() <= self.bench_end_time:
                if sensor_epoch > 0:
                    if self.bench_first_sensor == 0.0:
                        self.bench_first_sensor = sensor_epoch
                        self.bench_first_recv = recv_time

                    proc_ms = abs(send_epoch - sensor_epoch) * 1000
                    net_ms = abs(recv_time - send_epoch) * 1000
                    tot_ms = abs(recv_time - sensor_epoch) * 1000

                    self.bench_proc_latencies.append(proc_ms)
                    self.bench_net_latencies.append(net_ms)
                    self.bench_tot_latencies.append(tot_ms)
                    self.bench_points_count.append(len(xs))
                    self.bench_bytes_received += byte_size

                    # LIVE FRAME PRINT OUT
                    sense_str = datetime.fromtimestamp(sensor_epoch).strftime('%H:%M:%S.%f')[:-3]
                    recv_str = datetime.fromtimestamp(recv_time).strftime('%H:%M:%S.%f')[:-3]
                    self.log_message(f"RECV | Sense: {sense_str} -> Recv: {recv_str} | Pts: {len(xs)} | Proc: {proc_ms:.1f}ms | Net: {net_ms:.1f}ms | Tot: {tot_ms:.1f}ms")

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

        # --- TIMER DECOUPLED FROM PACKET STREAM ---
        if self.bench_active:
            now_time = time.time()
            if now_time <= self.bench_end_time:
                time_left = max(0.0, self.bench_end_time - now_time)
                self.start_btn.configure(text=f"RUNNING ({time_left:.1f}s)")
            else:
                self.bench_active = False
                self.start_btn.configure(state="normal", text="START BENCHMARK", bg="#89b4fa", fg="#11111b")
                self.bench_status.configure(text="Status: Benchmark complete!", fg="#a6e3a1")

                if len(self.bench_tot_latencies) > 0:
                    dur_actual = float(self.dur_entry.get())
                    first_sensor = datetime.fromtimestamp(self.bench_first_sensor).strftime('%H:%M:%S.%f')[:-3] if self.bench_first_sensor > 0 else "N/A"
                    first_recv = datetime.fromtimestamp(self.bench_first_recv).strftime('%H:%M:%S.%f')[:-3] if self.bench_first_recv > 0 else "N/A"
                    
                    avg_proc = sum(self.bench_proc_latencies) / len(self.bench_proc_latencies)
                    avg_net = sum(self.bench_net_latencies) / len(self.bench_net_latencies)
                    avg_tot = sum(self.bench_tot_latencies) / len(self.bench_tot_latencies)
                    
                    proc_jitter = self._calc_jitter(self.bench_proc_latencies)
                    net_jitter = self._calc_jitter(self.bench_net_latencies)
                    tot_jitter = self._calc_jitter(self.bench_tot_latencies)
                    
                    mbps = (self.bench_bytes_received / (1024 * 1024)) / dur_actual
                    fps = len(self.bench_tot_latencies) / dur_actual
                    tot_frames = len(self.bench_tot_latencies) + self.bench_backlog_drops
                    loss_rate = (self.bench_backlog_drops / tot_frames) * 100 if tot_frames > 0 else 0
                    
                    summary = (
                        f"\n=== ADVANCED MQTT POINT CLOUD TELEMETRY ({len(self.bench_tot_latencies)} Frames over {dur_actual}s) ===\n"
                        f" ├── Stream Start        : Optical @ {first_sensor} -> Recv @ {first_recv}\n"
                        f" ├── Avg Sensing / Middleware Latency : {avg_proc:.2f} ms (Jitter σ: ±{proc_jitter:.2f} ms)\n"
                        f" ├── Avg Network Wire Transit Latency  : {avg_net:.2f} ms (Jitter σ: ±{net_jitter:.2f} ms)\n"
                        f" ├── Avg Total End-to-End Latency      : {avg_tot:.2f} ms (Jitter σ: ±{tot_jitter:.2f} ms)\n"
                        f" ├── Throughput (JSON Text Bandwidth)  : {mbps:.2f} MB/s ({fps:.1f} FPS)\n"
                        f" └── Packet Loss (Socket Queue Overflow): {self.bench_backlog_drops} frames ({loss_rate:.1f}% loss)\n"
                        f"=========================================================================================="
                    )
                    self.log_message(summary)

        self.root.after(100, self._ui_consumer_tick)

if __name__ == "__main__":
    window = tk.Tk()
    app = MqttRawBenchmarkApp(window)
    window.mainloop()