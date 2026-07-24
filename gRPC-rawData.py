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
import numpy as np
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

# ==========================================
# --- CONFIGURATION ---
# ==========================================
LIDAR_IP = "192.168.26.26"
API_KEY = "2ee812bc2e745dddb8i1cmJwrEaz8ehy"
PROBE_PORT = 50051  # Target the Blickfeld gRPC service socket directly

class GrpcBenchmarkApp:
    def __init__(self, root):
        self.root = root
        self.root.title("gRPC Raw Data Monitor")
        self.root.geometry("1100x600")
        self.root.configure(bg="#1e1e2e")

        self.packet_queue = queue.Queue()
        self.bench_active = False
        self.bench_end_time = 0
        self.live_wire_latency_ms = 0.000 
        
        self.bench_hw_latencies = []
        self.bench_net_latencies = []
        self.bench_tot_latencies = []
        self.bench_points_count = []
        
        self.bench_bytes_received = 0
        self.bench_skipped_frames = 0
        self.last_sensor_epoch = 0.0
        self.bench_first_sensor = 0.0
        self.bench_first_recv = 0.0

        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=3)
        self.root.columnconfigure(1, weight=2)

        self._build_ui()

        threading.Thread(target=self._grpc_pointcloud_producer, daemon=True).start()
        threading.Thread(target=self._wire_latency_probe, daemon=True).start()

        self.root.after(100, self._ui_consumer_tick)

    def _build_ui(self):
        pc_frame = tk.LabelFrame(self.root, text=" Live gRPC 3D Laser Stream ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
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
        self.dur_entry = tk.Entry(ctrl_bar, width=5, font=("Consolas", 11, "bold"), bg="#181825", fg="#89b4fa", insertbackground="white")
        self.dur_entry.insert(0, "10")
        self.dur_entry.pack(side=tk.LEFT, padx=5)

        self.start_btn = tk.Button(ctrl_bar, text="START", font=("Arial", 10, "bold"), bg="#89b4fa", fg="#11111b", command=self._start_timed_benchmark, relief="flat")
        self.start_btn.pack(side=tk.LEFT, padx=10)
        
        self.wire_status_lbl = tk.Label(ctrl_bar, text="Wire Speed: Probing...", font=("Consolas", 9, "bold"), bg="#252538", fg="#f9e2af")
        self.wire_status_lbl.pack(side=tk.RIGHT, padx=5)

        self.bench_status = tk.Label(bench_frame, text="Status: Ready to record network telemetry...", font=("Arial", 10, "italic"), bg="#1e1e2e", fg="#a6adc8")
        self.bench_status.grid(row=1, column=0, sticky="w", padx=10, pady=2)

        self.raw_log = tk.Text(bench_frame, bg="#181825", fg="#bac2de", font=("Consolas", 10), state="disabled", wrap="word")
        self.raw_log.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)
        scroll = ttk.Scrollbar(bench_frame, orient="vertical", command=self.raw_log.yview)
        self.raw_log.configure(yscrollcommand=scroll.set)
        scroll.grid(row=2, column=1, sticky="ns")

    def log_message(self, msg):
        self.raw_log.configure(state="normal")
        self.raw_log.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}\n")
        self.raw_log.see(tk.END)
        self.raw_log.configure(state="disabled")

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
                self.wire_status_lbl.configure(text=f"Live Wire Speed: {self.live_wire_latency_ms:.3f} ms", fg="#a6e3a1")
            except Exception:
                self.wire_status_lbl.configure(text="Wire Speed: Probe Timeout", fg="#f38ba8")
            time.sleep(1.0)

    def _start_timed_benchmark(self):
        if self.bench_active: return
        try: dur = float(self.dur_entry.get())
        except ValueError: return

        self.bench_active = True
        self.bench_end_time = time.time() + dur
        self.bench_hw_latencies, self.bench_net_latencies, self.bench_tot_latencies, self.bench_points_count = [], [], [], []
        self.bench_bytes_received, self.bench_skipped_frames = 0, 0
        self.last_sensor_epoch = 0.0
        self.bench_first_sensor = 0.0
        self.bench_first_recv = 0.0
        
        self.start_btn.configure(state="disabled", text="RUNNING...", bg="#f38ba8", disabledforeground="#11111b")
        self.bench_status.configure(text=f"Status: Recording packets for {dur}s...", fg="#f9e2af")
        self.log_message(f"\n=== STARTING {dur}s ADVANCED gRPC POINT CLOUD BENCHMARK ===")

    def _extract_xyz(self, frame):
        try:
            if hasattr(frame, "binary") and hasattr(frame.binary, "cartesian"):
                xyz = frame.binary.cartesian
                if xyz is not None and len(xyz) > 0:
                    return xyz[:, 0], xyz[:, 1], xyz[:, 2]
        except Exception: pass
        return [], [], []

    def _grpc_pointcloud_producer(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            token_factory = blickfeld_qb2.TokenFactory(application_key_secret=API_KEY)
            with blickfeld_qb2.Channel(fqdn_or_ip=LIDAR_IP, token=token_factory) as channel:
                service = blickfeld_qb2.core_processing.services.PointCloud(channel)
                self.packet_queue.put(("LOG", "ONLINE: Connected to gRPC Raw Point Cloud stream"))
                for response in service.stream():
                    frame_bytes = getattr(response.frame, "ByteSize", lambda: len(response.frame.binary.cartesian) * 16)()
                    self.packet_queue.put(("PC", time.time(), response.frame, frame_bytes))
        except Exception as e:
            self.packet_queue.put(("LOG", f"gRPC ERROR: {str(e)}"))

    def _ui_consumer_tick(self):
        latest_pc = None
        while not self.packet_queue.empty():
            item = self.packet_queue.get_nowait()
            if item[0] == "LOG": self.log_message(item[1])
            elif item[0] == "PC": latest_pc = item

        if latest_pc:
            _, pc_wire_time, pc_frame, frame_bytes = latest_pc
            xs, ys, zs = self._extract_xyz(pc_frame)

            raw_ts = getattr(pc_frame, "timestamp", None) or 0.0
            pc_sensor_epoch = float(raw_ts) / 1e9 if float(raw_ts) > 1e16 else float(raw_ts)

            if self.bench_active and time.time() <= self.bench_end_time:
                if pc_sensor_epoch > 0:
                    if self.bench_first_sensor == 0.0:
                        self.bench_first_sensor = pc_sensor_epoch
                        self.bench_first_recv = pc_wire_time

                    tot_ms = abs(pc_wire_time - pc_sensor_epoch) * 1000
                    net_ms = self.live_wire_latency_ms
                    hw_compute_ms = max(0.0, tot_ms - net_ms)

                    self.bench_hw_latencies.append(hw_compute_ms)
                    self.bench_net_latencies.append(net_ms)
                    self.bench_tot_latencies.append(tot_ms)
                    self.bench_points_count.append(len(xs))
                    self.bench_bytes_received += frame_bytes
                    
                    if self.last_sensor_epoch > 0 and (pc_sensor_epoch - self.last_sensor_epoch) > 0.180:
                        self.bench_skipped_frames += 1
                    self.last_sensor_epoch = pc_sensor_epoch

                    # LIVE FRAME PRINT OUT
                    sense_str = datetime.fromtimestamp(pc_sensor_epoch).strftime('%H:%M:%S.%f')[:-3]
                    recv_str = datetime.fromtimestamp(pc_wire_time).strftime('%H:%M:%S.%f')[:-3]
                    self.log_message(f"RECV | Sense: {sense_str} -> Recv: {recv_str} | Pts: {len(xs)} | Sensing: {hw_compute_ms:.1f}ms | Net: {net_ms:.1f}ms | Tot: {tot_ms:.1f}ms")

            if len(xs) > 0:
                self.ax.clear()
                self.ax.set_facecolor("#1e1e2e")
                self.ax.tick_params(colors="white", labelsize=7)
                step = max(1, len(xs) // 4000)
                sub_x, sub_y, sub_z = xs[::step], ys[::step], zs[::step]
                self.ax.scatter(sub_x, sub_y, sub_z, c=sub_y, cmap="plasma_r", s=2.5, alpha=0.75, edgecolors="none")
                max_rng = max(max(sub_x)-min(sub_x), max(sub_y)-min(sub_y), max(sub_z)-min(sub_z))
                if max_rng > 0:
                    self.ax.set_box_aspect(((max(sub_x)-min(sub_x))/max_rng, (max(sub_y)-min(sub_y))/max_rng, (max(sub_z)-min(sub_z))/max_rng))
                self.ax.view_init(elev=22, azim=-45)
                self.canvas.draw_idle()

        # --- TIMER DECOUPLED FROM PACKET STREAM ---
        if self.bench_active:
            now_time = time.time()
            if now_time <= self.bench_end_time:
                time_left = max(0.0, self.bench_end_time - now_time)
                self.start_btn.configure(text=f"RUNNING ({time_left:.1f}s)")
            else:
                self.bench_active = False
                self.start_btn.configure(state="normal", text="START", bg="#89b4fa", fg="#11111b")
                self.bench_status.configure(text="Status: Benchmark complete!", fg="#a6e3a1")

                if len(self.bench_tot_latencies) > 0:
                    dur_actual = float(self.dur_entry.get())
                    first_sensor_time = datetime.fromtimestamp(self.bench_first_sensor).strftime('%H:%M:%S.%f')[:-3] if self.bench_first_sensor > 0 else "N/A"
                    first_recv_time = datetime.fromtimestamp(self.bench_first_recv).strftime('%H:%M:%S.%f')[:-3] if self.bench_first_recv > 0 else "N/A"
                    
                    avg_hw = sum(self.bench_hw_latencies) / len(self.bench_hw_latencies)
                    avg_net = sum(self.bench_net_latencies) / len(self.bench_net_latencies)
                    avg_tot = sum(self.bench_tot_latencies) / len(self.bench_tot_latencies)
                    
                    hw_jitter = self._calc_jitter(self.bench_hw_latencies)
                    net_jitter = self._calc_jitter(self.bench_net_latencies)
                    tot_jitter = self._calc_jitter(self.bench_tot_latencies)
                    
                    mbps = (self.bench_bytes_received / (1024 * 1024)) / dur_actual
                    fps = len(self.bench_tot_latencies) / dur_actual
                    loss_rate = (self.bench_skipped_frames / (len(self.bench_tot_latencies) + self.bench_skipped_frames)) * 100
                    
                    summary = (
                        f"\n=== ADVANCED 3D POINT CLOUD TELEMETRY ({len(self.bench_tot_latencies)} Frames over {dur_actual}s) ===\n"
                        f" ├── Stream Start        : Optical @ {first_sensor_time} -> Recv @ {first_recv_time}\n"
                        f" ├── Avg Sensing Latency (HW Scan)     : {avg_hw:.3f} ms (Jitter σ: ±{hw_jitter:.3f} ms)\n"
                        f" ├── Avg Network Latency (Wire Speed)   : {avg_net:.3f} ms (Jitter σ: ±{net_jitter:.3f} ms)\n"
                        f" ├── Avg Total End-to-End Latency      : {avg_tot:.3f} ms (Jitter σ: ±{tot_jitter:.3f} ms)\n"
                        f" ├── Throughput (Raw 3D Binary)        : {mbps:.2f} MB/s ({fps:.1f} FPS | {sum(self.bench_points_count)/len(self.bench_points_count):.0f} pts/frame)\n"
                        f" └── Packet Loss (Skipped Optical Gaps) : {self.bench_skipped_frames} frames ({loss_rate:.1f}% loss)\n"
                        f"========================================================================================"
                    )
                    self.log_message(summary)

        self.root.after(100, self._ui_consumer_tick)

if __name__ == "__main__":
    window = tk.Tk()
    app = GrpcBenchmarkApp(window)
    window.mainloop()