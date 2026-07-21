import asyncio
import queue
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

# Calibrated via SSH tcpdump network benchmark (Wire transit time in ms)
TCPDUMP_WIRE_LATENCY_MS = 0.2

class GrpcBenchmarkApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Blickfeld: gRPC Raw Benchmark (tcpdump Calibrated)")
        self.root.geometry("1100x600")
        self.root.configure(bg="#1e1e2e")

        self.packet_queue = queue.Queue()
        self.bench_active = False
        self.bench_end_time = 0
        
        self.bench_hw_latencies = []
        self.bench_net_latencies = []
        self.bench_tot_latencies = []
        self.bench_points_count = []

        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=3)
        self.root.columnconfigure(1, weight=2)

        self._build_ui()

        self.pointcloud_thread = threading.Thread(target=self._grpc_pointcloud_producer, daemon=True)
        self.pointcloud_thread.start()

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

        tk.Label(ctrl_bar, text="Duration (sec):", font=("Arial", 10, "bold"), bg="#252538", fg="white").pack(side=tk.LEFT, padx=5)
        self.dur_entry = tk.Entry(ctrl_bar, width=6, font=("Consolas", 11, "bold"), bg="#181825", fg="#89b4fa", insertbackground="white")
        self.dur_entry.insert(0, "10")
        self.dur_entry.pack(side=tk.LEFT, padx=5)

        self.start_btn = tk.Button(ctrl_bar, text="▶ START BENCHMARK", font=("Arial", 10, "bold"), bg="#89b4fa", fg="#11111b", command=self._start_timed_benchmark, relief="flat")
        self.start_btn.pack(side=tk.RIGHT, padx=5)

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

    def _start_timed_benchmark(self):
        if self.bench_active:
            return
        try:
            dur = float(self.dur_entry.get())
        except ValueError:
            return

        self.bench_active = True
        self.bench_end_time = time.time() + dur
        self.bench_hw_latencies, self.bench_net_latencies, self.bench_tot_latencies, self.bench_points_count = [], [], [], []
        self.start_btn.configure(state="disabled", text="⏳ RUNNING...", bg="#f38ba8")
        self.bench_status.configure(text=f"Status: Recording packets for {dur}s...", fg="#f9e2af")
        self.log_message(f"\n=== STARTING {dur}s gRPC NETWORK BENCHMARK ===")

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
                    self.packet_queue.put(("PC", time.time(), response.frame))
        except Exception as e:
            self.packet_queue.put(("LOG", f"gRPC ERROR: {str(e)}"))

    def _ui_consumer_tick(self):
        latest_pc = None

        while not self.packet_queue.empty():
            item = self.packet_queue.get_nowait()
            if item[0] == "LOG":
                self.log_message(item[1])
            elif item[0] == "PC":
                latest_pc = item

        if latest_pc:
            _, pc_wire_time, pc_frame = latest_pc
            xs, ys, zs = self._extract_xyz(pc_frame)

            raw_ts = getattr(pc_frame, "timestamp", None) or 0.0
            pc_sensor_epoch = float(raw_ts) / 1e9 if float(raw_ts) > 1e16 else float(raw_ts)

            if self.bench_active:
                if time.time() <= self.bench_end_time:
                    if pc_sensor_epoch > 0:
                        tot_ms = abs(pc_wire_time - pc_sensor_epoch) * 1000
                        net_ms = TCPDUMP_WIRE_LATENCY_MS
                        hw_compute_ms = max(0.0, tot_ms - net_ms)

                        self.bench_hw_latencies.append(hw_compute_ms)
                        self.bench_net_latencies.append(net_ms)
                        self.bench_tot_latencies.append(tot_ms)
                        self.bench_points_count.append(len(xs))
                else:
                    self.bench_active = False
                    self.start_btn.configure(state="normal", text="▶ START BENCHMARK", bg="#89b4fa")
                    self.bench_status.configure(text="Status: Benchmark complete!", fg="#a6e3a1")

                    if len(self.bench_tot_latencies) > 0:
                        first_sensor_time = datetime.fromtimestamp(pc_sensor_epoch).strftime('%H:%M:%S.%f')[:-3]
                        first_recv_time = datetime.fromtimestamp(pc_wire_time).strftime('%H:%M:%S.%f')[:-3]
                        
                        avg_hw = sum(self.bench_hw_latencies) / len(self.bench_hw_latencies)
                        avg_net = sum(self.bench_net_latencies) / len(self.bench_net_latencies)
                        avg_tot = sum(self.bench_tot_latencies) / len(self.bench_tot_latencies)
                        max_tot = max(self.bench_tot_latencies)
                        
                        summary = (
                            f"\n=== BENCHMARK RESULTS ({len(self.bench_tot_latencies)} Frames Recorded) ===\n"
                            f" ├── Stream Start        : SOF Optical Time @ {first_sensor_time} -> Recv Time @ {first_recv_time}\n"
                            f" ├── Avg HW Scan & AI    : {avg_hw:.2f} ms (Optical sweep + FPGA + On-Device C++ AI)\n"
                            f" ├── Avg Net Transit     : {avg_net:.2f} ms (Calibrated via SSH tcpdump)\n"
                            f" ├── Total System Latency: {avg_tot:.2f} ms (Max: {max_tot:.2f} ms)\n"
                            f" └── Points Avg / Frame  : {sum(self.bench_points_count)/len(self.bench_points_count):.0f} points\n"
                            f"========================================================="
                        )
                        self.log_message(summary)

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

        self.root.after(100, self._ui_consumer_tick)

if __name__ == "__main__":
    window = tk.Tk()
    app = GrpcBenchmarkApp(window)
    window.mainloop()