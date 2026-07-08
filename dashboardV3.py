import asyncio
import json
import math
import queue
import threading
import time
from datetime import datetime, timezone
import tkinter as tk
from tkinter import ttk

import blickfeld_qb2
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import numpy as np
import paho.mqtt.client as mqtt

# config & tuning parameters
TARGET_ZONE = "Security Zone 1"
COOLDOWN_SECONDS = 5
ALARM_HOLD_SECONDS = 1.5
UI_REFRESH_RATE_SEC = 0.2  # 5 FPS UI rendering cap

# gRPC Config
LIDAR_IP = "192.168.26.26"
API_KEY = "2ee812bc2e745dddb8i1cmJwrEaz8ehy"

# MQTT Config
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC = "#"


class LidarDashboardApp:

  def __init__(self, root):
    self.root = root
    self.root.title(
        "Blickfeld QB2 Hybrid Benchmark: Native MQTT Alerts vs gRPC Raw Stream"
    )
    self.root.geometry("1150x700")
    self.root.configure(bg="#1e1e2e")

    self.packet_queue = queue.Queue()

    # shared State Tracking
    self.last_log_time = 0
    self.alarm_active_until = 0
    self.last_ui_paint = 0
    self.cached_subjects = []
    self.cached_xyz = ([], [], [])

    # clock Auto-Zero Calibration State (Derived from gRPC, applied to MQTT)
    self.calib_deltas = []
    self.clock_offset = None

    # timed Benchmark State (Raw Data Tab)
    self.bench_active = False
    self.bench_end_time = 0
    self.bench_latencies = []
    self.bench_points_count = []

    self.root.rowconfigure(0, weight=1)
    self.root.columnconfigure(0, weight=1)

    self._build_ui()

    # launch Producer Threads
    self.mqtt_thread = threading.Thread(
        target=self._mqtt_subscriber_thread, daemon=True
    )
    self.mqtt_thread.start()

    self.pointcloud_thread = threading.Thread(
        target=self._grpc_pointcloud_producer, daemon=True
    )
    self.pointcloud_thread.start()

    # launch Consumer UI Loop
    self.root.after(100, self._ui_consumer_tick)

  def _build_ui(self):
    style = ttk.Style()
    style.theme_use("clam")
    style.configure("TNotebook", background="#1e1e2e", borderwidth=0)
    style.configure(
        "TNotebook.Tab",
        background="#32324d",
        foreground="white",
        padding=[15, 6],
        font=("Arial", 11, "bold"),
    )
    style.map(
        "TNotebook.Tab",
        background=[("selected", "#89b4fa")],
        foreground=[("selected", "#11111b")],
    )
    style.configure(
        "Treeview",
        background="#252538",
        foreground="white",
        fieldbackground="#252538",
        rowheight=26,
    )
    style.configure(
        "Treeview.Heading",
        background="#32324d",
        foreground="white",
        relief="flat",
    )
    style.map("Treeview", background=[("selected", "#4c4f69")])

    # --- MASTER TABBED NOTEBOOK ---
    self.notebook = ttk.Notebook(self.root)
    self.notebook.grid(row=0, column=0, sticky="nsew")

    self.edge_tab = tk.Frame(self.notebook, bg="#1e1e2e")
    self.raw_tab = tk.Frame(self.notebook, bg="#1e1e2e")

    self.notebook.add(self.edge_tab, text="  ⚡ PAGE 1: NATIVE MQTT ALERTS  ")
    self.notebook.add(self.raw_tab, text="  📡 PAGE 2: gRPC RAW BENCHMARK  ")

    self._build_edge_tab()
    self._build_raw_tab()

  def _build_edge_tab(self):
    """PAGE 1: Edge-AI Intrusion Monitor & Compute Latency."""
    self.edge_tab.rowconfigure(1, weight=1)
    self.edge_tab.rowconfigure(2, weight=1)
    self.edge_tab.columnconfigure(0, weight=1)

    # 1. status banner
    self.status_frame = tk.Frame(self.edge_tab, bg="#a6e3a1", height=80)
    self.status_frame.grid(row=0, column=0, sticky="ew", padx=15, pady=10)
    self.status_frame.grid_propagate(False)

    self.status_label = tk.Label(
        self.status_frame,
        text="SYSTEM SECURED - NO INTRUSIONS",
        font=("Arial", 16, "bold"),
        bg="#a6e3a1",
        fg="#11111b",
    )
    self.status_label.pack(expand=True)

    # 2. active subjects table
    subjects_frame = tk.LabelFrame(
        self.edge_tab,
        text=f" Native MQTT Tracked Subjects inside '{TARGET_ZONE}' ",
        bg="#1e1e2e",
        fg="#cdd6f4",
        font=("Arial", 11, "bold"),
    )
    subjects_frame.grid(row=1, column=0, sticky="nsew", padx=15, pady=5)
    subjects_frame.rowconfigure(0, weight=1)
    subjects_frame.columnconfigure(0, weight=1)

    self.tree = ttk.Treeview(
        subjects_frame, columns=("id", "type", "speed"), show="headings"
    )
    self.tree.heading("id", text="Cluster ID")
    self.tree.heading("type", text="Classification")
    self.tree.heading("speed", text="Travel Speed")
    self.tree.column("id", anchor="center", width=150)
    self.tree.column("type", anchor="center", width=250)
    self.tree.column("speed", anchor="center", width=200)
    self.tree.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

    # 3. edge telemetry log
    perf_frame = tk.LabelFrame(
        self.edge_tab,
        text=" MQTT Edge-AI Time-to-Insight Analytics ",
        bg="#1e1e2e",
        fg="#cdd6f4",
        font=("Arial", 11, "bold"),
    )
    perf_frame.grid(row=2, column=0, sticky="nsew", padx=15, pady=10)
    perf_frame.rowconfigure(0, weight=1)
    perf_frame.columnconfigure(0, weight=1)

    self.edge_log = tk.Text(
        perf_frame,
        bg="#181825",
        fg="#bac2de",
        font=("Consolas", 10),
        state="disabled",
        wrap="word",
    )
    self.edge_log.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
    scroll = ttk.Scrollbar(
        perf_frame, orient="vertical", command=self.edge_log.yview
    )
    self.edge_log.configure(yscrollcommand=scroll.set)
    scroll.grid(row=0, column=1, sticky="ns")

  def _build_raw_tab(self):
    """PAGE 2: Raw Point Cloud Visualizer & Timed Latency Benchmarker."""
    self.raw_tab.rowconfigure(0, weight=1)
    self.raw_tab.columnconfigure(0, weight=3)
    self.raw_tab.columnconfigure(1, weight=2)

    # 1. left: live 3D point cloud canvas
    pc_frame = tk.LabelFrame(
        self.raw_tab,
        text=" Unprocessed gRPC 3D Laser Stream (Subsampled for Display) ",
        bg="#1e1e2e",
        fg="#cdd6f4",
        font=("Arial", 11, "bold"),
    )
    pc_frame.grid(row=0, column=0, sticky="nsew", padx=(15, 5), pady=10)

    self.fig = Figure(figsize=(6, 5), dpi=90, facecolor="#1e1e2e")
    self.ax = self.fig.add_subplot(111, projection="3d", facecolor="#1e1e2e")
    self.ax.tick_params(colors="white", labelsize=7)
    self.canvas = FigureCanvasTkAgg(self.fig, master=pc_frame)
    self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    # 2. right: timed benchmarking tool
    bench_frame = tk.LabelFrame(
        self.raw_tab,
        text=" Timed gRPC Raw Stream Network Benchmarker ",
        bg="#1e1e2e",
        fg="#cdd6f4",
        font=("Arial", 11, "bold"),
    )
    bench_frame.grid(row=0, column=1, sticky="nsew", padx=(5, 15), pady=10)
    bench_frame.rowconfigure(2, weight=1)
    bench_frame.columnconfigure(0, weight=1)

    ctrl_bar = tk.Frame(bench_frame, bg="#252538", pady=10, padx=10)
    ctrl_bar.grid(row=0, column=0, sticky="ew", padx=5, pady=5)

    tk.Label(
        ctrl_bar,
        text="Duration (sec):",
        font=("Arial", 10, "bold"),
        bg="#252538",
        fg="white",
    ).pack(side=tk.LEFT, padx=5)
    self.dur_entry = tk.Entry(
        ctrl_bar,
        width=6,
        font=("Consolas", 11, "bold"),
        bg="#181825",
        fg="#89b4fa",
        insertbackground="white",
    )
    self.dur_entry.insert(0, "10")
    self.dur_entry.pack(side=tk.LEFT, padx=5)

    self.start_btn = tk.Button(
        ctrl_bar,
        text="▶ START BENCHMARK",
        font=("Arial", 10, "bold"),
        bg="#89b4fa",
        fg="#11111b",
        command=self._start_timed_benchmark,
        relief="flat",
        cursor="hand2",
    )
    self.start_btn.pack(side=tk.RIGHT, padx=5)

    self.bench_status = tk.Label(
        bench_frame,
        text="Status: Ready to run network load test",
        font=("Arial", 10, "italic"),
        bg="#1e1e2e",
        fg="#a6adc8",
    )
    self.bench_status.grid(row=1, column=0, sticky="w", padx=10, pady=2)

    self.raw_log = tk.Text(
        bench_frame,
        bg="#181825",
        fg="#bac2de",
        font=("Consolas", 10),
        state="disabled",
        wrap="word",
    )
    self.raw_log.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)
    scroll = ttk.Scrollbar(
        bench_frame, orient="vertical", command=self.raw_log.yview
    )
    self.raw_log.configure(yscrollcommand=scroll.set)
    scroll.grid(row=2, column=1, sticky="ns")

  def log_message(self, text_widget, msg):
    text_widget.configure(state="normal")
    text_widget.insert(
        tk.END, f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}\n"
    )
    text_widget.see(tk.END)
    text_widget.configure(state="disabled")

  def _start_timed_benchmark(self):
    if self.bench_active:
      return
    try:
      dur = float(self.dur_entry.get())
      if dur <= 0:
        raise ValueError
    except ValueError:
      self.log_message(
          self.raw_log, "ERROR: Please enter a valid duration in seconds."
      )
      return

    self.bench_active = True
    self.bench_end_time = time.time() + dur
    self.bench_latencies = []
    self.bench_points_count = []

    self.start_btn.configure(
        state="disabled", text="⏳ RUNNING...", bg="#f38ba8"
    )
    self.bench_status.configure(
        text=f"Status: Recording packets for next {dur}s...", fg="#f9e2af"
    )
    self.log_message(
        self.raw_log,
        f"\n=== STARTING {dur}s RAW STREAM NETWORK BENCHMARK ===",
    )

  def _update_smart_ui(self, is_alarm, display_list, xyz_data):
    current_tab = self.notebook.index(self.notebook.select())

    if current_tab == 0:
      master_threat = "MOTION DETECTED"
      if any(x["classification"] == "PERSON" for x in display_list):
        master_threat = "PERSON INTRUSION"
      elif any(x["classification"] == "VEHICLE" for x in display_list):
        master_threat = "VEHICLE INTRUSION"

      target_bg = "#f38ba8" if is_alarm else "#a6e3a1"
      target_text = (
          f"ALARM: {master_threat} DETECTED!"
          if is_alarm
          else "SYSTEM SECURED - NO INTRUSIONS"
      )

      if self.status_label.cget("text") != target_text:
        self.status_frame.configure(bg=target_bg)
        self.status_label.configure(bg=target_bg, text=target_text)

      new_ids = [x["cluster_id"] for x in display_list]
      rendered_ids = [
          self.tree.item(c)["values"][0]
          for c in self.tree.get_children()
          if self.tree.item(c)["values"]
      ]
      if new_ids != rendered_ids:
        for item in self.tree.get_children():
          self.tree.delete(item)
        for target in display_list:
          self.tree.insert(
              "",
              tk.END,
              values=(
                  target["cluster_id"],
                  target["classification"],
                  f"{target['speed_mph']} mph",
              ),
          )

    elif current_tab == 1:
      xs, ys, zs = xyz_data
      if len(xs) > 0:
        self.ax.clear()
        self.ax.set_facecolor("#1e1e2e")
        self.ax.tick_params(colors="white", labelsize=7)

        step = max(1, len(xs) // 4000)
        sub_x, sub_y, sub_z = xs[::step], ys[::step], zs[::step]
        colors = "#f38ba8" if is_alarm else sub_y

        self.ax.scatter(
            sub_x,
            sub_y,
            sub_z,
            c=colors,
            cmap="plasma_r",
            s=2.5,
            alpha=0.75,
            edgecolors="none",
        )

        x_rng = max(sub_x) - min(sub_x)
        y_rng = max(sub_y) - min(sub_y)
        z_rng = max(sub_z) - min(sub_z)
        max_rng = max(x_rng, y_rng, z_rng)

        if max_rng > 0:
          self.ax.set_box_aspect(
              (x_rng / max_rng, y_rng / max_rng, z_rng / max_rng)
          )
        self.ax.view_init(elev=22, azim=-45)
        self.ax.set_title(
            f"Live Stream ({len(xs)} Total Wire Points)",
            color="white",
            fontsize=9,
        )
        self.canvas.draw_idle()

  # Thread 1 - MQTT alerts
  def _mqtt_subscriber_thread(self):
    """Listens to the local Mosquitto broker for native LiDAR JSON alerts."""
    def on_connect(client, userdata, flags, rc, properties=None):
      if rc == 0:
        self.packet_queue.put(
            ("EDGE_LOG", f"ONLINE: Connected to local MQTT Broker on {MQTT_PORT}")
        )
        client.subscribe(MQTT_TOPIC)
      else:
        self.packet_queue.put(
            ("EDGE_LOG", f"ERROR: MQTT Connection failed with code {rc}")
        )

    def on_message(client, userdata, msg):
      try:
        # Print the literal raw string as it arrives off the wire
        raw_str = msg.payload.decode("utf-8")
        print(f"\n[RAW MQTT] Topic: {msg.topic} | Payload: {raw_str[:200]}...")
        
        payload = json.loads(raw_str)
        self.packet_queue.put(("MQTT_OBJECTS", time.time(), payload))
      except Exception as e:
        print(f"[!] MQTT PARSE ERROR on topic {msg.topic}: {e}")

    try:
      # paho MQTT v2 & v1 compatibility wrap
      try:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="Dashboard_UI",
        )
      except AttributeError:
        client = mqtt.Client(client_id="Dashboard_UI")

      client.on_connect = on_connect
      client.on_message = on_message
      client.connect(MQTT_BROKER, MQTT_PORT, 60)
      client.loop_forever()
    except Exception as e:
      self.packet_queue.put(("EDGE_LOG", f"MQTT ERROR: {str(e)}"))

  # Thread 2: gRPC raw point cloud
  def _extract_xyz(self, frame):
    try:
      if hasattr(frame, "binary") and hasattr(frame.binary, "cartesian"):
        xyz = frame.binary.cartesian
        if xyz is not None and len(xyz) > 0:
          return xyz[:, 0], xyz[:, 1], xyz[:, 2]
      if isinstance(frame, dict):
        xyz = frame.get("binary", {}).get("cartesian", [])
        if len(xyz) > 0:
          arr = np.array(xyz)
          if arr.ndim == 2 and arr.shape[1] >= 3:
            return arr[:, 0], arr[:, 1], arr[:, 2]
    except Exception:
      pass
    return [], [], []

  def _grpc_pointcloud_producer(self):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
      token_factory = blickfeld_qb2.TokenFactory(application_key_secret=API_KEY)
      with blickfeld_qb2.Channel(
          fqdn_or_ip=LIDAR_IP, token=token_factory
      ) as channel:
        service = blickfeld_qb2.core_processing.services.PointCloud(channel)
        self.packet_queue.put(
            ("RAW_LOG", f"ONLINE: Connected to gRPC Raw Point Cloud stream")
        )

        for response in service.stream():
          self.packet_queue.put(("POINTCLOUD", time.time(), response.frame))
    except Exception as e:
      self.packet_queue.put(("RAW_LOG", f"ERROR: {str(e)}"))

  # Main data consumer engine
  # Main data consumer engine
  def _ui_consumer_tick(self):
    latest_mqtt = None
    latest_pc = None

    while not self.packet_queue.empty():
      item = self.packet_queue.get_nowait()
      if item[0] == "EDGE_LOG":
        self.log_message(self.edge_log, item[1])
      elif item[0] == "RAW_LOG":
        self.log_message(self.raw_log, item[1])
      elif item[0] == "MQTT_OBJECTS":
        latest_mqtt = item
      elif item[0] == "POINTCLOUD":
        latest_pc = item

    # --- PROCESS gRPC RAW STREAM & CLOCK CALIBRATION ---
    if latest_pc:
      _, pc_wire_time, pc_frame = latest_pc
      xs, ys, zs = self._extract_xyz(pc_frame)
      self.cached_xyz = (xs, ys, zs)

      raw_ts = getattr(pc_frame, "timestamp", None) or 0.0
      pc_sensor_epoch = (
          float(raw_ts) / 1e9 if float(raw_ts) > 1e16 else float(raw_ts)
      )

      # TRUE WIRE-SPEED CALIBRATION (No longer blocking!)
      if self.clock_offset is None:
        if pc_sensor_epoch > 0:
          self.calib_deltas.append(pc_wire_time - pc_sensor_epoch)
        if len(self.calib_deltas) >= 30:
          fastest_raw_gap = min(self.calib_deltas)
          self.clock_offset = fastest_raw_gap - 0.003  # 3ms switch baseline
          self.log_message(
              self.edge_log, "CALIBRATION COMPLETE: Hardware clock locked."
          )
          self.log_message(
              self.raw_log,
              "CALIBRATION COMPLETE: Ready to run network benchmarks.",
          )

      # TIMED BENCHMARK LOGIC
      if self.bench_active and self.clock_offset is not None:
        if time.time() <= self.bench_end_time:
          if pc_sensor_epoch > 0:
            norm_ts = pc_sensor_epoch + self.clock_offset
            raw_lat_ms = max(0.01, (pc_wire_time - norm_ts) * 1000)
            self.bench_latencies.append(raw_lat_ms)
            self.bench_points_count.append(len(xs))
        else:
          self.bench_active = False
          self.start_btn.configure(
              state="normal", text="▶ START BENCHMARK", bg="#89b4fa"
          )
          self.bench_status.configure(
              text="Status: Benchmark complete!", fg="#a6e3a1"
          )

          if len(self.bench_latencies) > 0:
            avg_lat = sum(self.bench_latencies) / len(self.bench_latencies)
            min_lat = min(self.bench_latencies)
            max_lat = max(self.bench_latencies)
            total_pts = sum(self.bench_points_count)
            avg_pts = total_pts / len(self.bench_points_count)

            summary = (
                f"\n=== BENCHMARK RESULTS ({len(self.bench_latencies)} Frames"
                " Analyzed) ===\n"
                f" ├── Average Raw Network Latency : {avg_lat:.2f} ms\n"
                f" ├── Minimum Transit Latency     : {min_lat:.2f} ms\n"
                f" ├── Maximum Transit Latency     : {max_lat:.2f} ms\n"
                f" ├── Latency Jitter (Max - Min)  : {max_lat - min_lat:.2f} ms\n"
                f" └── Average Frame Size          : {avg_pts:.0f} points/frame"
                f" (~{avg_pts * 12 / 1024:.1f} KB)\n"
                f"==========================================================="
            )
            self.log_message(self.raw_log, summary)

    # --- PROCESS MQTT JSON OBJECTS ---
    now = time.time()
    lat_str = "CALIBRATING CLOCK..."

    if latest_mqtt:
      _, wire_arrival_time, payload = latest_mqtt
      
      # 1. Safely extract Timestamp
      raw_ts = payload.get("timestamp") or payload.get("time") or 0.0
      sensor_epoch = 0.0
      
      if isinstance(raw_ts, str):
          try:
              # Convert ISO 8601 string back to a Unix float
              sensor_epoch = datetime.fromisoformat(raw_ts.replace("Z", "+00:00")).timestamp()
          except Exception:
              pass
      else:
          try:
              val = float(raw_ts)
              sensor_epoch = val / 1e9 if val > 1e16 else (val / 1e3 if val > 1e10 else val)
          except (ValueError, TypeError):
              pass

      # ---------------------------------------------------------
      # THE FIX: Smart Clock Routing
      # ---------------------------------------------------------
      if sensor_epoch > 0:
        # Check if timestamp is already in laptop system time (within 24 hours of right now)
        if abs(now - sensor_epoch) < 86400:
            # It came from your Bridge Script! (Local machine transit)
            edge_lat_ms = abs(wire_arrival_time - sensor_epoch) * 1000
            lat_str = f"{edge_lat_ms:.3f}ms (Local)"
        elif self.clock_offset is not None:
            # It came straight from the LiDAR hardware! (Ethernet transit)
            norm_sensor = sensor_epoch + self.clock_offset
            edge_lat_ms = max(0.0, (wire_arrival_time - norm_sensor) * 1000)
            lat_str = f"{edge_lat_ms:.2f}ms (Network)"
        else:
            lat_str = "CALIBRATING CLOCK..."
      else:
        lat_str = "UNKNOWN (No timestamp)"

      incoming_intrusions = []

      # ---------------------------------------------------------
      # THE FIX: Read the pre-processed custom payload directly!
      # ---------------------------------------------------------
      # (Added 'subejects' to catch any potential typos in the incoming stream)
      custom_subjects = payload.get("subjects") or payload.get("subejects") or []
      
      if custom_subjects and isinstance(custom_subjects, list):
          # The data is ALREADY processed by our bridge script! No math needed.
          for subj in custom_subjects:
              incoming_intrusions.append({
                  "cluster_id": str(subj.get("cluster_id", "Unknown")),
                  "classification": subj.get("classification", "UNCLASSIFIED_MOTION"),
                  "speed_mph": float(subj.get("speed_mph", 0.0)),
              })
      else:
          # Fallback: Native Blickfeld Nested Parsing (just in case you switch back)
          raw_objs = payload.get("objects", payload.get("intruders", payload))
          if isinstance(raw_objs, dict) and "objects" in raw_objs:
              raw_objs = raw_objs["objects"]
              
          obj_list = list(raw_objs.values()) if isinstance(raw_objs, dict) else (raw_objs if isinstance(raw_objs, list) else [payload])

          for obj in obj_list:
            if not isinstance(obj, dict):
              continue

            intruding_data = obj.get("intruding", True)
            is_intruder = intruding_data.get("value", True) if isinstance(intruding_data, dict) else (intruding_data if isinstance(intruding_data, bool) else True)

            if is_intruder:
              c_id = obj.get("id", obj.get("cluster_id", "Unknown"))
              props = obj.get("properties", {}) or obj.get("classification", {})
              raw_size = str(props.get("size", obj.get("size", ""))).upper()
              
              friendly_type = "UNCLASSIFIED_MOTION"
              if "MEDIUM" in raw_size or "PERSON" in raw_size: friendly_type = "PERSON"
              elif "LARGE" in raw_size or "VEHICLE" in raw_size: friendly_type = "VEHICLE"
              elif "SMALL" in raw_size: friendly_type = "ANIMAL_OR_DEBRIS"

              vel = obj.get("velocity", {})
              if isinstance(vel, list) and len(vel) >= 3:
                vx, vy, vz = vel[0], vel[1], vel[2]
              else:
                vx, vy, vz = vel.get("x", 0), vel.get("y", 0), vel.get("z", 0)
                
              speed_mph = math.sqrt(vx**2 + vy**2 + vz**2) * 2.23694

              incoming_intrusions.append({
                  "cluster_id": str(c_id),
                  "classification": friendly_type,
                  "speed_mph": round(speed_mph, 1),
              })

      # Trigger UI Updates
      if len(incoming_intrusions) > 0:
        self.alarm_active_until = now + ALARM_HOLD_SECONDS
        self.cached_subjects = incoming_intrusions

        if (now - self.last_log_time) >= COOLDOWN_SECONDS:
          self.last_log_time = now
          log_msg = f"INTRUSION | MQTT Edge Latency: {lat_str} | Active Subjects: {len(incoming_intrusions)}"
          self.log_message(self.edge_log, log_msg)

    # --- REFRESH UI CORE ENGINE ---
    is_alarm = now < self.alarm_active_until
    display_list = self.cached_subjects if is_alarm else []

    if (now - self.last_ui_paint) >= UI_REFRESH_RATE_SEC:
      self.last_ui_paint = now
      self._update_smart_ui(is_alarm, display_list, self.cached_xyz)

    self.root.after(100, self._ui_consumer_tick)


if __name__ == "__main__":
  window = tk.Tk()
  app = LidarDashboardApp(window)
  window.mainloop()