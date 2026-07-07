import asyncio
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

# config & tuning parameters
TARGET_ZONE = "Security Zone 1"
COOLDOWN_SECONDS = 5
ALARM_HOLD_SECONDS = 1.5
UI_REFRESH_RATE_SEC = 0.2  # capped to 5 FPS UI rendering
MAX_UI_POINTS = 600  # subsample point cloud for UI to prevent Tkinter lag
LIDAR_IP = "192.168.26.26"
API_KEY = "2ee812bc2e745dddb8i1cmJwrEaz8ehy"


class LidarDashboardApp:

  def __init__(self, root):
    self.root = root
    self.root.title("Blickfeld QB2 Dual-Stream Benchmark & Telemetry Monitor")
    self.root.geometry("1150x700")
    self.root.configure(bg="#1e1e2e")

    # decoupled Thread Memory Queues
    self.packet_queue = queue.Queue()

    # state Tracking
    self.last_log_time = 0
    self.alarm_active_until = 0
    self.last_ui_paint = 0
    self.cached_subjects = []
    self.cached_xyz = ([], [], [])

    #sSoftware clock calibration state
    self.calib_deltas = []
    self.clock_offset = None

    self.root.rowconfigure(1, weight=1)
    self.root.rowconfigure(2, weight=1)
    self.root.columnconfigure(0, weight=1)
    self.root.columnconfigure(1, weight=1)

    self._build_ui()

    # launch THREAD 1: edge AI objects producer
    self.objects_thread = threading.Thread(
        target=self._grpc_objects_producer, daemon=True
    )
    self.objects_thread.start()

    # launch THREAD 2: raw point cloud producer
    self.pointcloud_thread = threading.Thread(
        target=self._grpc_pointcloud_producer, daemon=True
    )
    self.pointcloud_thread.start()

    # launch UI consumer tick (native 10 FPS loop)
    self.root.after(100, self._ui_consumer_tick)

  def _build_ui(self):
    style = ttk.Style()
    style.theme_use("clam")
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

    # PANEL 1: STATUS BANNER
    self.status_frame = tk.Frame(self.root, bg="#a6e3a1", height=70)
    self.status_frame.grid(
        row=0, column=0, columnspan=2, sticky="ew", padx=15, pady=10
    )
    self.status_frame.grid_propagate(False)

    self.status_label = tk.Label(
        self.status_frame,
        text="SYSTEM SECURED - NO INTRUSIONS",
        font=("Arial", 16, "bold"),
        bg="#a6e3a1",
        fg="#11111b",
    )
    self.status_label.pack(expand=True)

    # PANEL 2 (LEFT): LIVE 3D POINT CLOUD RADAR
    pc_frame = tk.LabelFrame(
        self.root,
        text=" Live Raw Point Cloud (Subsampled for UI) ",
        bg="#1e1e2e",
        fg="#cdd6f4",
        font=("Arial", 11, "bold"),
    )
    pc_frame.grid(row=1, column=0, sticky="nsew", padx=(15, 5), pady=5)

    self.fig = Figure(figsize=(5, 4), dpi=90, facecolor="#1e1e2e")
    self.ax = self.fig.add_subplot(111, projection="3d", facecolor="#1e1e2e")
    self.ax.tick_params(colors="white", labelsize=7)
    self.ax.xaxis.label.set_color("white")
    self.ax.yaxis.label.set_color("white")
    self.ax.zaxis.label.set_color("white")

    self.canvas = FigureCanvasTkAgg(self.fig, master=pc_frame)
    self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    # PANEL 3 (RIGHT): ACTIVE SUBJECTS TABLE
    subjects_frame = tk.LabelFrame(
        self.root,
        text=f" Active Subjects inside '{TARGET_ZONE}' ",
        bg="#1e1e2e",
        fg="#cdd6f4",
        font=("Arial", 11, "bold"),
    )
    subjects_frame.grid(row=1, column=1, sticky="nsew", padx=(5, 15), pady=5)
    subjects_frame.rowconfigure(0, weight=1)
    subjects_frame.columnconfigure(0, weight=1)

    self.tree = ttk.Treeview(
        subjects_frame, columns=("id", "type", "speed"), show="headings"
    )
    self.tree.heading("id", text="Cluster ID")
    self.tree.heading("type", text="Classification")
    self.tree.heading("speed", text="Travel Speed")
    self.tree.column("id", anchor="center", width=110)
    self.tree.column("type", anchor="center", width=180)
    self.tree.column("speed", anchor="center", width=130)
    self.tree.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

    # PANEL 4: PERFORMANCE LOG
    perf_frame = tk.LabelFrame(
        self.root,
        text=" Network Performance & Dual-Stream Telemetry ",
        bg="#1e1e2e",
        fg="#cdd6f4",
        font=("Arial", 11, "bold"),
    )
    perf_frame.grid(
        row=2, column=0, columnspan=2, sticky="nsew", padx=15, pady=10
    )
    perf_frame.rowconfigure(0, weight=1)
    perf_frame.columnconfigure(0, weight=1)

    self.log_text = tk.Text(
        perf_frame,
        bg="#181825",
        fg="#bac2de",
        font=("Consolas", 10),
        state="disabled",
        wrap="word",
    )
    self.log_text.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

    scrollbar = ttk.Scrollbar(
        perf_frame, orient="vertical", command=self.log_text.yview
    )
    self.log_text.configure(yscrollcommand=scrollbar.set)
    scrollbar.grid(row=0, column=1, sticky="ns")

  def log_performance_message(self, message):
    self.log_text.configure(state="normal")
    self.log_text.insert(
        tk.END, f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {message}\n"
    )
    self.log_text.see(tk.END)
    self.log_text.configure(state="disabled")

  def _update_smart_ui(self, is_alarm, display_list, xyz_data):
    """DIFFING & PLOTTING ENGINE: Safely renders 3D plots and tables."""
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
        self.tree.item(child)["values"][0]
        for child in self.tree.get_children()
        if self.tree.item(child)["values"]
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

    xs, ys, zs = xyz_data
    if len(xs) > 0:
      self.ax.clear()
      self.ax.set_facecolor("#1e1e2e")
      self.ax.tick_params(colors="white", labelsize=7)

      # keep high density (~4,000 points) for structural fidelity
      step = max(1, len(xs) // 4000)
      sub_x, sub_y, sub_z = xs[::step], ys[::step], zs[::step]

      if is_alarm:
        colors = "#f38ba8"
      else:
        colors = sub_y  # Keep Depth coloring (foreground pops out)

      # CRITICAL TWEAK: s=2.5 (crisp fine dots), alpha=0.75 (soft blending), edgecolors="none" (removes dot borders)
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

      x_range = max(sub_x) - min(sub_x)
      y_range = max(sub_y) - min(sub_y)
      z_range = max(sub_z) - min(sub_z)
      max_range = max(x_range, y_range, z_range)

      if max_range > 0:
        self.ax.set_box_aspect(
            (x_range / max_range, y_range / max_range, z_range / max_range)
        )

      self.ax.view_init(elev=22, azim=-45)

      self.ax.set_title(
          f"Live Stream ({len(xs)} Total Wire Points)",
          color="white",
          fontsize=9,
      )
      self.ax.set_xlabel("X Width (m)", color="white", fontsize=7)
      self.ax.set_ylabel("Y Depth (m)", color="white", fontsize=7)
      self.ax.set_zlabel("Z Height (m)", color="white", fontsize=7)
      self.canvas.draw_idle()

  def _parse_sensor_epoch(self, frame_data):
    objs = frame_data.get("objects", {})
    raw_ts = (
        objs.get("timestamp")
        if isinstance(objs, dict)
        else frame_data.get("timestamp")
    )

    if not raw_ts:
      header = (
          frame_data.get("header") or frame_data.get("frame_header") or {}
      )
      raw_ts = header.get("timestamp")

    if not raw_ts:
      return 0.0

    try:
      val = float(raw_ts)
      if val > 1e16:
        return val / 1e9
      elif val > 1e13:
        return val / 1e6
      elif val > 1e10:
        return val / 1e3
      elif val > 1e8:
        return val
    except (ValueError, TypeError):
      pass
    return 0.0

  def _extract_xyz(self, frame):
    """safely extracts Cartesian coordinates from Blickfeld core_processing frames."""
    try:
      # native SDK approach: decodes Protobuf directly into a 2D NumPy array
      if hasattr(frame, "binary") and hasattr(frame.binary, "cartesian"):
        xyz = frame.binary.cartesian
        if xyz is not None and len(xyz) > 0:
          return xyz[:, 0], xyz[:, 1], xyz[:, 2]

      # fallback dictionary approach
      if isinstance(frame, dict):
        binary = frame.get("binary", {})
        xyz = binary.get("cartesian", [])
        if len(xyz) > 0:
          arr = np.array(xyz)
          if arr.ndim == 2 and arr.shape[1] >= 3:
            return arr[:, 0], arr[:, 1], arr[:, 2]
    except Exception:
      pass
    return [], [], []

  def _grpc_objects_producer(self):
    """THREAD 1: Streams pre-processed Edge AI Detections."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
      token_factory = blickfeld_qb2.TokenFactory(application_key_secret=API_KEY)
      with blickfeld_qb2.Channel(
          fqdn_or_ip=LIDAR_IP, token=token_factory
      ) as channel:
        service = blickfeld_qb2.percept_processing.services.Objects(channel)
        self.packet_queue.put(
            ("LOG", f"STREAM 1 ONLINE: Dialed Edge AI Engine at {LIDAR_IP}")
        )

        for response in service.stream():
          wire_time = time.time()
          frame_data = response.to_dict()
          self.packet_queue.put(("OBJECTS", wire_time, frame_data))
    except Exception as e:
      self.packet_queue.put(("LOG", f"EDGE AI STREAM ERROR: {str(e)}"))

  def _grpc_pointcloud_producer(self):
    """THREAD 2: Streams Raw Unprocessed 3D Laser Point Clouds."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
      token_factory = blickfeld_qb2.TokenFactory(application_key_secret=API_KEY)
      with blickfeld_qb2.Channel(
          fqdn_or_ip=LIDAR_IP, token=token_factory
      ) as channel:
        # Correct official Blickfeld QB2 point cloud namespace
        service = blickfeld_qb2.core_processing.services.PointCloud(channel)
        self.packet_queue.put(
            ("LOG", f"STREAM 2 ONLINE: Dialed Raw Point Cloud at {LIDAR_IP}")
        )

        for response in service.stream():
          wire_time = time.time()
          frame = response.frame
          self.packet_queue.put(("POINTCLOUD", wire_time, frame))
    except Exception as e:
      self.packet_queue.put(("LOG", f"POINT CLOUD STREAM ERROR: {str(e)}"))

  def _ui_consumer_tick(self):
    """CONSUMER: Merges Dual Streams & Handles Clock Auto-Calibration."""
    latest_objects = None
    latest_pc = None

    while not self.packet_queue.empty():
      item = self.packet_queue.get_nowait()
      if item[0] == "LOG":
        self.log_performance_message(item[1])
      elif item[0] == "OBJECTS":
        latest_objects = item
      elif item[0] == "POINTCLOUD":
        latest_pc = item

    if latest_pc:
      _, pc_wire_time, pc_frame = latest_pc
      self.cached_xyz = self._extract_xyz(pc_frame)

    if latest_objects:
      _, wire_arrival_time, frame_data = latest_objects
      now = time.time()
      sensor_epoch = self._parse_sensor_epoch(frame_data)

      if self.clock_offset is None:
        if sensor_epoch > 0:
          self.calib_deltas.append(wire_arrival_time - sensor_epoch)
        if len(self.calib_deltas) >= 30:
          fastest_raw_gap = min(self.calib_deltas)
          self.clock_offset = fastest_raw_gap - 0.003
          self.log_performance_message(
              "CALIBRATION COMPLETE: Dual-Stream Benchmark ready."
          )
        self.root.after(100, self._ui_consumer_tick)
        return

      if sensor_epoch > 0:
        normalized_sensor = sensor_epoch + self.clock_offset
        pure_pipeline_latency_ms = max(
            0.01, (wire_arrival_time - normalized_sensor) * 1000
        )
        lat_str = f"{pure_pipeline_latency_ms:.2f}ms"
      else:
        lat_str = "UNKNOWN"

      raw_objs = frame_data.get("objects", {})
      obj_map = (
          raw_objs.get("objects", {})
          if isinstance(raw_objs, dict) and "objects" in raw_objs
          else raw_objs
      )

      incoming_intrusions = []
      for obj_id, obj in obj_map.items():
        if not isinstance(obj, dict):
          continue
        intruding = obj.get("intruding", {})

        if intruding.get("value") is True or intruding.get("state") is True:
          intruder = obj.get("intruder", {})
          rep_zone = (
              intruder.get("zone_name")
              or intruder.get("zone_id")
              or intruder.get("name")
              or ""
          )

          if rep_zone in (TARGET_ZONE, ""):
            props = obj.get("properties", {}) or obj.get("classification", {})
            raw_size = props.get("size", "")

            friendly_type = "UNCLASSIFIED_MOTION"
            if "MEDIUM" in raw_size:
              friendly_type = "PERSON"
            elif "LARGE" in raw_size:
              friendly_type = "VEHICLE"
            elif "SMALL" in raw_size:
              friendly_type = "ANIMAL_OR_DEBRIS"

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

      is_alarm = now < self.alarm_active_until
      display_list = self.cached_subjects if is_alarm else []

      if (now - self.last_ui_paint) >= UI_REFRESH_RATE_SEC:
        self.last_ui_paint = now
        self._update_smart_ui(is_alarm, display_list, self.cached_xyz)

      if len(incoming_intrusions) > 0 and (
          now - self.last_log_time
      ) >= COOLDOWN_SECONDS:
        self.last_log_time = now
        log_msg = (
            f"INTRUSION | Edge AI Latency: {lat_str} | Active Subjects:"
            f" {len(incoming_intrusions)}"
        )
        self.log_performance_message(log_msg)

    self.root.after(100, self._ui_consumer_tick)


if __name__ == "__main__":
  window = tk.Tk()
  app = LidarDashboardApp(window)
  window.mainloop()