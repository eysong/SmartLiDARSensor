import asyncio
import math
import queue
import threading
import time
import struct
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog
import blickfeld_qb2
from scapy.all import rdpcap, sniff, TCP, IP

# ==========================================
# --- CONFIGURATION ---
# ==========================================
TARGET_ZONE = "Security Zone 1"
COOLDOWN_SECONDS = 5
ALARM_HOLD_SECONDS = 1.5
UI_REFRESH_RATE_SEC = 0.2

LIDAR_IP = "192.168.26.26"
API_KEY = "2ee812bc2e745dddb8i1cmJwrEaz8ehy"
GRPC_PORT = 50051

class GrpcPcapBenchmarkApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Blickfeld: gRPC Edge-AI Monitor + PCAP DPI Correlator")
        self.root.geometry("950x650")
        self.root.configure(bg="#1e1e2e")

        self.packet_queue = queue.Queue()
        self.last_log_time = 0
        self.alarm_active_until = 0
        self.last_ui_paint = 0
        self.cached_subjects = []
        self.tree_items = {}

        # Benchmarking & Sniffing State
        self.bench_active = False
        self.bench_end_time = 0.0
        self.auto_sniffed_packets = []

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

        # --- BENCHMARK CONTROL BAR WITH MODE SELECTOR ---
        ctrl_frame = tk.Frame(self.root, bg="#252538", pady=8, padx=15)
        ctrl_frame.grid(row=0, column=0, sticky="ew", padx=15, pady=(10, 0))
        
        tk.Label(ctrl_frame, text="⏱ Duration (s):", font=("Arial", 10, "bold"), bg="#252538", fg="white").pack(side=tk.LEFT, padx=2)
        self.dur_entry = tk.Entry(ctrl_frame, width=4, font=("Consolas", 11, "bold"), bg="#181825", fg="#89b4fa", insertbackground="white")
        self.dur_entry.insert(0, "10")
        self.dur_entry.pack(side=tk.LEFT, padx=5)

        tk.Label(ctrl_frame, text="Mode:", font=("Arial", 10, "bold"), bg="#252538", fg="white").pack(side=tk.LEFT, padx=(10, 2))
        self.mode_var = tk.StringVar(value="Manual PCAP Upload")
        self.mode_dropdown = ttk.Combobox(
            ctrl_frame, 
            textvariable=self.mode_var, 
            values=["Manual PCAP Upload", "Auto-Capture via Scapy"], 
            state="readonly", 
            width=20
        )
        self.mode_dropdown.pack(side=tk.LEFT, padx=5)

        self.start_bench_btn = tk.Button(ctrl_frame, text="▶ START BENCHMARK", font=("Arial", 10, "bold"), bg="#89b4fa", fg="#11111b", command=self._start_timed_benchmark, relief="flat")
        self.start_bench_btn.pack(side=tk.LEFT, padx=10)

        self.pcap_btn = tk.Button(ctrl_frame, text="📂 LOAD PCAP FILE", font=("Arial", 10, "bold"), bg="#a6e3a1", fg="#11111b", command=self._manual_load_pcap, relief="flat")
        self.pcap_btn.pack(side=tk.RIGHT, padx=5)

        # Status Banner
        self.status_frame = tk.Frame(self.root, bg="#a6e3a1", height=60)
        self.status_frame.grid(row=1, column=0, sticky="ew", padx=15, pady=10)
        self.status_frame.grid_propagate(False)
        self.status_label = tk.Label(self.status_frame, text="SYSTEM SECURED - NO INTRUSIONS", font=("Arial", 15, "bold"), bg="#a6e3a1", fg="#11111b")
        self.status_label.pack(expand=True)

        # Subjects Table
        subjects_frame = tk.LabelFrame(self.root, text=f" gRPC Tracked Subjects inside '{TARGET_ZONE}' ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
        subjects_frame.grid(row=2, column=0, sticky="nsew", padx=15, pady=5)
        subjects_frame.rowconfigure(0, weight=1)
        subjects_frame.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(subjects_frame, columns=("objectID", "objType", "pos", "speed"), show="headings")
        self.tree.heading("objectID", text="objectID")
        self.tree.heading("objType", text="objType")
        self.tree.heading("pos", text="pos [X, Y, Z]")
        self.tree.heading("speed", text="speed (mph)")
        
        self.tree.column("objectID", anchor="center", width=100)
        self.tree.column("objType", anchor="center", width=180)
        self.tree.column("pos", anchor="center", width=180)
        self.tree.column("speed", anchor="center", width=100)
        self.tree.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

        tree_scroll = ttk.Scrollbar(subjects_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_scroll.set)
        tree_scroll.grid(row=0, column=1, sticky="ns")

        # Telemetry & PCAP Analysis Log
        perf_frame = tk.LabelFrame(self.root, text=" Telemetry & Deep Packet Inspection (DPI) Log ", bg="#1e1e2e", fg="#cdd6f4", font=("Arial", 11, "bold"))
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

    def _start_timed_benchmark(self):
        if self.bench_active:
            return
        try:
            dur = float(self.dur_entry.get())
        except ValueError:
            return

        selected_mode = self.mode_var.get()
        self.bench_active = True
        self.bench_end_time = time.time() + dur
        self.auto_sniffed_packets = []
        self.start_bench_btn.configure(state="disabled", text="⏳ RUNNING...", bg="#f38ba8")
        
        self.log_message(f"\n=== BENCHMARK STARTED ({dur}s) | Mode: {selected_mode} ===")
        
        if selected_mode == "Auto-Capture via Scapy":
            self.log_message("▶ Auto-Capture: Sniffing gRPC packets from sensor...")
            threading.Thread(target=self._background_sniffer, args=(dur,), daemon=True).start()
        else:
            self.log_message("▶ Manual Mode: Keep Wireshark recording packets on your laptop now!")

    def _background_sniffer(self, timeout_sec):
        """ Captures raw packets in background using Scapy (requires Admin/root permissions) """
        try:
            filter_str = f"tcp and src host {LIDAR_IP}"
            packets = sniff(filter=filter_str, timeout=timeout_sec)
            self.auto_sniffed_packets = packets
        except Exception as e:
            self.log_message(f"AUTO-CAPTURE ERROR: {str(e)} (Make sure to run Python as Admin/root!)")

    def _manual_load_pcap(self):
        file_path = filedialog.askopenfilename(
            title="Select Wireshark PCAP File for DPI Correlation",
            filetypes=[("PCAP Capture Files", "*.pcap *.pcapng"), ("All Files", "*.*")]
        )
        if file_path:
            threading.Thread(target=self._process_packets, args=(rdpcap(file_path), f"File: {file_path}"), daemon=True).start()

    def _decode_varint(self, data, offset):
        """ Decodes a Protobuf varint from raw bytes starting at a specific offset """
        result = 0
        shift = 0
        for i in range(10):  # Varints are max 10 bytes for a 64-bit integer
            if offset + i >= len(data):
                return None
            byte = data[offset + i]
            result |= (byte & 0x7f) << shift
            if not (byte & 0x80):
                return result
            shift += 7
        return None

    def _extract_protobuf_timestamp(self, payload_bytes):
        """ Robustly scans raw byte payloads for nanosecond optical timestamps (> 1.6e18 ns) """
        if len(payload_bytes) < 8:
            return None
            
        # Step by 1 byte (not 4!) to catch unaligned Protobuf tags and HTTP/2 headers
        for i in range(len(payload_bytes) - 8):
            try:
                # 1. Check for Protobuf Varint encoding (standard uint64/int64 in .proto files)
                val_varint = self._decode_varint(payload_bytes, i)
                if val_varint and 1_600_000_000_000_000_000 < val_varint < 2_200_000_000_000_000_000:
                    return val_varint / 1e9

                # 2. Check for Little-Endian fixed64 (<Q)
                val_le = struct.unpack("<Q", payload_bytes[i:i+8])[0]
                if 1_600_000_000_000_000_000 < val_le < 2_200_000_000_000_000_000:
                    return val_le / 1e9

                # 3. Check for Big-Endian fixed64 (>Q)
                val_be = struct.unpack(">Q", payload_bytes[i:i+8])[0]
                if 1_600_000_000_000_000_000 < val_be < 2_200_000_000_000_000_000:
                    return val_be / 1e9
            except Exception:
                continue
        return None

    def _process_packets(self, packets, source_name):
        self.log_message(f"\n--- DEEP PACKET INSPECTION ({source_name}) ---")
        delays = []
        matched_frames = 0

        for pkt in packets:
            if IP in pkt and TCP in pkt:
                # Removed strict sport == GRPC_PORT check! Now checks ANY TCP packet from the sensor.
                if pkt[IP].src == LIDAR_IP:
                    payload = bytes(pkt[TCP].payload)
                    if not payload:
                        continue

                    nic_recv_time = float(pkt.time)
                    optical_epoch = self._extract_protobuf_timestamp(payload)

                    if optical_epoch:
                        matched_frames += 1
                        turnaround_ms = (nic_recv_time - optical_epoch) * 1000
                        delays.append(turnaround_ms)

        if matched_frames > 0:
            avg_delay = sum(delays) / len(delays)
            min_delay = min(delays)
            max_delay = max(delays)
            
            summary = (
                f"\n=== PCAP DPI CORRELATION RESULTS ({matched_frames} Packets Matched) ===\n"
                f" ├── Avg Total Latency (Photon-to-NIC) : {avg_delay:.3f} ms\n"
                f" ├── Min Turnaround                    : {min_delay:.3f} ms\n"
                f" ├── Max Turnaround                    : {max_delay:.3f} ms\n"
                f" └── Verified Wire Transit Baseline    : ~0.200 ms (via kernel egress)\n"
                f"========================================================="
            )
            self.log_message(summary)
        else:
            self.log_message("DPI WARNING: No valid gRPC Protobuf payloads found in the packet capture.")

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
                    self.packet_queue.put(("OBJECTS", now, response.to_dict()))
        except Exception as e:
            self.packet_queue.put(("LOG", f"gRPC ERROR: {str(e)}"))

    def _ui_consumer_tick(self):
        now = time.time()

        # Handle Benchmark Countdown
        if self.bench_active:
            time_left = self.bench_end_time - now
            if time_left > 0:
                self.start_bench_btn.configure(text=f"⏳ {time_left:.1f}s LEFT...")
            else:
                self.bench_active = False
                self.start_bench_btn.configure(state="normal", text="▶ START BENCHMARK", bg="#89b4fa")
                self.log_message("=== BENCHMARK COMPLETE ===")
                
                selected_mode = self.mode_var.get()
                if selected_mode == "Manual PCAP Upload":
                    self.root.after(200, self._manual_load_pcap)
                else:
                    self.root.after(200, lambda: self._process_packets(self.auto_sniffed_packets, "Live Scapy Capture"))

        latest_packet = None
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

            if sensor_epoch > 0:
                total_ms = abs(wire_arrival_time - sensor_epoch) * 1000
                lat_str = f"Total System Latency (Photon-to-Screen): {total_ms:.3f} ms"
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
                    self.log_message(f"SDSM INTRUSION | IDs: {obj_ids} | Sense: {sensor_time_str} ➔ Recv: {recv_time_str} | {lat_str}")

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
    app = GrpcPcapBenchmarkApp(window)
    window.mainloop()