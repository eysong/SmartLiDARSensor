import math
import time
from datetime import datetime, timezone
import blickfeld_qb2

# --- CONFIGURATION ---
# the exact name of the security zone polygon drawn in the Blickfeld WebGUI
TARGET_ZONE = "Security Zone 1"
# prevents terminal spam by forcing the script to wait 5 seconds between alerts
COOLDOWN_SECONDS = 5  

# --- AUTHENTICATION ---
# the LiDAR requires a secure API token to allow direct gRPC connections.
token_factory = blickfeld_qb2.TokenFactory(
    application_key_secret="2ee812bc2e745dddb8i1cmJwrEaz8ehy"
)

# --- CONNECTION ---
# opens a direct, authenticated gRPC channel to the sensor's IP address
with blickfeld_qb2.Channel(
    fqdn_or_ip="192.168.26.26", token=token_factory
) as channel:
  
  # connects specifically to the Qb2's internal 3D Perception engine
  service = blickfeld_qb2.percept_processing.services.Objects(channel)
  
  print(
      f"[*] Listening to gRPC stream for intrusions in '{TARGET_ZONE}'... (Rate"
      f" limit: 1 msg per {COOLDOWN_SECONDS}s)"
  )

  # initializes the rate limiter stopwatch to 0
  last_alarm_time = 0

  # --- MAIN STREAMING LOOP ---
  # this loop runs continuously at 15 frames per second
  for response in service.stream():
    
    # converts the incoming C++ Protobuf packet into a standard Python dictionary
    frame_data = response.to_dict() 

    # --- UN-NESTING THE DATA ---
    # the gRPC schema double-nests the objects data (frame -> objects -> objects).
    # this safely drills down to the actual dictionary of tracked physical masses.
    raw_objs = frame_data.get("objects", {})
    obj_map = (
        raw_objs.get("objects", {})
        if isinstance(raw_objs, dict) and "objects" in raw_objs
        else raw_objs
    )

    active_intrusions = []

    # loop through every tracked 3D cluster in the current frame
    for obj_id, obj in obj_map.items():
      # ignore any metadata properties that aren't physical objects
      if not isinstance(obj, dict):
        continue

      intruding = obj.get("intruding", {})

      # check if the LiDAR's internal math flagged this object as intruding a zone
      if intruding.get("value") is True or intruding.get("state") is True:
        
        intruder = obj.get("intruder", {})
        # safely extract the name of the zone the object is violating
        rep_zone = (
            intruder.get("zone_name")
            or intruder.get("zone_id")
            or intruder.get("name")
            or ""
        )

        # confirm the object is violating our specific target zone
        if rep_zone in (TARGET_ZONE, ""):
          
          # --- CLASSIFICATION TRANSLATION ---
          # grab the LiDAR's native physical size bucket calculation
          props = obj.get("properties", {}) or obj.get("classification", {})
          raw_size = props.get("size", "")

          # translate the raw volume metrics into plain English types
          friendly_type = "UNCLASSIFIED_MOTION"
          if "MEDIUM" in raw_size:
            friendly_type = "PERSON"
          elif "LARGE" in raw_size:
            friendly_type = "VEHICLE"
          elif "SMALL" in raw_size:
            friendly_type = "ANIMAL_OR_DEBRIS"

          # --- VELOCITY CALCULATION ---
          # use the Pythagorean theorem on the 3D velocity vectors (x, y, z in m/s)
          # then multiply by 2.23694 to convert to Miles Per Hour
          vel = obj.get("velocity", {})
          vx, vy, vz = vel.get("x", 0), vel.get("y", 0), vel.get("z", 0)
          speed_mph = math.sqrt(vx**2 + vy**2 + vz**2) * 2.23694

          # add the processed subject to this frame's list of intruders
          active_intrusions.append({
              "cluster_id": str(obj_id),
              "classification": friendly_type,
              "speed_mph": round(speed_mph, 1),
          })

    # --- ALARM GENERATION & RATE LIMITING ---
    # only proceed if there is at least one active subject in the zone
    if len(active_intrusions) > 0:
      current_time = time.time()

      # RATE LIMITER: Check if 5 seconds have passed since the last alert was printed
      if (current_time - last_alarm_time) >= COOLDOWN_SECONDS:
        
        # update the stopwatch to prevent immediate re-triggering
        last_alarm_time = current_time

        # upgrade the master threat level if a high-priority subject is detected
        master_threat = "MOTION_DETECTED"
        if any(x["classification"] == "PERSON" for x in active_intrusions):
          master_threat = "PERSON_DETECTED"
        elif any(x["classification"] == "VEHICLE" for x in active_intrusions):
          master_threat = "VEHICLE_DETECTED"

        # construct the final, clean JSON payload to send to the terminal
        clean_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "zone": TARGET_ZONE,
            "alarm_type": master_threat,
            "subject_count": len(active_intrusions),
            "subjects": active_intrusions,
        }

        print(clean_payload)