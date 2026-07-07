import math
import time
from datetime import datetime, timezone
import blickfeld_qb2


# setting up the security zone & rate limit (to prevent messages from flooding the terminal)
TARGET_ZONE = "Security Zone 1"
COOLDOWN_SECONDS = 5  # rate limit threshold

# setting up Qb2's API key
token_factory = blickfeld_qb2.TokenFactory(
    application_key_secret="2ee812bc2e745dddb8i1cmJwrEaz8ehy"
)

# setting up the gRPC channel to the sensor's IP address
with blickfeld_qb2.Channel(
    fqdn_or_ip="192.168.26.26", token=token_factory
) as channel:
  
  # connects specifically to the Qb2's itnernal 3D perception engine (using Qb2's API)
  service = blickfeld_qb2.percept_processing.services.Objects(channel)
  print(
      f"[*] Listening to gRPC stream for intrusions in '{TARGET_ZONE}'... (Rate"
      f" limit: 1 msg per {COOLDOWN_SECONDS}s)"
  )

  # initializes rate limiter stopwatch to 0 
  last_alarm_time = 0

  # main streaming loop, runs at 15 frames/second
  for response in service.stream():
    frame_data = response.to_dict() # converts incoming C++ protobuf packet into a standard Python dictionary (to be read)

    # un-nesting the data
    # the gRPC schema double-nests the objects data (frame -> object -> objects), getting to the dictionary of tracked masses
    raw_objs = frame_data.get("objects", {})
    obj_map = (
        raw_objs.get("objects", {})
        if isinstance(raw_objs, dict) and "objects" in raw_objs
        else raw_objs
    )

    active_intrusions = []

    # looping through every tracked 3D cluster in the current frame
    for obj_id, obj in obj_map.items():

      # ignores any metadata properties that aren't physical objects
      if not isinstance(obj, dict):
        continue

      intruding = obj.get("intruding", {})

      # check if LiDAR's internal math flagged this object as intruding a zone
      if intruding.get("value") is True or intruding.get("state") is True:
        intruder = obj.get("intruder", {})

        # extract the naem of the zone the object is violating
        rep_zone = (
            intruder.get("zone_name")
            or intruder.get("zone_id")
            or intruder.get("name")
            or ""
        )

        # checking if the object is violating our specified zone
        if rep_zone in (TARGET_ZONE, ""):

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

          # uses Pythagorean theorem on 3D vectors to get velocity in miles per hour
          vel = obj.get("velocity", {})
          vx, vy, vz = vel.get("x", 0), vel.get("y", 0), vel.get("z", 0)
          speed_mph = math.sqrt(vx**2 + vy**2 + vz**2) * 2.23694

          # add processed subject to this frame's list of intruders
          active_intrusions.append({
              "cluster_id": str(obj_id),
              "classification": friendly_type,
              "speed_mph": round(speed_mph, 1),
          })

    # check if there's an active intrusion event
    if len(active_intrusions) > 0:
      current_time = time.time()

      # RATE LIMITER: only proceed if enough time has passed since last_alarm_time
      if (current_time - last_alarm_time) >= COOLDOWN_SECONDS: # in this case cooldown is 5
        # update our tracking timestamp immediately
        last_alarm_time = current_time

        # upgrade master threat level if a high-priority subject is detected
        master_threat = "MOTION_DETECTED"
        if any(x["classification"] == "PERSON" for x in active_intrusions):
          master_threat = "PERSON_DETECTED"
        elif any(x["classification"] == "VEHICLE" for x in active_intrusions):
          master_threat = "VEHICLE_DETECTED"

        # constructing the final, clean JSON payload to send to terminal
        clean_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "zone": TARGET_ZONE,
            "alarm_type": master_threat,
            "subject_count": len(active_intrusions),
            "subjects": active_intrusions,
        }

        print(clean_payload)