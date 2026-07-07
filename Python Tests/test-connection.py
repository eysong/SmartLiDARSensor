import blickfeld_qb2
import time

print("Testing connection to Qb2...")
token_factory = blickfeld_qb2.TokenFactory(
    application_key_secret="2ee812bc2e745dddb8i1cmJwrEaz8ehy"
)

try:
    print("Creating channel...")
    channel = blickfeld_qb2.Channel(
        fqdn_or_ip="192.168.26.26",
        token=token_factory
    )
    print("Channel created.")

    print("Creating PointCloud service...")
    service = blickfeld_qb2.core_processing.services.PointCloud(channel)
    print("Service created.")

    print("Getting frame...")
    start = time.time()
    frame = service.get().frame
    end = time.time()
    print(f"Frame received in {end-start:.2f} seconds.")
    print(f"Frame ID: {frame.id}")

    if hasattr(frame, 'binary') and hasattr(frame.binary, 'cartesian'):
        cartesian = frame.binary.cartesian
        print(f"Point cloud shape: {cartesian.shape}")
        print(f"First few points:\n{cartesian[:5]}")
    else:
        print("Could not extract point cloud data")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    print("Test complete.")