logs = ["ERROR: Disk Full on ServerA", "INFO: User login success", "WARNING: High CPU on ServerB", "ERROR: Database connection lost"]

for log in logs:
    if log.startswith("ERROR"):
        # Split berdasarkan ":" dan ambil bagian setelahnya (detail pesan)
        detail = log.split(":", 1)[1].strip()
        print(f"ERROR {detail}")