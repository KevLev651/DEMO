from pathlib import Path
import os

BASE_DIR = Path(r"c:\Users\snows\Desktop\DemoHost")
STATIC_DIR = BASE_DIR / "portal" / "static"
VIDEO_DIR = STATIC_DIR / "videos"

tools = ["scanner", "comparator", "ifb_gmp", "data_cube", "one_line"]
extensions = [".mp4", ".webm", ".mov"]

print(f"Checking VIDEO_DIR: {VIDEO_DIR}")
print(f"Exists: {VIDEO_DIR.exists()}")

for tool in tools:
    found = False
    for ext in extensions:
        candidate = VIDEO_DIR / f"{tool}_example{ext}"
        if candidate.exists():
            print(f"FOUND: {candidate}")
            found = True
    if not found:
        print(f"NOT FOUND: {tool}")
