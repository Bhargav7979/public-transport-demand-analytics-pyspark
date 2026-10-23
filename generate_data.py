import csv
import random
from datetime import datetime, timedelta

# Set seed for reproducible synthetic data
random.seed(42)

# Synthetic Routes
routes = [
    "Route 101 - Downtown Express",
    "Route 202 - Tech Hub Shuttle",
    "Route 303 - University Circular",
    "Route 404 - Airport Link",
    "Route 505 - Suburban Metro Feed",
    "Route 606 - Coastal Transit"
]

# Time slots across a transit operating day
time_slots = [
    "06:00 - 08:00 (Early Morning)",
    "08:00 - 10:00 (Morning Peak)",
    "10:00 - 13:00 (Midday Off-Peak)",
    "13:00 - 16:00 (Afternoon Off-Peak)",
    "16:00 - 19:00 (Evening Peak)",
    "19:00 - 22:00 (Late Evening)"
]

# Base passenger demand multipliers by time slot
slot_multipliers = {
    "06:00 - 08:00 (Early Morning)": 0.65,
    "08:00 - 10:00 (Morning Peak)": 1.45,
    "10:00 - 13:00 (Midday Off-Peak)": 0.80,
    "13:00 - 16:00 (Afternoon Off-Peak)": 0.75,
    "16:00 - 19:00 (Evening Peak)": 1.50,
    "19:00 - 22:00 (Late Evening)": 0.50
}

# Route base volume
route_base = {
    "Route 101 - Downtown Express": 520,
    "Route 202 - Tech Hub Shuttle": 610,
    "Route 303 - University Circular": 440,
    "Route 404 - Airport Link": 390,
    "Route 505 - Suburban Metro Feed": 480,
    "Route 606 - Coastal Transit": 310
}

# Generate 30 days of data (e.g., September 1 to September 30, 2026)
start_date = datetime(2026, 9, 1)
data_rows = []

for day_offset in range(30):
    current_date = start_date + timedelta(days=day_offset)
    date_str = current_date.strftime("%Y-%m-%d")
    is_weekend = current_date.weekday() >= 5

    for route in routes:
        base = route_base[route]
        if is_weekend:
            # Lower overall commute, higher recreational / coastal
            if route == "Route 606 - Coastal Transit":
                base *= 1.3
            else:
                base *= 0.65

        for slot in time_slots:
            mult = slot_multipliers[slot]
            # Random variation ±15%
            noise = random.uniform(0.85, 1.15)
            passengers = int(base * mult * noise)
            passengers = max(50, passengers)

            # Buses operating allocated proportional to expected demand
            buses = max(2, int(passengers / random.uniform(38, 52)))

            data_rows.append({
                "Date": date_str,
                "Route": route,
                "TimeSlot": slot,
                "Passengers": passengers,
                "BusesOperating": buses
            })

output_file = "e:/BDA Project/data/passenger_data.csv"
import os
os.makedirs(os.path.dirname(output_file), exist_ok=True)

with open(output_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["Date", "Route", "TimeSlot", "Passengers", "BusesOperating"])
    writer.writeheader()
    writer.writerows(data_rows)

print(f"Generated {len(data_rows)} rows of synthetic passenger data in {output_file}")
