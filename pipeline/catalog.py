import random

# Deterministic (seeded) shared catalog so extract.py (shipments) and inventory.py
# (stock snapshots) reference the exact same parts and warehouses — this is what
# keeps dim_part/dim_warehouse a valid foreign-key target for both fact tables.
_CATALOG_SEED = 42
_PART_CATEGORIES = ["Battery", "Motor", "Chassis", "Control", "Sensor", "Bracket", "Wiring Harness"]
_PART_VARIANTS = ["Standard", "Heavy Duty", "Compact", "Industrial", "Marine", "Rugged"]
_PART_COUNT = 500
_WAREHOUSE_COUNT = 5

WAREHOUSE_IDS = [f"WH-{i}" for i in range(1, _WAREHOUSE_COUNT + 1)]


def get_part_catalog():
    """Returns a stable list of part names, identical across every call/process."""
    rng = random.Random(_CATALOG_SEED)
    names = set()
    while len(names) < _PART_COUNT:
        category = rng.choice(_PART_CATEGORIES)
        variant = rng.choice(_PART_VARIANTS)
        model = rng.randint(100, 999)
        names.add(f"{variant} {category} Model {model}")
    return sorted(names)
