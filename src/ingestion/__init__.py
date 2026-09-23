import os

# Prefect reads this setting when it is imported, and every flow module lives here.
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")
