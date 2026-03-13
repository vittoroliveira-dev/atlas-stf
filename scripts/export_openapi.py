"""Export OpenAPI spec from the FastAPI application."""

import json
import sys

from atlas_stf.api.app import create_app

app = create_app(database_url="sqlite:///dummy.db")
spec = app.openapi()

output = sys.argv[1] if len(sys.argv) > 1 else "openapi.json"
with open(output, "w", encoding="utf-8") as f:
    json.dump(spec, f, ensure_ascii=False, indent=2)
print(f"OpenAPI spec exported to {output}")
