import json
import unittest
from pathlib import Path

from route_parity import collect_routes, load_manifest, verify_manifest


ROOT = Path(__file__).resolve().parents[1]


class RouteParityTests(unittest.TestCase):
    def test_manifest_covers_registered_go_and_rust_routes(self):
        manifest = load_manifest(ROOT / "contracts" / "route-parity.json")
        go_routes, rust_routes = collect_routes(ROOT)
        errors = verify_manifest(manifest, go_routes, rust_routes)
        self.assertEqual(errors, [], "route parity manifest errors:\n" + "\n".join(errors))


    def test_manifest_is_machine_readable_and_marks_unported_routes_explicitly(self):
        data = json.loads((ROOT / "contracts" / "route-parity.json").read_text())
        self.assertEqual(data["version"], 1)
        self.assertTrue(data["routes"])
        for route in data["routes"]:
            self.assertTrue({"method", "path", "owner", "status"} <= route.keys())
            self.assertIn(route["status"], {"rust", "admin-static", "legacy-go", "compatibility"})

    def test_versioned_api_has_no_legacy_go_route(self):
        data = json.loads((ROOT / "contracts" / "route-parity.json").read_text())
        legacy_api = [
            f"{route['method']} {route['path']}"
            for route in data["routes"]
            if (route["path"].startswith("/api/v1") or route["path"].startswith("/api/v2"))
            and route["status"] == "legacy-go"
        ]
        self.assertEqual(legacy_api, [], "versioned API still depends on Go routes: " + ", ".join(legacy_api))

    def test_no_route_depends_on_the_legacy_go_process(self):
        data = json.loads((ROOT / "contracts" / "route-parity.json").read_text())
        legacy = [f"{route['method']} {route['path']}" for route in data["routes"] if route["status"] == "legacy-go"]
        self.assertEqual(legacy, [], "routes still depend on the retired Go process: " + ", ".join(legacy))


if __name__ == "__main__":
    unittest.main()
