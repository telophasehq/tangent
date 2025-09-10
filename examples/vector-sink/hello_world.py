import json

# NOTE: wit_world is provided by componentize-py during build (compile-wasm)
import wit_world


class WitWorld(wit_world.WitWorld):
    def process_logs(self, logs: str) -> None:
        events = json.loads(logs)
        print(f"hello world! I see {len(events)} logs")

        return None
