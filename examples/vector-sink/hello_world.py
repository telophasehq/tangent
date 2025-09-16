import json

# NOTE: wit_world is provided by componentize-py during build (compile-wasm)
import wit_world


class WitWorld(wit_world.WitWorld):
    def process_logs(self, logs: str) -> wit_world.Result[None, str]:
        try:
            json.loads(logs)
        except Exception as e:
            return wit_world.Err(str(e))

        return wit_world.Ok(None)
