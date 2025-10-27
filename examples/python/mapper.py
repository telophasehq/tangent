from typing import List

import json
import wit_world
from wit_world.exports import mapper
from wit_world.imports import log


class Mapper(wit_world.WitWorld):
    def metadata(self) -> mapper.Meta:
        return mapper.Meta(name="example-mapper", version="0.1.3")

    def probe(self) -> List[mapper.Selector]:
        return [mapper.Selector(any=[], all=[], none=[])]

    def process_logs(
        self,
        logs: List[log.Logview]
    ) -> wit_world.Result[bytes, str]:
        buf = bytearray()

        for lv in logs:
            with lv:
                out = {}
                # Check presence
                if lv.has("message"):
                    s = lv.get("message")  # Optional[log.Scalar]
                    message = s.value if s is not None else None
                    out.message = message

                # Get string field
                s = lv.get("host.name")
                out.host = s.value if s is not None else None

                # Get list field
                lst = lv.get_list("tags")  # Optional[List[log.Scalar]]
                out.tags = [x.value for x in lst] if lst is not None else []

                # Get map/object field
                # Optional[List[Tuple[str, log.Scalar]]]
                m = lv.get_map("labels")
                out.labels = {k: v.value for k,
                              v in m} if m is not None else {}

                # Inspect available keys at a path
                out.top_keys = lv.keys("")  # top-level keys
                out.detail_keys = lv.keys("detail")

                buf.extend(json.dumps(out).encode("utf-8") + b"\n")

        return wit_world.Ok(bytes(buf))
