
from typing import List

import json
import wit_world
from wit_world.exports import mapper
from wit_world.imports import log


class Mapper(wit_world.WitWorld):
    def metadata(self) -> mapper.Meta:
        return mapper.Meta(name="python", version="0.1.0")

    def probe(self) -> List[mapper.Selector]:
        # Match logs where source.name == "myservice"
        return [
            mapper.Selector(
                any=[],
                all=[
                    mapper.Pred_Eq(
                        ("source.name", log.Scalar_Str("myservice"))
                    )
                ],
                none=[],
            )
        ]

    def process_logs(
        self,
        logs: List[log.Logview]
    ) -> bytes:
        buf = bytearray()

        for lv in logs:
            with lv:
                out = {
                    "message": "",
                    "level": "",
                    "seen": 0,
                    "duration": 0.0,
                    "service": "",
                    "tags": None,
                }

                # get string
                s = lv.get("msg")
                if s is not None and hasattr(s, "value"):
                    out["message"] = s.value

                # get dot path
                s = lv.get("msg.level")
                if s is not None and hasattr(s, "value"):
                    out["level"] = s.value

                # get int
                s = lv.get("seen")
                if s is not None and hasattr(s, "value"):
                    out["seen"] = s.value

                # get float
                s = lv.get("duration")
                if s is not None and hasattr(s, "value"):
                    out["duration"] = s.value

                # get value from nested json
                s = lv.get("source.name")
                if s is not None and hasattr(s, "value"):
                    out["service"] = s.value

                # get string list
                lst = lv.get_list("tags")
                if lst is not None:
                    tags: List[str] = []
                    for item in lst:
                        tags.append(item.value)
                    out["tags"] = tags

                buf.extend(json.dumps(out).encode('utf-8') + b"\n")

        return bytes(buf)
