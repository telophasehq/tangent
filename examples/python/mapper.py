from typing import List

import wit_world
from wit_world.exports import mapper
from wit_world.imports import log


def push_value(values: List[log.Value], value: log.Value) -> int:
    values.append(value)
    return len(values) - 1


def scalar_to_index(values: List[log.Value], scalar: log.Scalar) -> int:
    if isinstance(scalar, log.Scalar_Str):
        return push_value(values, log.Value_StringValue(scalar.value))
    if isinstance(scalar, log.Scalar_Int):
        return push_value(values, log.Value_S64Value(scalar.value))
    if isinstance(scalar, log.Scalar_Float):
        return push_value(values, log.Value_F64Value(scalar.value))
    if isinstance(scalar, log.Scalar_Boolean):
        return push_value(values, log.Value_BoolValue(scalar.value))
    if isinstance(scalar, log.Scalar_Bytes):
        return push_value(values, log.Value_BlobValue(scalar.value))
    return push_value(values, log.Value_NullValue())


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
    ) -> List[log.Frame]:
        frames: List[log.Frame] = []

        for lv in logs:
            values: List[log.Value] = []
            fields: List[log.Field] = []

            s = lv.get("msg")
            message = s.value if s is not None and hasattr(s, "value") else ""
            message_idx = push_value(values, log.Value_StringValue(message))
            fields.append(("message", message_idx))

            s = lv.get("msg.level")
            level = s.value if s is not None and hasattr(s, "value") else ""
            level_idx = push_value(values, log.Value_StringValue(level))
            fields.append(("level", level_idx))

            s = lv.get("seen")
            seen = int(s.value) if s is not None and hasattr(s, "value") else 0
            seen_idx = push_value(values, log.Value_S64Value(seen))
            fields.append(("seen", seen_idx))

            s = lv.get("duration")
            duration = float(s.value) if s is not None and hasattr(s, "value") else 0.0
            duration_idx = push_value(values, log.Value_F64Value(duration))
            fields.append(("duration", duration_idx))

            s = lv.get("source.name")
            service = s.value if s is not None and hasattr(s, "value") else ""
            service_idx = push_value(values, log.Value_StringValue(service))
            fields.append(("service", service_idx))

            lst = lv.get_list("tags")
            if lst is not None:
                tag_indices = [
                    scalar_to_index(values, item)
                    for item in lst
                    if hasattr(item, "value")
                ]
                tag_index = push_value(values, log.Value_ListValue(tag_indices))
            else:
                tag_index = push_value(values, log.Value_NullValue())

            fields.append(("tags", tag_index))

            metrics_idx = push_value(
                values,
                log.Value_MapValue(
                    [
                        ("duration", duration_idx),
                        ("seen", seen_idx),
                    ]
                ),
            )

            context_idx = push_value(
                values,
                log.Value_MapValue(
                    [
                        ("service", service_idx),
                        ("metrics", metrics_idx),
                        ("tags", tag_index),
                    ]
                ),
            )

            fields.append(("context", context_idx))

            frames.append(log.Frame(values=values, fields=fields))

        return frames
