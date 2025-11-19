#!/usr/bin/env python3
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import yaml

THROUGHPUT_RE = re.compile(r"MB/s=(?P<mbs>[0-9.]+)")


def parse_args(argv: Sequence[str]) -> List[Tuple[str, Path, Path]]:
    if len(argv) == 0 or len(argv) % 3 != 0:
        raise SystemExit(
            "usage: extract_bench.py <language> <config> <bench_log> "
            "[<language> <config> <bench_log> ...]"
        )

    triples: List[Tuple[str, Path, Path]] = []
    for idx in range(0, len(argv), 3):
        lang, cfg, bench = argv[idx: idx + 3]
        triples.append((lang, Path(cfg), Path(bench)))
    return triples


def load_config_routes(cfg_path: Path) -> Tuple[List[str], Dict[str, List[str]]]:
    data = yaml.safe_load(cfg_path.read_text())
    dag_entries = data.get("dag", [])
    adjacency: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)
    for entry in dag_entries:
        frm = entry.get("from") or {}
        frm_key = (frm.get("kind"), frm.get("name"))
        for target in entry.get("to", []):
            tgt_key = (target.get("kind"), target.get("name"))
            adjacency[frm_key].append(tgt_key)

    sources = data.get("sources", {}) or {}
    source_names = sorted(sources.keys())

    source_routes: Dict[str, List[str]] = {}
    sources_cfg = data.get("sources", {}) or {}
    sinks_cfg = data.get("sinks", {}) or {}

    source_kinds: Dict[str, str] = {}
    for name, cfg in sources_cfg.items():
        kind = cfg.get("type") or cfg.get("kind") or "source"
        source_kinds[name] = str(kind)

    sink_kinds: Dict[str, str] = {}
    for name, cfg in sinks_cfg.items():
        kind = cfg.get("type")
        if kind is None:
            kind = cfg.get("kind")
            if isinstance(kind, dict):
                if len(kind) == 1:
                    kind = next(iter(kind.keys()))
                else:
                    kind = "sink"
        sink_kinds[name] = str(kind or "sink")

    for src in source_names:
        sinks = sorted(find_sinks(("source", src), adjacency))
        if sinks:
            source_routes[src] = sinks

    return source_names, source_routes, source_kinds, sink_kinds


def find_sinks(
    start: Tuple[str, str], adjacency: Dict[Tuple[str, str], List[Tuple[str, str]]]
) -> Iterable[str]:
    sinks = set()
    stack = [start]
    visited = set()

    while stack:
        node = stack.pop()
        for nxt in adjacency.get(node, []):
            if nxt in visited:
                continue
            visited.add(nxt)
            if nxt[0] == "sink":
                sinks.add(nxt[1])
            else:
                stack.append(nxt)

    return sinks


def extract_throughputs(path: Path) -> List[float]:
    txt = path.read_text()
    matches = THROUGHPUT_RE.findall(txt)
    if not matches:
        raise RuntimeError(f"no throughput entries found in {path}")
    return [float(m) for m in matches]


def format_table(triples: List[Tuple[str, Path, Path]]) -> str:
    lang_order: List[str] = []
    results: Dict[str, Dict[str, str]] = defaultdict(
        dict)  # route -> {lang: value}

    for lang, cfg_path, bench_path in triples:
        if lang not in lang_order:
            lang_order.append(lang)

        sources, routes, source_kinds, sink_kinds = load_config_routes(
            cfg_path)
        throughputs = extract_throughputs(bench_path)

        if len(throughputs) != len(sources):
            raise RuntimeError(
                f"{bench_path} produced {len(throughputs)} throughput entries, "
                f"but config {cfg_path} has {len(sources)} sources."
            )

        for src, mb in zip(sources, throughputs):
            route_targets = routes.get(src)
            if not route_targets:
                continue
            src_kind = source_kinds.get(src, "source")
            for sink_name in route_targets:
                sink_kind = sink_kinds.get(sink_name, "sink")
                route_label = f"{src_kind} -> {sink_kind}"
                results[route_label][lang] = f"{mb:.2f}"

    all_routes = sorted(results.keys())
    if not all_routes:
        return "No routes with sinks found."

    header = "| Source -> Sink | " + " | ".join(lang_order) + " |"
    divider = "|---|" + "|".join("---" for _ in lang_order) + "|"
    lines = ["### Throughput (MB/s)", "", header, divider]

    for route in all_routes:
        row = [route]
        for lang in lang_order:
            val = results[route].get(lang, "-")
            row.append(f"`{val}`" if val != "-" else "-")
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    sys.stdout.write(format_table(args))
