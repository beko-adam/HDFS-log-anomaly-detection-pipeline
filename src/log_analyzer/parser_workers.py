import re
from collections import Counter, defaultdict


BLOCK_PATTERN = re.compile(r"blk_-?\d+")
TEMPLATES = None


def initialize_parser(templates):
    global TEMPLATES
    TEMPLATES = templates


def parse_chunk(lines):
    block_counts = defaultdict(Counter)

    statistics = {
        "total_lines": 0,
        "blank_lines": 0,
        "matched_lines": 0,
        "unmatched_lines": 0,
        "matched_lines_without_block": 0,
    }

    for line in lines:
        statistics["total_lines"] += 1

        if not line.strip():
            statistics["blank_lines"] += 1
            continue

        event_id = None

        for template_id, pattern in TEMPLATES:
            if pattern.fullmatch(line):
                event_id = template_id
                break

        if event_id is None:
            statistics["unmatched_lines"] += 1
            continue

        statistics["matched_lines"] += 1

        blocks = set(BLOCK_PATTERN.findall(line))

        if not blocks:
            statistics["matched_lines_without_block"] += 1

        for block_id in blocks:
            block_counts[block_id][event_id] += 1

    return dict(block_counts), statistics