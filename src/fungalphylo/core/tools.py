from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


TOOLS_YAML_TEMPLATE = """# tools.yaml - configure paths to executables/environments not provided by modules
busco:
  # Directory containing the 'busco' executable.
  # Example: /scratch/project_2015320/software/busco_env/bin
  bin_dir: ""
  # Optional: executable name (default: "busco")
  command: "busco"
"""

@dataclass(frozen=True)
class BuscoTool:
    bin_dir: Optional[Path] = None
    command: str = "busco"


@dataclass(frozen=True)
class ToolsConfig:
    busco: BuscoTool


def load_tools(project_dir: Path) -> ToolsConfig:
    """
    Load <project_dir>/tools.yaml.

    Expected structure:
      busco:
        bin_dir: "/path/to/bin"
        command: "busco"
    """
    project_dir = project_dir.expanduser().resolve()
    path = project_dir / "tools.yaml"
    if not path.exists():
        # default empty config
        return ToolsConfig(busco=BuscoTool())

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    busco = data.get("busco") or {}

    bin_dir_raw = (busco.get("bin_dir") or "").strip()
    cmd = (busco.get("command") or "busco").strip() or "busco"

    bin_dir = Path(bin_dir_raw).expanduser().resolve() if bin_dir_raw else None

    return ToolsConfig(busco=BuscoTool(bin_dir=bin_dir, command=cmd))