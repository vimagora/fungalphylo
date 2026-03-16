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
interproscan:
  # Optional: directory containing cluster_interproscan and related helpers.
  # On Puhti, prefer loading modules inside the job script:
  #   module load biokit
  #   module load interproscan
  bin_dir: ""
  # Optional: executable name (default: "cluster_interproscan")
  command: "cluster_interproscan"
mafft:
  # MAFFT alignment tool. On Puhti: module load mafft
  command: "mafft"
trimal:
  # trimAl alignment trimming tool.
  command: "trimal"
iqtree:
  # IQ-TREE phylogenetic tree builder. On Puhti: module load iqtree
  command: "iqtree2"
"""

@dataclass(frozen=True)
class BuscoTool:
    bin_dir: Optional[Path] = None
    command: str = "busco"


@dataclass(frozen=True)
class InterProScanTool:
    bin_dir: Optional[Path] = None
    command: str = "cluster_interproscan"


@dataclass(frozen=True)
class MafftTool:
    command: str = "mafft"


@dataclass(frozen=True)
class TrimalTool:
    command: str = "trimal"


@dataclass(frozen=True)
class IqtreeTool:
    command: str = "iqtree2"


@dataclass(frozen=True)
class ToolsConfig:
    busco: BuscoTool
    interproscan: InterProScanTool
    mafft: MafftTool = MafftTool()
    trimal: TrimalTool = TrimalTool()
    iqtree: IqtreeTool = IqtreeTool()


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
        return ToolsConfig(busco=BuscoTool(), interproscan=InterProScanTool())

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    busco = data.get("busco") or {}
    interproscan = data.get("interproscan") or {}

    bin_dir_raw = (busco.get("bin_dir") or "").strip()
    cmd = (busco.get("command") or "busco").strip() or "busco"
    ipr_bin_dir_raw = (interproscan.get("bin_dir") or "").strip()
    ipr_cmd = (interproscan.get("command") or "cluster_interproscan").strip() or "cluster_interproscan"

    bin_dir = Path(bin_dir_raw).expanduser().resolve() if bin_dir_raw else None
    ipr_bin_dir = Path(ipr_bin_dir_raw).expanduser().resolve() if ipr_bin_dir_raw else None

    mafft_data = data.get("mafft") or {}
    mafft_cmd = (mafft_data.get("command") or "mafft").strip() or "mafft"

    trimal_data = data.get("trimal") or {}
    trimal_cmd = (trimal_data.get("command") or "trimal").strip() or "trimal"

    iqtree_data = data.get("iqtree") or {}
    iqtree_cmd = (iqtree_data.get("command") or "iqtree2").strip() or "iqtree2"

    return ToolsConfig(
        busco=BuscoTool(bin_dir=bin_dir, command=cmd),
        interproscan=InterProScanTool(bin_dir=ipr_bin_dir, command=ipr_cmd),
        mafft=MafftTool(command=mafft_cmd),
        trimal=TrimalTool(command=trimal_cmd),
        iqtree=IqtreeTool(command=iqtree_cmd),
    )
