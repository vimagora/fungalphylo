from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

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
    bin_dir: Path | None = None
    command: str = "busco"


@dataclass(frozen=True)
class InterProScanTool:
    bin_dir: Path | None = None
    command: str = "cluster_interproscan"


@dataclass(frozen=True)
class MafftTool:
    bin_dir: Path | None = None
    command: str = "mafft"


@dataclass(frozen=True)
class TrimalTool:
    bin_dir: Path | None = None
    command: str = "trimal"


@dataclass(frozen=True)
class IqtreeTool:
    bin_dir: Path | None = None
    command: str = "iqtree2"


@dataclass(frozen=True)
class FasttreeTool:
    bin_dir: Path | None = None
    command: str = "fasttree"


@dataclass(frozen=True)
class ToolsConfig:
    busco: BuscoTool
    interproscan: InterProScanTool
    mafft: MafftTool = MafftTool()
    trimal: TrimalTool = TrimalTool()
    iqtree: IqtreeTool = IqtreeTool()
    fasttree: FasttreeTool = FasttreeTool()


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
    mafft_bin_dir_raw = (mafft_data.get("bin_dir") or "").strip()
    mafft_bin_dir = Path(mafft_bin_dir_raw).expanduser().resolve() if mafft_bin_dir_raw else None

    trimal_data = data.get("trimal") or {}
    trimal_cmd = (trimal_data.get("command") or "trimal").strip() or "trimal"
    trimal_bin_dir_raw = (trimal_data.get("bin_dir") or "").strip()
    trimal_bin_dir = Path(trimal_bin_dir_raw).expanduser().resolve() if trimal_bin_dir_raw else None

    iqtree_data = data.get("iqtree") or {}
    iqtree_cmd = (iqtree_data.get("command") or "iqtree2").strip() or "iqtree2"
    iqtree_bin_dir_raw = (iqtree_data.get("bin_dir") or "").strip()
    iqtree_bin_dir = Path(iqtree_bin_dir_raw).expanduser().resolve() if iqtree_bin_dir_raw else None

    fasttree_data = data.get("fasttree") or {}
    fasttree_cmd = (fasttree_data.get("command") or "fasttree").strip() or "fasttree"
    fasttree_bin_dir_raw = (fasttree_data.get("bin_dir") or "").strip()
    fasttree_bin_dir = Path(fasttree_bin_dir_raw).expanduser().resolve() if fasttree_bin_dir_raw else None

    return ToolsConfig(
        busco=BuscoTool(bin_dir=bin_dir, command=cmd),
        interproscan=InterProScanTool(bin_dir=ipr_bin_dir, command=ipr_cmd),
        mafft=MafftTool(bin_dir=mafft_bin_dir, command=mafft_cmd),
        trimal=TrimalTool(bin_dir=trimal_bin_dir, command=trimal_cmd),
        iqtree=IqtreeTool(bin_dir=iqtree_bin_dir, command=iqtree_cmd),
        fasttree=FasttreeTool(bin_dir=fasttree_bin_dir, command=fasttree_cmd),
    )


def bin_dir_export_lines(bin_dirs: list[Path | None]) -> str:
    """Generate shell export lines for bin_dirs that are set."""
    dirs = [str(d) for d in bin_dirs if d is not None]
    if not dirs:
        return ""
    joined = ":".join(dirs)
    return f'export PATH="{joined}:$PATH"\n'
