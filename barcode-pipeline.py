from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ifdo.models import ImageData

from marimba.core.pipeline import BasePipeline


class BarCodePipeline(BasePipeline):

    @staticmethod
    def get_pipeline_config_schema() -> dict:
        return {}

    @staticmethod
    def get_collection_config_schema() -> dict:
        return {}

    def _import(
        self,
        data_dir: Path,
        source_path: Path,
        config: Dict[str, Any],
        **kwargs: dict,
    ):

        return

    def _process(
        self,
        data_dir: Path,
        config: Dict[str, Any],
        **kwargs: dict,
    ):

        return

    def _package(
        self,
        data_dir: Path,
        config: Dict[str, Any],
        **kwargs: dict,
    ) -> Dict[Path, Tuple[Path, Optional[List[ImageData]], Optional[Dict[str, Any]]]]:

        data_mapping: Dict[Path, Tuple[Path, Optional[List[ImageData]], Optional[Dict[str, Any]]]] = {}
        return data_mapping