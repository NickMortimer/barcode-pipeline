from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import yaml
import subprocess
import shlex
import datetime
import uuid
import pandas as pd
import typer

from ifdo.models import ImageData

from marimba.core.pipeline import BasePipeline
from marimba.core.pipeline import BasePipeline
from marimba.core.utils.constants import Operation
from marimba.core.wrappers.dataset import DatasetWrapper
from marimba.core.utils.rich import error_panel
from marimba.lib import image
from marimba.lib.concurrency import multithreaded_generate_image_thumbnails
from marimba.main import __version__


class BarCodePipeline(BasePipeline):

    @staticmethod
    def get_pipeline_config_schema() -> dict:
        return {
            "voyage": "IN2024-05",
            "operation_file": "",
        }

    def initialise(self,file_path):
         
        """
        initialise the card so that if the copy fails it will be able to restart the copy
        """

        import_dict =self.config.copy()
        import_dict["importdate"] = f"{datetime.now():%Y-%m-%d}"
        import_dict["importtoken"]  = str(uuid.uuid4())[0:8]
        with open(file_path, 'w') as file:
            yaml.dump(import_dict, file, default_flow_style=False)

    def execute_command(self, command):
        if not self.dry_run:
            process = subprocess.Popen(shlex.split(command))
            process.wait()
    
    @staticmethod
    def get_collection_config_schema() -> dict:
        return {}

    def _import(
        self,
        data_dir: Path,
        source_path: Path,
        config: Dict[str, Any],
        operation = Operation.copy,
        **kwargs: dict,
    ):
        
        self.logger.info(f"Importing data from {source_path=} to {data_dir}")
        import_file = source_path /'import.yml'
        if import_file.exists():
             pass
        else:
            self.initialise(import_file)
        with open(import_file, 'r') as file:
            import_details = yaml.safe_load(file)

        processing_path = data_dir / 'processing' / 'temp_exif'
        processing_path.mkdir(parents=True,exist_ok=True)
        exif_image_cache = processing_path / f"{import_details['importtoken']}_image_exif.json"
        # Define the directory containing your files
        command = f'exiftool -ext jpg -ext JPG -r -f -json {str(source_path)} >{str(exif_image_cache)}'
        result = subprocess.run(command, capture_output=True, text=True,shell=True)
        print('Images:')
        print(result.stderr)
        image_data = pd.DataFrame()
        if (exif_image_cache.exists()) and (exif_image_cache.stat().st_size>0):
            image_data = pd.read_json(exif_image_cache)
            image_data = image_data.loc[~image_data.DateTimeOriginal.isna()]
        
        image_serial,image_date =self.get_serialnumbaer(image_data)
        if (image_serial is None) :
            print(error_panel('{source_path} cannot find tags'))
            raise typer.Exit()

        destination =   data_dir / 'cards' / import_details["importdate"] / f"{image_serial}_{import_details['importtoken']}"
        destination.mkdir(parents=True,exist_ok=True)
        if operation == Operation.move:
            command = f"rclone move {source_path.resolve()} {destination.resolve()} --progress --delete-empty-src-dirs"
            self.logger.info(
                f"Using Rclone to move {source_path.resolve()} to {destination.resolve()}"
            )
            destination.mkdir(parents=True, exist_ok=True)
            self.execute_command(command)
        # Execute copy command if applicable
        elif operation == Operation.copy:
            command = f"rclone copy {source_path.resolve()} {destination.resolve()} --progress --low-level-retries 1 "
            self.logger.info(
                f"Using Rclone to copy {source_path.resolve()} to {destination.resolve()}"
            )
            destination.mkdir(parents=True, exist_ok=True)
            self.execute_command(command)
        if len(image_data)>0:
            image_data.SourceFile =image_data.SourceFile.str.replace(str(source_path),'{CATALOG_DIR}')
            image_data.to_csv(destination / '.image_exif.csv')

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
    
# def run():
#     from doit.cmd_base import ModuleTaskLoader, get_loader
#     from doit.doit_cmd import DoitMain
#     DOIT_CONFIG = {'check_file_uptodate': 'timestamp',"continue": True}
#     #print(globals())
#     DoitMain(ModuleTaskLoader(globals())).run(sys.argv[1:])
#     ):
