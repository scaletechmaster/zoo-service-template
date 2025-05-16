from __future__ import annotations
from typing import Dict
import pathlib

try:
    import zoo
except ImportError:

    class ZooStub(object):
        def __init__(self):
            self.SERVICE_SUCCEEDED = 3
            self.SERVICE_FAILED = 4

        def update_status(self, conf, progress):
            print(f"Status {progress}")

        def _(self, message):
            print(f"invoked _ with {message}")

    zoo = ZooStub()

import os
import sys
import traceback
import yaml
import json
import boto3  # noqa: F401
import botocore
from loguru import logger
from urllib.parse import urlparse
from botocore.exceptions import ClientError
from botocore.client import Config
from pystac import read_file, Collection, Catalog
from pystac.stac_io import DefaultStacIO, StacIO
from pystac.item_collection import ItemCollection
from zoo_calrissian_runner import ExecutionHandler, ZooCalrissianRunner


logger.remove()
logger.add(sys.stderr, level="INFO")


class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self):
        self.session = botocore.session.Session()
        self.s3_client = self.session.create_client(
            service_name="s3",
            region_name=os.environ["AWS_S3_REGION"],
            endpoint_url=os.environ["AWS_S3_ENDPOINT"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )

    def read_text(self, source, *args, **kwargs):
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            return (
                self.s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path[1:])[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(self, dest, txt, *args, **kwargs):
        parsed = urlparse(dest)
        if parsed.scheme == "s3":
            self.s3_client.put_object(
                Body=txt.encode("UTF-8"),
                Bucket=parsed.netloc,
                Key=parsed.path[1:],
                ContentType="application/geo+json",
            )
        else:
            super().write_text(dest, txt, *args, **kwargs)


StacIO.set_default(CustomStacIO)


class SimpleExecutionHandler(ExecutionHandler):
    def __init__(self, conf):
        super().__init__()
        self.conf = conf
        self.results = None

    def pre_execution_hook(self):

        logger.info("Pre execution hook")

    def post_execution_hook(self, log, output, usage_report, tool_logs):

        # unset HTTP proxy or else the S3 client will use it and fail
        os.environ.pop("HTTP_PROXY", None)

        os.environ["AWS_S3_REGION"] = self.get_additional_parameters()["region_name"]
        os.environ["AWS_S3_ENDPOINT"] = self.get_additional_parameters()["endpoint_url"]
        os.environ["AWS_ACCESS_KEY_ID"] = self.get_additional_parameters()["aws_access_key_id"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.get_additional_parameters()["aws_secret_access_key"]

        logger.info("Post execution hook")

        StacIO.set_default(CustomStacIO)

        logger.info(f"Read catalog from STAC Catalog URI: {output['s3_catalog_output']}")

        cat: Catalog  = read_file(output["s3_catalog_output"])

        collection_id = self.get_additional_parameters()["sub_path"]

        logger.info(f"Create collection with ID {collection_id}")

        collection = None

        collection: Collection = next(cat.get_all_collections())

        logger.info("Got collection {collection.id} from processing outputs")
        
        items = []
        
        for item in collection.get_all_items():

            logger.info("Processing item {item.id}")
            
            for asset_key in item.assets.keys():

                logger.info(f"Processing asset {asset_key}")
                
                temp_asset = item.assets[asset_key].to_dict()
                temp_asset["storage:platform"] = "eoap"
                temp_asset["storage:requester_pays"] = False
                temp_asset["storage:tier"] = "Standard"
                temp_asset["storage:region"] = self.get_additional_parameters()[
                    "region_name"
                ]
                temp_asset["storage:endpoint"] = self.get_additional_parameters()[
                    "endpoint_url"
                ]
                item.assets[asset_key] = item.assets[asset_key].from_dict(temp_asset)
            
            item.collection_id = collection_id

            items.append(item.clone())

        item_collection = ItemCollection(items=items)

        logger.info("Created feature collection from items")

        # Trap the case of no output collection
        if item_collection is None:
            logger.error("The output collection is empty")
            self.feature_collection = json.dumps({}, indent=2)
            return

        # Set the feature collection to be returned
        self.results = item_collection.to_dict()
        self.results["id"] = collection_id

    @staticmethod
    def local_get_file(fileName):
        """
        Read and load the contents of a yaml file

        :param yaml file to load
        """
        try:
            with open(fileName, "r") as file:
                data = yaml.safe_load(file)
            return data
        # if file does not exist
        except FileNotFoundError:
            return {}
        # if file is empty
        except yaml.YAMLError:
            return {}
        # if file is not yaml
        except yaml.scanner.ScannerError:
            return {}

    def get_pod_env_vars(self) -> Dict[str, str]:
        # This method is used to set environment variables for the pod
        # spawned by calrissian.

        logger.info("get_pod_env_vars")

        env_vars: Dict[str, str] = {}
        env_vars = self.conf.get("pod_env_vars", {})

        return env_vars

    def get_pod_node_selector(self) -> Dict[str, str]:
        # This method is used to set node selectors for the pod
        # spawned by calrissian.

        logger.info("get_pod_node_selector")
        node_selector: Dict[str, str] = {}
        node_selector = self.conf.get("pod_node_selector", {})

        logger.info(f"node_selector: {node_selector.keys()}")

        return node_selector

    def get_additional_parameters(self) -> Dict[str, str]:
        # sets the additional parameters for the execution
        # of the wrapped Application Package

        logger.info("get_additional_parameters")
        additional_parameters: Dict[str, str] = {}
        additional_parameters = self.conf.get("additional_parameters", {})
        # params override
        additional_parameters["aws_access_key_id"] = os.getenv('MINIO_ACCESS_KEY')
        additional_parameters["aws_secret_access_key"] = os.getenv('MINIO_SECRET_KEY')
        additional_parameters["endpoint_url"] = f"https://{os.getenv('MINIO_HOST')}"
        additional_parameters["s3_bucket"] = self.conf["lenv"]["Identifier"]

        additional_parameters["sub_path"] = self.conf["lenv"]["usid"]

        logger.info(f"additional_parameters: {additional_parameters.keys()}")

        return additional_parameters

    def handle_outputs(self, log, output, usage_report, tool_logs):
        """
        Handle the output files of the execution.

        :param log: The application log file of the execution.
        :param output: The output file of the execution.
        :param usage_report: The metrics file.
        :param tool_logs: A list of paths to individual workflow step logs.

        """

        try:
            logger.info("handle_outputs")

            logger.info(f"Set output to {output['s3_catalog_output']}")
            self.results = {"url": output["s3_catalog_output"]}

            self.conf["main"]["tmpUrl"] = self.conf["main"]["tmpUrl"].replace(
                "temp/", self.conf["auth_env"]["user"] + "/temp/"
            )
            services_logs = [
                {
                    "url": os.path.join(
                        self.conf["main"]["tmpUrl"],
                        f"{self.conf['lenv']['Identifier']}-{self.conf['lenv']['usid']}",
                        os.path.basename(tool_log),
                    ),
                    "title": f"Tool log {os.path.basename(tool_log)}",
                    "rel": "related",
                }
                for tool_log in tool_logs
            ]
            for i in range(len(services_logs)):
                okeys = ["url", "title", "rel"]
                keys = ["url", "title", "rel"]
                if i > 0:
                    for j in range(len(keys)):
                        keys[j] = keys[j] + "_" + str(i)
                if "service_logs" not in self.conf:
                    self.conf["service_logs"] = {}
                for j in range(len(keys)):
                    self.conf["service_logs"][keys[j]] = services_logs[i][okeys[j]]

            self.conf["service_logs"]["length"] = str(len(services_logs))
            logger.info(f"service_logs: {self.conf['service_logs']}")

        except Exception as e:
            logger.error("ERROR in handle_outputs...")
            logger.error(traceback.format_exc())
            raise (e)

    def get_secrets(self):
        logger.info("get_secrets")
        secrets={
            "auths": self.local_get_file("/assets/pod_imagePullSecrets.yaml"),
            "additionalImagePullSecrets": self.local_get_file("/assets/pod_additionalImagePullSecrets.yaml")
        }
        return secrets


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs):  # noqa

    try:
        with open(
            os.path.join(
                pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
                "app-package.cwl",
            ),
            "r",
        ) as stream:
            cwl = yaml.safe_load(stream)

        execution_handler = SimpleExecutionHandler(conf=conf)

        runner = ZooCalrissianRunner(
            cwl=cwl,
            conf=conf,
            inputs=inputs,
            outputs=outputs,
            execution_handler=execution_handler,
        )

        working_dir = os.path.join(conf["main"]["tmpPath"], runner.get_namespace_name())
        os.makedirs(
            working_dir,
            mode=0o777,
            exist_ok=True,
        )
        os.chdir(working_dir)

        exit_status = runner.execute()

        if exit_status == zoo.SERVICE_SUCCEEDED:
            logger.info(f"Setting Collection into output key {list(outputs.keys())[0]}")
            outputs[list(outputs.keys())[0]]["value"] = json.dumps(
                execution_handler.results, indent=2
            )
            return zoo.SERVICE_SUCCEEDED

        else:
            conf["lenv"]["message"] = zoo._("Execution failed")
            logger.error("Execution failed")
            return zoo.SERVICE_FAILED

    except Exception as e:

        logger.error("ERROR in processing execution template...")
        logger.error("Try to fetch the tool logs if any...")

        try:
            tool_logs = runner.execution.get_tool_logs()
            execution_handler.handle_outputs(None, None, None, tool_logs)
        except Exception as e:
            logger.error(f"Fetching tool logs failed! ({str(e)})")

        stack = traceback.format_exc()

        logger.error(stack)

        conf["lenv"]["message"] = zoo._(f"Exception during execution...\n{stack}\n")

        return zoo.SERVICE_FAILED
