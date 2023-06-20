import argparse
import logging
import os
import time
from datetime import datetime, timedelta

from bioblend import ConnectionError
from bioblend.galaxy import GalaxyInstance


def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-a", "--api-key", required=True, help="Admin API Key")
    arg_parser.add_argument("-u", "--url", required=True, help="Galaxy URL")
    arg_parser.add_argument(
        "--debug", action="store_true", default=False, help="Print debug information"
    )
    arg_parser.add_argument(
        "-l",
        "--limit_tools",
        action="store_true",
        default=False,
        help="Limit the number of tools to be downloaded to the ones used in the last week.",
    )
    # add option for the amount of time in the past that tools used should be considered
    arg_parser.add_argument(
        "-t",
        "--time",
        default=7,
        type=int,
        help="The amount of time in the past that tools used should be considered for container download, in days. Only applicable if limit_tools is set to True.",
    )
    # dry run mode
    arg_parser.add_argument(
        "-d",
        "--dry_run",
        action="store_true",
        default=False,
        help="Dry run mode. Only print the tools that would be downloaded.",
    )
    return arg_parser.parse_args()

    args = arg_parser.parse_args()
    return args


def set_logging_level(debug=False):
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%d-%m-%y %H:%M:%S",
    )


def main():
    """
    Downloads singularity containers on a Galaxy instance which is configured
    to use such dependency resolvers (mulled_singularity and cached_mulled_singularity).

    It will request the resolution of tools to containers from the instance,
    and then for all mulled_singularity resolved tools that are pointing to
    docker://<container-url>, it will download and build the singularity SIF.
    This download is done only once per container, and applies after for all tools
    that will make use of it.
    """
    args = get_args()
    set_logging_level(debug=args.debug)
    gi = GalaxyInstance(url=args.url, key=args.api_key)

    tools_deps = gi.make_get_request(
        gi.base_url + "/api/container_resolvers/toolbox"
    ).json()

    # if limit_tools is set to True, we limit the number of tools to be downloaded to the ones
    # used in the last week.
    unique_tool_ids = set()
    if args.limit_tools:
        today = datetime.now()
        # set min date range in YYYY-MM-DD format
        min_date = (today - timedelta(days=args.time)).strftime("%Y-%m-%d")
        # set max date range in YYYY-MM-DD format
        max_date = today.strftime("%Y-%m-%d")
        jobs = gi.jobs.get_jobs(date_range_min=min_date, date_range_max=max_date)
        # get unique tool ids in the jobs dictionary
        for job in jobs:
            if "tool_id" in job:
                unique_tool_ids.add(job["tool_id"])
        logging.info(
            f"Found {len(unique_tool_ids)} unique tools used in the last {args.time} days."
        )

    # We keep a container url to tools, since the
    # RAW API call to download a container requires a tool identifier (but only one).
    container2tool_id = dict()
    for tool_deps in tools_deps:
        if args.limit_tools and tool_deps["tool_id"] not in unique_tool_ids:
            logging.info(
                f"Tool {tool_deps['tool_id']} is not used in the last {args.time} days. Skipping..."
            )
            continue
        if "container_description" in tool_deps["status"]:
            if "identifier" in tool_deps["status"]["container_description"]:
                if tool_deps["status"]["container_description"][
                    "identifier"
                ].startswith("docker://"):
                    container2tool_id[
                        tool_deps["status"]["container_description"]["identifier"]
                    ] = tool_deps["tool_id"]

    downloads = 0
    for cont in container2tool_id:
        logging.info(f"Retrieving container {cont}...")
        tool_id = container2tool_id[cont]
        try:
            if not args.dry_run:
                result = gi.make_post_request(
                    url=gi.base_url + "/api/container_resolvers/toolbox/install",
                    payload={"tool_ids": [tool_id]},
                )
                logging.debug(result)
            else:
                logging.info(
                    f"Conatainer {cont} would be downloaded, but running on dry run mode."
                )
        except ConnectionError as e:
            logging.warning(
                "Connection interrupted... waiting for potential download before proceeding with next container."
            )
            time.sleep(20)
        downloads += 1

    logging.info(f"Downloaded {downloads} containers.")
    if args.dry_run:
        logging.info("But not really, this was a dry run mode.")


if __name__ == "__main__":
    main()
