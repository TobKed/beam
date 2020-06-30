#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Script for downloading GitHub Actions artifacts from 'Build python wheels' workflow."""
import argparse
import itertools
import os
import shutil
import tempfile
import time
import zipfile

import dateutil.parser
import requests

ARTIFACTS_FOLDER = "artifacts"
ARTIFACTS_DIRECTORY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ARTIFACTS_FOLDER)

GH_API_URL_WORKLOW_FMT = (
    "https://api.github.com/repos/{repo_url}/actions/workflows/build_wheels.yml"
)
GH_API_URL_WORKFLOW_RUNS_FMT = (
    "https://api.github.com/repos/{repo_url}/actions/workflows/{workflow_id}/runs"
)
GH_WEB_URL_WORKLOW_RUN_FMT = "https://github.com/TobKed/beam/actions/runs/{workflow_id}"


def parse_arguments():
  parser = argparse.ArgumentParser(
      description=
      "Script for downloading GitHub Actions artifacts from 'Build python wheels' workflow."
  )
  parser.add_argument("--github-token", required=True)
  parser.add_argument("--github-user", required=True)
  parser.add_argument("--repo-url", required=True)
  parser.add_argument("--release-branch", required=True)
  parser.add_argument("--release-commit", required=True)

  args = parser.parse_args()

  global GITHUB_TOKEN, USER_GITHUB_ID, REPO_URL, RELEASE_BRANCH, RELEASE_COMMIT
  GITHUB_TOKEN = args.github_token
  USER_GITHUB_ID = args.github_user
  REPO_URL = args.repo_url
  RELEASE_BRANCH = args.release_branch
  RELEASE_COMMIT = args.release_commit


def requester(url, *args, return_raw_request=False, **kwargs):
  """Helper function form making requests authorized by GitHub token"""
  r = requests.get(url, *args, auth=("token", GITHUB_TOKEN), **kwargs)
  r.raise_for_status()
  if return_raw_request:
    return r
  return r.json()


def yes_or_no(question):
  """Helper function to ask yes or no question"""
  reply = str(input(question + " (y/n): ")).lower().strip()
  if reply == "y":
    return True
  if reply == "n":
    return False
  else:
    return yes_or_no("Uhhhh... please enter ")


def get_build_wheels_workflow_id():
  url = GH_API_URL_WORKLOW_FMT.format(repo_url=REPO_URL)
  data = requester(url)
  return data["id"]


def get_last_run(workflow_id):
  url = GH_API_URL_WORKFLOW_RUNS_FMT.format(
      repo_url=REPO_URL, workflow_id=workflow_id)
  event_types = ["push", "pull_request"]
  runs = []
  for event in event_types:
    data = requester(
        url,
        params={
            "event": event, "branch": RELEASE_BRANCH
        },
    )
    runs.extend(data["workflow_runs"])

  filtered_commit_runs = list(
      filter(lambda w: w.get("head_sha", "") == RELEASE_COMMIT, runs))
  if not filtered_commit_runs:
    workflow_web_url = GH_WEB_URL_WORKLOW_RUN_FMT.format(
        workflow_id=workflow_id)
    raise Exception(f"No runs for workflow. Verify at {workflow_web_url}")

  sorted_runs = sorted(
      filtered_commit_runs,
      key=lambda w: dateutil.parser.parse(w["created_at"]),
      reverse=True,
  )
  last_run = sorted_runs[0]
  print(
      f"Found last run. SHA: {RELEASE_COMMIT}, created_at: '{last_run['created_at']}', id: {last_run['id']}"
  )
  workflow_web_url = GH_WEB_URL_WORKLOW_RUN_FMT.format(
      workflow_id=last_run["id"])
  print(f"Verify at {workflow_web_url}")
  print(
      f"Optional upload to GCS will be available at:\n"
      f"\tgs://beam-wheels-staging/{RELEASE_BRANCH}/{RELEASE_COMMIT}-{workflow_id}/"
  )
  return last_run


def validate_run(run_data):
  status = run_data["status"]
  conclusion = run_data["conclusion"]
  if status == "completed" and conclusion == "success":
    return run_data

  url = run_data["url"]
  workflow_web_url = GH_WEB_URL_WORKLOW_RUN_FMT.format(
      workflow_id=run_data["id"])
  print(
      f"Waiting for Workflow run {run_data['id']} to finish. Check on {workflow_web_url}"
  )
  start_time = time.time()
  last_request = start_time
  spinner = itertools.cycle(["|", "/", "-", "\\"])

  while True:
    now = time.time()
    elapsed_time = time.strftime("%H:%M:%S", time.gmtime(now - start_time))
    print(
        f"\r {next(spinner)} Waiting to finish. Elapsed time: {elapsed_time}. "
        f"Current state: status: `{status}`, conclusion: `{conclusion}`.",
        end="",
    )

    time.sleep(0.3)
    if (now - last_request) > 10:
      last_request = now
      run_data = requester(url)
      status = run_data["status"]
      conclusion = run_data["conclusion"]
      if status != "completed":
        continue
      elif conclusion == "success":
        print(
            f"\rFinished in: {elapsed_time}. "
            f"Last state: status: `{status}`, conclusion: `{conclusion}`.",
        )
        return run_data
      elif conclusion:
        print("\r")
        raise Exception(run_data)


def reset_directory():
  question = (
      f"Artifacts directory will be cleared. Is it OK for you?\n"
      f"Artifacts directory: {ARTIFACTS_DIRECTORY}\n"
      f"Your answer")
  if yes_or_no(question):
    print(f"Clearing directory: {ARTIFACTS_DIRECTORY}")
    shutil.rmtree(ARTIFACTS_DIRECTORY, ignore_errors=True)
    os.makedirs(ARTIFACTS_DIRECTORY)
  else:
    print("You said NO for clearing artifacts directory. Quitting ...")
    quit(1)


def download_artifacts(artifacts_url):
  print("Starting downloading artifacts ... (it may take a while)")
  data_artifacts = requester(artifacts_url)
  filtered_artifacts = [
      a for a in data_artifacts["artifacts"] if (
          a["name"].startswith("source_gztar_zip") or
          a["name"].startswith("wheelhouse"))
  ]
  for artifact in filtered_artifacts:
    url = artifact["archive_download_url"]
    name = artifact["name"]
    artifacts_size_mb = round(artifact["size_in_bytes"] / (1024 * 1024), 2)
    print(
        f"\tDownloading {name}.zip artifact (size: {artifacts_size_mb} megabytes)"
    )
    r = requester(url, return_raw_request=True, allow_redirects=True)

    with tempfile.NamedTemporaryFile(
        "wb",
        prefix=name,
        suffix=".zip",
    ) as f:
      f.write(r.content)

      with zipfile.ZipFile(f.name, "r") as zip_ref:
        print(f"\tUnzipping {len(zip_ref.filelist)} files")
        zip_ref.extractall(ARTIFACTS_DIRECTORY)


if __name__ == "__main__":
  print(
      "Starting script for download GitHub Actions artifacts for Build Wheels workflow"
  )
  parse_arguments()

  workflow_id = get_build_wheels_workflow_id()
  run = get_last_run(workflow_id)
  run = validate_run(run)
  artifacts_url = run["artifacts_url"]
  reset_directory()
  download_artifacts(artifacts_url)
  print("Script finished successfully!")
  print(f"Artifacts available in directory: {ARTIFACTS_DIRECTORY}")
