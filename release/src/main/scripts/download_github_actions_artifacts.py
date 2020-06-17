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

"""Script for downloading GitHub Actions artifacts."""

import os
import argparse
import requests
from pprint import pprint as pp


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Script for downloading GitHub Actions artifacts."
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


#
#
#
#
#
# # WORKFLOW ID
# url = f"https://api.github.com/repos/{REPO_URL}/actions/workflows/build_wheels.yml"
# r = requests.get(url, auth=("token", GITHUB_TOKEN))
# r.raise_for_status()
# workflow_id = r.json()["id"]
# print(workflow_id)
#
# # LIST OF WORKFLOW RUNS -> RUN_ID
# # https://developer.github.com/v3/actions/workflow-runs/#list-workflow-runs-for-a-repository
# url = f"https://api.github.com/repos/{REPO_URL}/actions/workflows/{workflow_id}/runs"
# r = requests.get(
#     url,
#     params={"event": "push", "actor": USER_GITHUB_ID, "branch": RELEASE_BRANCH},
#     auth=("token", GITHUB_TOKEN),
# )
# r.raise_for_status()
# # pp(r.json()["total_count"])
# # run_id = r.json()["workflow_runs"][0]["id"]
# # run_url = r.json()["workflow_runs"][0]["url"]
#
# # for debug
# RUN = None
# for run in r.json()["workflow_runs"]:
#     if run["status"] == "completed" and run["conclusion"] == "success":
#         print(f"HEAD SHA: {run['head_sha']}  created_at: {run['created_at']}  run_id: {run['id']}")
#         RUN = run
#         break
# run_id = RUN["id"]
# run_url = RUN["url"]
# print(run_url)
#
#
# # WORKFLOW RUN
# # https://developer.github.com/v3/actions/workflow-runs/#get-a-workflow-run
# url = run_url
# r = requests.get(url, auth=("token", GITHUB_TOKEN))
# r.raise_for_status()
# # pp(r.json())
# run_status = r.json()["status"]
# run_artifacts_url = r.json()["artifacts_url"]
#
#
# # WORKFLOW ARTIFACTS
# # https://developer.github.com/v3/actions/artifacts/#list-workflow-run-artifacts
# # DOWNLOAD ARTIFACTS
# # https://developer.github.com/v3/actions/artifacts/#download-an-artifact
#
# url = run_artifacts_url
# r = requests.get(url, auth=("token", GITHUB_TOKEN))
# r.raise_for_status()
# for artifact in r.json()["artifacts"]:
#     url = artifact["archive_download_url"]
#     name = artifact["name"]
#     print(f"Downloading {name}")
#     r = requests.get(url, auth=("token", GITHUB_TOKEN), allow_redirects=True)
#     r.raise_for_status()
#     with open(name + ".zip", "wb") as f:
#         f.write(r.content)

if __name__ == "__main__":
    parse_arguments()

    pp(globals())
