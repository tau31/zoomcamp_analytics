"""
From here: https://github.com/Nordgaren/Github-Folder-Downloader/blob/master/gitdl.py#L8
"""
from github import Github
import requests
import os
os.makedirs("data", exist_ok=True)

gh = Github()

repo = "DataTalksClub/zoomcamp-analytics"
path_to_files = "data/de-zoomcamp-2023"
out_dir = "data"
gh_repo = gh.get_repo(repo)
files = gh_repo.get_contents(path_to_files)

for c in files:
    url = c.download_url
    file_name = c.raw_data["name"]
    file_path = f"{out_dir}/{file_name}"
    r = requests.get(url)
    with open(file_path, "wb") as f:
        print(f"downloading file {file_path}")
        f.write(r.content)
