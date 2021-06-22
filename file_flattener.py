from subprocess import PIPE, run
import os
import shutil
from pathlib import Path
line_arr = []


def out(command):
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return result.stdout, result.stderr


def find(name, path):
    for root, dirs, files in os.walk(path):
        if name in dirs:
            return os.path.join(root, name)

with open("file_to_locate.txt", "r") as f:
    for line in f:
        line_arr.append(line)

wd = os.getcwd()

for item in line_arr:
    item = item.strip()
    l = item.split()
    chd = os.path.join('.', l[0])
    path_to_missing = find(l[-1], chd)
    
    try:
        shutil.move(path_to_missing, chd)
    except shutil.Error:
        pass

    merged_dir = find(l[0]+'_m', chd)
    if len(os.listdir(merged_dir)) == 0:
        out('rmdir ' + merged_dir)