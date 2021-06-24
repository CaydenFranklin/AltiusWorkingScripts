from subprocess import PIPE, run
import sys
import os
import shutil
import glob
line_arr = []


def out(command):
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return result.stdout, result.stderr


read_file = sys.argv[1]

with open(read_file, "r") as f:
    for line in f:
        line_arr.append(line)

wd = os.getcwd()
out_arr = []

for item in line_arr:
    item = item.strip()
    l = item.split()
    chd = os.path.join('./experiment_files', l[1])
    glob_str = os.path.join(chd, l[0], '*')
    for data in glob.glob(glob_str):
        try:
            shutil.move(data, os.path.join(chd))
        except shutil.Error:
            out_arr.append('Could not move files inside ' + chd)
    rm_out = out('rmdir ' + os.path.join(chd, l[0]))
    if(len(rm_out[1]) > 0):
        out_arr.append('Could not delete directory ' + l[1])

with open("file_flattener_out.txt", 'w') as file:
    file.writelines("\n".join(out_arr))