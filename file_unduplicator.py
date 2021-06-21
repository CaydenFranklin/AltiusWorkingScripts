from subprocess import PIPE, run
import os
line_arr = []


def out(command):
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return result.stdout, result.stderr


with open("folder_duplicates.txt", "r") as f:
    for line in f:
        line_arr.append(line)

wd = os.getcwd()
for item in line_arr:
    item = item.strip()
    l = item.split('/')
    chd = '/'.join(l[0:3])
    try:
        os.chdir(chd)
        print(out('ls')[0])
        m = "mv " + l[-1] + '/* .'
        m_o,m_e = out(m)
        r_o,r_e = out("rmdir " + l[-1])
        print(chd)
        print(m)
        if len(m_e) > 0:
            print("out: " + m_o + '\n' + "err: " + m_e + '\n')
        if len(r_e) > 0:
            print("out: " + r_o + '\n' + "err: " + r_e + '\n')
    except OSError as e:
        print(e)
    os.chdir(wd)
