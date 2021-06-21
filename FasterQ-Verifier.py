import os
import sys
from subprocess import PIPE, run
import json
import re
import numpy as np

def out(command):
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return result.stdout, result.stderr

def check_move(source, target):
    print('Move ' + source + ' and overwrite ' + target + ' ? (y/n)')
    print('Contents of ' + target + ':\n' + out('ls ' + target)[0])
    i = input('(y/n)?: ')
    if i == 'y':
        out('rm -rf ' + target)
        out('mv ' + source + ' ' + target)
        return True
    else:
        command_arr.append('rm -rf ' + source)
        command_arr.append('mv ' + source + ' ' + target)
        return False

def write_job_file(job_file, job_name, srr, dir_str):
    mem = check_mem(srr, default_mem)
    with open(job_file, 'w+') as j:
        j.writelines('#!/bin/bash\n')
        j.writelines('memSize="40G"\n')
        j.writelines('OUTDIR="/tmp"\n')
        j.writelines('jobName="' + job_name + '"\n')
        j.writelines('jobID=$(sbatch --parsable --partition=queue0 --job-name=$jobName --output=${OUTDIR}/${jobName}.o%j --error=${OUTDIR}/${jobName}.e%j --mem=$memSize --priority=10 <<EOF \n')
        j.writelines('#!/bin/bash\n')
        j.writelines('cd ' + dir_str)
        j.writelines('find . -name "*.fastq" -exec pigz {} \;' + dir_str + '\n')
        j.writelines('EOF' + '\n')
        j.writelines(') \n')
        j.writelines('exit 0 \n')


def test_file(srr, ls_results, dir_str):
    if ((srr + "_1.fastq.gz" in ls_results) and (srr + "_2.fastq.gz") in ls_results) or (srr + ".fastq.gz") in ls_results:
        #if fastq files are gzip, in proper directory, and metadata present, case is correct
        if ((srr + "_info.json") in ls_results):
            good_arr.append("Succesfully downloaded: " + srr_srx[srr] + '\t' + srr)
            return True             
        else:
            #likely need to rerun ffq
            fail_arr.append("Missing metadata only: " + srr_srx[srr] + '\t' + srr)
            return False
    elif ((srr + "_1.fastq" in ls_results) and (srr + "_2.fastq") in ls_results) or (srr + ".fastq") in ls_results:
        #if files are uncompressed, compress them
        fail_arr.append("Uncompressed. Compressing: " + srr_srx[srr] + '\t' + srr)
        write_job_file(srr +'_c.txt', srr+'_c', srr, dir_str)
        print('Compress fastq files in ' + dir_str + '?')
        print('Contents of ' + dir_str + ':\n' + out('ls ' + dir_str)[0])
        i = input('(y/n)?: ')
        if i == 'y':
            out("sbatch %s" %(srr+'_c.txt'))
        return False
    else:
        is_dir = os.path.isdir(dir_str + '_m')
        if is_dir and len(out('mv ' + dir_str+'_m' + ' ' + dir_str)[1]) > 1:
            c_m = check_move(dir_str, dir_str+'_m')
            if not c_m:
                fail_arr.append("File requiring move: " + srr_srx[srr] + '\t' + srr)
            else:
                good_arr.append("Succesfully downloaded: " + srr_srx[srr] + '\t' + srr)
            return c_m

        else:
            is_dir = os.path.isdir(dir_str + '/' + srr +'_m')
            if is_dir and len(out('ls ' + dir_str + '/' + srr +'_m')[0]) > 0:
                print('Move all files in ' + dir_str + '/' + srr +'_m' + ' up a level? (y/n)')
                print('Contents of' + dir_str + '/' + srr +'_m:\n' + out('ls ' + dir_str + '/' + srr +'_m')[0])
                i = input('(y/n)?: ')
                if i == 'y':
                    print('move attempting')
                    arr = out('mv ' + dir_str + '/' + srr + '_m/* ' + dir_str)
                    print(arr[1])
                    arr = out('rmdir ' + dir_str + '/' + srr + '_m')
                    print(arr[1])
                    if len(arr[1]) == 0:
                        good_arr.append("Succesfully moved: " + srr_srx[srr] + '\t' + srr)
                    else:
                        fail_arr.append("File requiring move: " + srr_srx[srr] + '\t' + srr)
                    return True
                else:
                    command_arr.append('mv ' + dir_str + '/' + srr + '_m/* ' + dir_str + '/' + srr)
                    command_arr.append('rmdir ' + dir_str + '/' + srr + '_m')
                    return False

            fail_arr.append("Missing FastQ Files: " + srr_srx[srr] + '\t' + srr)
            return False

if __name__ == "__main__":
    good_arr = []
    fail_arr = []
    command_arr = []
    wd = os.getcwd()
    srr_srx = {}
    path = sys.argv[1]
    with open(path, "r") as f:
        for line in f:
            li = line.split('\t')
            srr_srx[li[0].strip()] = li[1].strip()

    for item in srr_srx:
        dir_str = srr_srx[item] + '/' + item
        #Test for default directory (./SRXxxx/SRRxxx)
        is_dir = os.path.isdir(dir_str)
        if is_dir:
            arr = out('ls ' + dir_str)
            result = test_file(item, arr[0], dir_str)
        else:
            dir_str = srr_srx[item] + '/' + item + '_m'
            is_dir = os.path.isdir(dir_str)
            if is_dir:
                arr = out('ls ' + dir_str)
                result = test_file(item, arr[0], dir_str)
                if result == True and len(out('mv ' + dir_str + ' ' + dir_str[0:-2])[1]) > 1:
                    check_move(dir_str, dir_str[0:-2])
            else:
                dir_str = item
                is_dir = os.path.isdir(dir_str)
                if not is_dir:
                    fail_arr.append("File cannot be located: " + srr_srx[item] + '\t' + item)
                    continue
                else:
                    arr = out('ls ' + dir_str)
                    result = test_file(srr, arr[0], dir_str)
                    if result == True and len(out('mv ' + dir_str + ' ' + srr_srx[item] + '/' + dir_str)[1]) > 1:
                        check_move(dir_str, (srr_srx[item] + '/' + dir_str))
    fail_arr.sort()
    with open("fast_q_verified.txt", "w") as f:
        for f_a in fail_arr:
            f.write(f_a + '\n')
        for g_a in good_arr:
            f.write(g_a + '\n')
        for c_a in command_arr:
            f.write(c_a + '\n')