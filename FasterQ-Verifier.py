import os
import sys
from subprocess import PIPE, run
import json
import re
import numpy as np
import glob
import shutil


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


#runs vbd-dump and gets size of fast-q file
#then checks if we've allocated enough memory
def check_mem(accession, default_mem):
    try:
        check = out("vdb-dump " + accession + " --info")
        np_arr = np.array(check[0].split())
        np_arr = np_arr[np_arr != ':']
        x = np.where(np_arr == 'size')[0]
        size = np_arr[x+1]
        size = int(size[0].replace(",", ""))
        if size > default_mem:
            return size*1.2
        else:
            return default_mem
    except IndexError:
        return default_mem

def write_job_file(job_file, job_name, srr, dir_str):
    mem = check_mem(srr, default_mem)
    with open(job_file, 'w+') as j:
        j.writelines('#!/bin/bash\n')
        j.writelines('memSize="40G"\n')
        j.writelines('OUTDIR="/tmp"\n')
        j.writelines('jobName="' + job_name + '"\n')
        j.writelines('jobID=$(sbatch --parsable --partition=queue0 --job-name=$jobName --output=${OUTDIR}/${jobName}.o%j --error=${OUTDIR}/${jobName}.e%j --mem=$memSize --priority=10 <<EOF \n')
        j.writelines('#!/bin/bash\n')
        j.writelines('cd ' + dir_str + '\n')
        j.writelines('find . -name "*.fastq" -exec pigz {} \; \n')
        j.writelines('EOF' + '\n')
        j.writelines(') \n')
        j.writelines('exit 0 \n')


def move_file(src, dest):
    is_dir = os.path.isdir(src)
    if is_dir:
        for data in glob.glob(src + '/*'):
            try:
                shutil.move(data, dest)
            except shutil.Error:
                no_fastq.append('Could not move files inside ' + dest)
        return True
    else:
        return False


def test_file(srr, ls_results, dir_str):
    if ((srr + "_1.fastq.gz" in ls_results) and (srr + "_2.fastq.gz") in ls_results) or (srr + ".fastq.gz") in ls_results:
        #if fastq files are gzip, in proper directory, and metadata present, case is correct
        if ((srr + "_info.json") in ls_results):
            good_arr.append("Succesfully downloaded: " + srr_srx[srr] + '\t' + srr)
            return True             
        else:
            #likely need to rerun ffq
            no_metadata.append("Missing metadata only: " + srr + '\t' + srr_srx[srr])
            return False
    elif ((srr + "_1.fastq" in ls_results) and (srr + "_2.fastq") in ls_results) or (srr + ".fastq") in ls_results:
        #if files are uncompressed, compress them
        no_fastq.append("Uncompressed. Compressing: " + srr + '\t' + srr_srx[srr])
        write_job_file(srr +'_c.sh', srr+'_c', srr, dir_str)
        comp_arr[dir_str] = srr+'_c.sh'
        return False
    else:
        un_move_dir = os.path.join(dir_str, srr)
        if os.path.isdir(un_move_dir):
            b = move_file(dir_str, un_move_dir)
        elif os.path.isdir(un_move_dir + '_m'):
            un_move_dir = un_move_dir + '_m'
            b_m = move_file(dir_str, un_move_dir)
        else:
            no_fastq.append('Cannot locate FastQ files for %s' %(' '.join([srr, srr_srx[srr]])))
            return False
        if b or b_m:
            rm_out = out('rmdir ' + un_move_dir)
            if(len(rm_out[1]) > 0):
                directory_err.append(' '.join(['Could not delete directory', un_move_dir]))
        return False


def ask_compression():
    if len(comp_arr.keys()) == 0:
        return
    i = input('Compress ' + str(len(comp_arr.keys())) + " total runs (y/n)?")
    if i == 'y':
        for item in comp_arr.values():
            out("sbatch %s" %item)
    else:
        i = input('Review files individually for compression? (y/n)?')
        if i == 'y':
            for dir_str, item in comp_arr.entries():
                print('Compress fastq files in ' + dir_str + '?')
                print('Contents of ' + dir_str + ':\n' + out('ls ' + dir_str)[0])
                i = input('(y/n)?: ')
                if i == 'y':
                    out("sbatch %s" %item)
                    out("rm %s" %job_file)

def print_stats():
    n_files = len(srr_srx.keys())
    return '\n'.join(['%s verified.' %n_files, 
                      '%s succesfully downloaded.' %len(good_arr), 
                      '%s have either FastQ files missing.' %len(no_fastq),
                      '%s have directory manipulation errors.' %len(directory_err),
                      '%s have metadata missing.' %len(no_metadata),
                      'Total correct percentage of %s%%\n' %round(len(good_arr)/n_files * 100, 2),
                      'Total correct percentage (excluding metadata errors) of %s%%\n' %round((1 - len(no_fastq)/n_files) * 100, 2)
                    ])


def write_output(out_file):
    with open(out_file, "w") as f:
        f.writelines(print_stats())
        f.write('\n')
        f.writelines('\n'.join(no_fastq))
        f.write('\n')
        f.writelines('\n'.join(directory_err))
        f.write('\n')
        f.writelines('\n'.join(no_metadata))
        f.write('\n')
        f.write('\nAll other files succesfully downloaded')


if __name__ == "__main__":
    good_arr = []
    fail_arr = []
    no_fastq = []
    no_metadata = []
    directory_err = []
    comp_arr = {}
    base_dir = 'experiment_files'
    wd = os.getcwd()
    srr_srx = {}
    path = sys.argv[1]
    file_name = '_'.join(sys.argv[2], 'verified.txt')

    #default prefetch memory is 20G
    default_mem = 2*10**10 

    with open(path, "r") as f:
        for line in f:
            li = line.split('\t')
            srr_srx[li[0].strip()] = li[1].strip()

    for item in srr_srx:
        dir_str = os.path.join(base_dir, srr_srx[item])
        #Test for default directory (./experiment_files/SRXxxx)
        is_dir = os.path.isdir(dir_str)
        if is_dir:
            arr = out('ls ' + dir_str)
            result = test_file(item, arr[0], dir_str)
        else:
            no_fastq.append(' '.join(["File cannot be located:", srr_srx[item], item]))

    ask_compression()
    write_output(file_name)
