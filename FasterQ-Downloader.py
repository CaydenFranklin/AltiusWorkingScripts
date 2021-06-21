import os
import sys
from subprocess import PIPE, run
import json
import re
import numpy as np


#runs bash commands and returns stdout and stderr as a tuple 
def out(command):
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return result.stdout, result.stderr

def write_job_file(job_file, job_name, srr, dir_str):
    mem = check_mem(srr, default_mem)
    with open(job_file, 'w+') as j:
        j.writelines('#!/bin/bash\n')
        j.writelines('memSize="40G"\n')
        j.writelines('OUTDIR="/tmp"\n')
        j.writelines('jobName="' + job_name + '"\n')
        j.writelines('jobID=$(sbatch --parsable --partition=queue0 --job-name=$jobName --output=${OUTDIR}/${jobName}.o%j --error=${OUTDIR}/${jobName}.e%j --mem=$memSize --priority=10 <<EOF \n')
        j.writelines('#!/bin/bash\n')
        j.writelines('mkdir -p ' + dir_str + '\n')
        j.writelines('mkdir ' + srr + '\n')
        j.writelines('cd ' + srr + '\n')
        j.writelines('vdb-config --prefetch-to-cwd' + '\n')
        j.writelines('ffq -o ' + srr + "_info.json " + srr + '\n')
        j.writelines('prefetch ' + srr + ' --max-size ' + str(mem) + '\n')
        j.writelines('fasterq-dump ' + srr + '\n')
        j.writelines('find . -name "*.fastq" -exec pigz {} \;' + '\n')
        j.writelines('cd ..' + '\n')
        j.writelines('if [mv ' + srr + " " + dir_str + ']; then\n')
        j.writelines('    :\n')
        j.writelines('else \n')
        j.writelines('    mv ' + srr + " " + dir_str + '/' + srr + '_m' + '\n')
        j.writelines('fi \n')
        j.writelines('EOF' + '\n')
        j.writelines(') \n')
        j.writelines('exit 0 \n')



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


if __name__ == "__main__":
    
    #default prefetch memory is 20G
    default_mem = 2*10**10 
    
    #read file with SRR and SRX accessions, tab delimited 
    dnase_path = sys.argv[1]
    file1 = open(dnase_path, 'r')
    lines = file1.readlines()

    #cache present working directory
    wd = os.getcwd()
    for line in lines:
        # reset to top level directory
        os.chdir(wd)
        
        #split file line into SRX and SRR accessions
        arr = line.split()
        srr = arr[0]
        srx = arr[1]
        
        #create string for new directory address
        dir_str = "/net/seq/data/projects/SuperIndex/cfranklin/" + srx
        job_file = srr + "_fq_dump.sh"
        job_name = srr +'_cfranklin_download'
        write_job_file(job_file, job_name, srr, dir_str)
        out("sbatch %s" %job_file)
