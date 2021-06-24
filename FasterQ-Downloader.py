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

#item is a dictionary containing at most 10 SRR values, of format
#{SRXxxx: [SRRxxx, SRRxxx....], SRXxxx: [SRRxxx, SRRxxx....]}
def write_job_file(job_file, job_name, item, default_mem, base_dir, nodelist):
    mem = check_mem(item, default_mem)
    with open(job_file, 'w+') as j:
        j.write('#!/bin/bash\n')
        j.write('#SBATCH --mem=' + mem +'\n')
        j.write('#SBATCH --job-name=%s\n' %job_name)
        j.write('#SBATCH --output=' + os.path.join(base_dir, 'slurm_output', job_name) + '.o%j\n')
        j.write('#SBATCH --error=' + os.path.join(base_dir, 'slurm_output', job_name) + '.e%j\n')
        j.write('#SBATCH --nodelist hpcA13\n')
        j.write('#SBATCH --partition=queue0\n')
        j.write('#SBATCH --priority=10\n')
        j.write('#SBATCH --nodes=1\n')
        for key in item.keys():
            j.write('cd /tmp\n')
            j.write('mkdir -p ' + key + '\n')
            j.write('SRRs=(' + ' '.join(item[key]) + ')\n')
            j.write('cd ' + key + '\n')
            j.write('vdb-config --prefetch-to-cwd\n')
            j.write('for i in ${SRRs[@]}' + '\n')
            j.write('do' + '\n')
            j.write('   ffq -o $i_info.json $i' + '\n')
            j.write('   prefetch $i --max-size ' + mem + '\n')
            j.write('   fasterq-dump $i' + '\n')
            j.write('   find . -name "*.fastq" -exec pigz {} \;' + '\n')
            j.write('done' + '\n')
            j.write('cd ..' + '\n')
            j.write('if [ ! -d ' + os.path.join(base_dir, 'experiment_files', key) + ' ] && [ ! -L ' + os.path.join(base_dir, 'experiment_files',  key) + ' ]\n')
            j.write('then\n')
            j.write('    mv ' + key + ' ' + os.path.join(base_dir, 'experiment_files') + '\n')
            j.write('else \n')
            j.write('    mv ' + key + "/* " + os.path.join(base_dir, 'experiment_files', key) + '\n')
            j.write('fi \n')
        j.write('exit 0 \n')



#runs vbd-dump and gets size of fast-q file
#then checks if we've allocated enough memory
def check_mem(item, default_mem):
    min_mem = 0
    for value in item.values():
        for srr in value:
            try:
                check = out("vdb-dump " + srr + " --info")
                np_arr = np.array(check[0].split())
                np_arr = np_arr[np_arr != ':']
                x = np.where(np_arr == 'size')[0]
                size = np_arr[x+1]
                size = int(size[0].replace(",", ""))
                if size > default_mem:
                    mem = size
                else:
                    mem = default_mem
            except IndexError:
                mem = default_mem
            if mem > min_mem:
                min_mem = mem
    return str(min_mem // (1*10**9)) + 'G'


def split_dict(lines):
    job_dict = {}
    for line in lines:
        arr = line.split()
        srr = arr[0]
        srx = arr[1]
        if srx in job_dict:
            job_dict[srx].append(srr)
        else:
            job_dict[srx] = [srr]
    return job_dict

def key_value_gen(data):
    for key, values in data.items():
        for value in values:
            yield key, value
        


if __name__ == "__main__":
    #get nodelist
    nodelist = sys.argv[1]
    dnase_path = sys.argv[2]
    
    #default prefetch memory is 20G
    default_mem = 2*10**10 
    
    #read file with SRR and SRX accessions, tab delimited 
    with open(dnase_path, 'r') as file1:
        lines = file1.readlines()

    d = split_dict(lines)

    normed_exp = []
    size = 10

    #normalizes size so we have an array of dicts with 10 SRRs each
    for index, (key, value) in enumerate(key_value_gen(d)):
        if index % size == 0:
            normed_exp.append({})
        normed_exp[-1].setdefault(key, []).append(value)

    #cache present working directory
    wd = os.getcwd()

    index = 1
    for item in normed_exp:
        #create string for new directory address
        base_dir = "/net/seq/data/projects/SuperIndex/cfranklin"
        job_file = '_'.join([str(index), 'cfranklin', "fq_dump.sh"])
        job_name = '_'.join([str(index), str(len(normed_exp)), 'cfranklin_download'])
        write_job_file(job_file, job_name, item, default_mem, base_dir, nodelist)
        os.chmod(job_file, 0o755)
        o = out("sbatch %s" %job_file)
        print(' '.join(o))
        index+=1
