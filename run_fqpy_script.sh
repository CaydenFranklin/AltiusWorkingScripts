#!/bin/bash
memSize="40G" # this varies widely with job needs; use ls -lah on your fastq files to estimate what you'll need
OUTDIR="/net/seq/data/projects/SuperIndex/cfranklin/slurm_output" # fine to use your favorite current directory in this case
jobName="cfranklinFastQDownload" # customize as desired
jobID=$(sbatch --parsable --partition=queue0 --job-name=$jobName --output=${OUTDIR}/${jobName}.o%j --error=${OUTDIR}/${jobName}.e%j --mem=$memSize --priority=10 <<EOF                         
#!/bin/bash
module load python/3.6.4
module load sratoolkit/2.9.1
module load atlas-lapack/3.10.2
module load numpy/1.11.0
module load scipy/1.0.0   
module load pandas/0.19.1
module load pigz/2.3.3
python FasterQ-Downloader.py $1

EOF
      )
exit 0