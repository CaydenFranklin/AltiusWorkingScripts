#!/bin/bash
memSize="1G" # this varies widely with job needs; use ls -lah on your fastq files to estimate what you'll need
OUTDIR="/net/seq/data/projects/SuperIndex/cfranklin" # fine to use your favorite current directory in this case
jobName="cfranklinFastQDownload" # customize as desired
jobID=$(sbatch --parsable --partition=queue0 --job-name=$jobName --output=${OUTDIR}/${jobName}.o%j --error=${OUTDIR}/${jobName}.e%j --mem=$memSize --priority=10 <<EOF                         
#!/bin/bash
module load python/3.6.4
python testfile.py

EOF
      )
exit 0