#!/bin/bash

#SBATCH --job-name=bam2cram
#SBATCH --nodes 8
#SBATCH --ntasks=8
#SBATCH --cpus-per-task 24
#SBATCH --time 3-0
#SBATCH --output=slurm.%j.out
#SBATCH --error=slurm.%j.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=your@email.de
#SBATCH --partition rosa.p


module load hpc-env/13.1
module load SAMtools
ml parallel/20230822-GCCcore-13.1.0
ml HTSlib
ml Python


script="bam2cram.sh"
cram_list="bamlist.txt"
output_dir="/your/folder/converted_files"

"$script" "$cram_list" "$output_dir"
