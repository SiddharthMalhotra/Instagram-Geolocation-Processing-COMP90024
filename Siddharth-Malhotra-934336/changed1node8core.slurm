#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=0-1:00:00
#SBATCH --partition=physical

module load Python/3.5.2-goolf-2015a
mpiexec python3 Assignment1.py
