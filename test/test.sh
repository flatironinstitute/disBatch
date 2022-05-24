module add slurm disBatch/2.0-beta

#direct disbatch
sbatch -n 20 --ntasks-per-node 10 disBatch 4KTasksRep

#get the job id
#wait until the job is done


