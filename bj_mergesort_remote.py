import os, time, sys
import saga 
from pilot import PilotComputeService, ComputeDataService, State
import random
import time

# Redis password and 'user' name a aquired from the environment
REDIS_PWD   = os.environ.get('XSEDE_TUTORIAL_REDIS_PASSWORD')
USER_NAME   = os.environ.get('XSEDE_TUTORIAL_USER_NAME')
HOSTNAME    = "username@india.futuregrid.org"
QUEUE       = "development"
WORKDIR     = "/N/u/username"
COORDINATION_URL = "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379" 

### This is the number of jobs you want to run
NUM_JOBS = 2
array_size = 128
input_size = int(array_size/NUM_JOBS)

# define merge function for final merge
def merge(left,right):
    
    # sorted array to store result
    sorted_list = []
    i = 0
    j = 0
    
    # merge operation
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            sorted_list.append(left[i])
            i += 1
        else:
            sorted_list.append(right[j])
            j += 1
    
    sorted_list += left[i:]
    sorted_list += right[j:]

    return sorted_list

################################################################################

if __name__ == "__main__":

    	pilot_compute_service = PilotComputeService(COORDINATION_URL)
    	
    	# specify local directory to copy input and output text files back 
    	dirname = 'sftp://%s/%s' % (HOSTNAME, WORKDIR)
    	workdir = saga.filesystem.Directory(dirname, saga.filesystem.CREATE_PARENTS)

    	pilot_compute_description = { "service_url": "ssh://" + HOSTNAME + WORKDIR,
                                      "number_of_processes": 8,
                                      "working_directory": WORKDIR,
                                      "walltime":10
                              	    }

 	pilot_compute_service.create_pilot(pilot_compute_description)

	#create compute-unit to create input file
	compute_data_service = ComputeDataService()
    	compute_data_service.add_pilot_compute_service(pilot_compute_service)

	print ("Finished Pilot-Job setup.")
	print ("\nSubmitting first compute-unit to create the input file...")

	# submit compute unit to obtain unsorted input file
	compute_unit_description = {	"executable": "python",
                        		"arguments": [WORKDIR + '/input.py', array_size],
                        		"number_of_processes": 1,    
                        		"working_directory": WORKDIR,        
                        		"output": "stdout.txt",
                        		"error": "stderr.txt",
                        	    }   
	compute_data_service.submit_compute_unit(compute_unit_description)

	compute_data_service.wait()

	print ("Input file ready.")

	#start time for merge-sort
	qstart = time.time()

	#create compute unit to perform merge-sort
 	compute_data_service = ComputeDataService()
    	compute_data_service.add_pilot_compute_service(pilot_compute_service)

	print ("\nSubmitting second set of compute-units for merge-sorting...")

	# submit compute units merge-sorting
        for x in range(0, NUM_JOBS):
		
		""" SINGLE MERGE-SORT JOB """
		
		# create an input file for each job to store the split array
		split_filename = 'unsorted%s.txt' % x

		compute_unit_description = {	"executable": "python",
                        			"arguments": [WORKDIR + '/mergesort.py', input_size, 
                        			NUM_JOBS, x, split_filename],
                        			"number_of_processes": 1,    
                        			"working_directory": WORKDIR,        
                        			"output": "stdout_%s.txt" % x,
                        			"error": "stderr_%s.txt" % x,
                        		    }   
		compute_data_service.submit_compute_unit(compute_unit_description)


	print ("Waiting for merge-sort compute-units to complete...")

	#wait time start
	wstart = time.time()	
	
    	compute_data_service.wait()

	#end time for merge-sort
	qend = time.time()
        
	#create compute-unit to do final-merge
	compute_data_service = ComputeDataService()
    	compute_data_service.add_pilot_compute_service(pilot_compute_service)

	print ("\nSubmitting final compute-unit to create the output file...")

	# submit compute unit to obtain unsorted input file
	compute_unit_description = {	"executable": "python",
                        		"arguments": [WORKDIR + '/final_merge.py', NUM_JOBS],
                        		"number_of_processes": 1,    
                        		"working_directory": WORKDIR,        
                        		"output": "stdout.txt",
                        		"error": "stderr.txt",
                        	    }   
	
	compute_data_service.submit_compute_unit(compute_unit_description)

	compute_data_service.wait()  
    	
    	print ("\nTerminate Pilot Jobs")
    	compute_data_service.cancel()    
    	pilot_compute_service.cancel()

	print "Queuing time: ", round((qend-qstart)-(qend-wstart),2)
	
	#record the queuing time
	recordfile = open('time.txt', 'a')
	recordfile.write(str(NUM_JOBS)+"	")
	recordfile.write(str(array_size)+"	")
	recordfile.write(str(round((qend-qstart)-(qend-wstart),2))+"\n")
	recordfile.close()
