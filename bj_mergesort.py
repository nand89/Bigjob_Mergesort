import os, time, sys
import bliss.saga as saga 
from pilot import PilotComputeService, ComputeDataService, State
import random

HOSTNAME    = "pbs+ssh://username@repex1.tacc.utexas.edu"

QUEUE       = "normal"

WORKDIR     = "/home/tutorial-21" 

### This is the number of jobs you want to run
NUM_JOBS = 2
array_size = 100
input_size = int(array_size/NUM_JOBS)

# create unsorted random list of numbers and store in 'unsorted.txt'
unsorted_list = random.sample(range(10*array_size), array_size)
unsortedfile = open('ms_input.txt', 'w')
unsorted_string = ','.join(map(str, unsorted_list))
unsortedfile.write(unsorted_string)
unsortedfile.close()

# set up redis server connection
COORDINATION_URL = "redis://username@gw68.quarry.iu.teragrid.org:6379"

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

if __name__ == "__main__":

    	pilot_compute_service = PilotComputeService(COORDINATION_URL)
    
    	# specify local directory to copy input and output text files back 
    	dirname = 'sftp://localhost/%s/mergesort_agent' % os.getcwd()
    	workdir = saga.filesystem.Directory(dirname, saga.filesystem.Create)

    	pilot_compute_description = { "service_url": HOSTNAME,
                                      "number_of_processes": 12,
                                      "working_directory": workdir.get_url().path,
                                      "walltime":10
                              	    }

    	# copy the executable and input file to the remote host
        msexe = saga.filesystem.File('sftp://localhost/%s/mergesort.py' % os.getcwd())
        msexe.copy(workdir.get_url())
	msinput = saga.filesystem.File('sftp://localhost/%s/ms_input.txt' % os.getcwd())
        msinput.copy(workdir.get_url())

 	pilot_compute_service.create_pilot(pilot_compute_description)

 	compute_data_service = ComputeDataService()
    	compute_data_service.add_pilot_compute_service(pilot_compute_service)

	print ("Finished Pilot-Job setup. Submitting compute units...")

	# submit compute units
        for x in range(0, NUM_JOBS):
		
		""" SINGLE MERGE-SORT JOB """
		
		# split the unsorted array into equal parts based on number of jobs 
		split_array = unsorted_list[(x*input_size):(input_size + (x*input_size))]
		
		# create an input file for each job to store the split array
		input_filename = 'unsorted%s.txt' % x
		inputfile = open(input_filename, 'w')
		input_string = ','.join(map(str, split_array))
		inputfile.write(input_string)
		inputfile.close()

		compute_unit_description = {	"executable": "python",
                        			"arguments": [workdir.get_url().path + 'mergesort.py', input_size, 
                        			NUM_JOBS, x],
                        			"number_of_processes": 1,    
                        			"working_directory":workdir.get_url().path,        
                        			"output": "stdout_%s.txt" % x,
                        			"error": "stderr_%s.txt" % x,
                        		    }   
		compute_data_service.submit_compute_unit(compute_unit_description)

	print ("Waiting for compute units to complete...")
    	compute_data_service.wait()
                
    	# Copy the outputs back to local file
        for textfile in workdir.list('sorted?.txt'):
            	print ' * Copying %s/%s to %s' % (workdir.get_url(), textfile, WORKDIR)
            	workdir.copy(textfile, 'sftp://localhost/%s/' % os.getcwd())

        # concatenate the strings together
        print ' * Performing a final MERGE of sorted outputs of all jobs to : ms_output.txt'
	final_array = []
        for x in range(0, NUM_JOBS):
        	temp_file = open('sorted%s.txt' % x, 'r')
		temp_array = temp_file.readline().split(',')
		temp_array = map(int, temp_array)		
		final_array = merge(final_array,temp_array)
	final_output = open('ms_output.txt','w')
	final_string = ','.join(map(str,final_array))
	final_output.write(final_string)	

    	print ("Terminate Pilot Jobs")
    	compute_data_service.cancel()    
    	pilot_compute_service.cancel()


