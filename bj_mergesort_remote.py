import os
import sys
import pilot
import traceback
import random
import saga

# Redis password and 'user' name a aquired from the environment
REDIS_PWD   = os.environ.get('XSEDE_TUTORIAL_REDIS_PASSWORD')
USER_NAME   = os.environ.get('XSEDE_TUTORIAL_USER_NAME')

# The coordination server
COORD       = "redis://localhost:6379" 
# The host (+username) to run BigJob on
HOSTNAME    = "username@india.futuregrid.org"
# The queue on the remote system
QUEUE       = "normal"
# The working directory on the remote cluster / machine
WORKDIR     = "/N/u/username/mergesort_agent" 
# The number of jobs you want to run
NUM_JOBS = 2
array_size = 100
input_size = int(array_size/NUM_JOBS)

########################################################################

# create unsorted random list of numbers and store in 'unsorted.txt'
unsorted_list = random.sample(range(10*array_size), array_size)
unsortedfile = open('ms_input.txt', 'w')
unsorted_string = ','.join(map(str, unsorted_list))
unsortedfile.write(unsorted_string)
unsortedfile.close()

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

#########################################################################

def main():
    try:

	# copy the executable and input file to the remote host
        msexe = saga.filesystem.File('sftp://localhost/%s/mergesort.py' % os.getcwd())
        msexe.copy('ssh://%s' % HOSTNAME)
	msinput = saga.filesystem.File('sftp://localhost/%s/ms_input.txt' % os.getcwd())
        msinput.copy('ssh://%s' % HOSTNAME)	

	# this describes the parameters and requirements for our pilot job
        pilot_description = pilot.PilotComputeDescription()
        pilot_description.service_url = "pbs+ssh://%s/%s" % (HOSTNAME, WORKDIR)
        pilot_description.number_of_processes = 12
        pilot_description.working_directory = WORKDIR
        pilot_description.walltime = 10

	# create a new pilot job
        pilot_compute_service = pilot.PilotComputeService(COORD)
        pilotjob = pilot_compute_service.create_pilot(pilot_description)

	# submit tasks to pilot job
        tasks = list()
        for x in range(NUM_JOBS):
	
	    """ SINGLE MERGE-SORT JOB """

	    split_filename = 'unsorted%s.txt' % x

	    #compute unit description
            task_desc = pilot.ComputeUnitDescription()
            task_desc.executable = 'python'
            task_desc.arguments = [WORKDIR + '/mergesort.py', input_size, 
                        			NUM_JOBS, x, split_filename]
            task_desc.number_of_processes = 1
	    task_desc.queue = QUEUE
            task_desc.output = 'stdout.txt'
            task_desc.error = 'stderr.txt'

            task = pilotjob.submit_compute_unit(task_desc)
            print "* Submitted task '%s' with id '%s' to %s" % (x, task.get_id(), HOSTNAME)
            tasks.append(task)

        print "Waiting for tasks to finish..."
        pilotjob.wait()

	# Copy the outputs back to local file
        for textfile in workdir.list('sorted*'):
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

        return(0)

    except Exception, ex:
            print "AN ERROR OCCURED: %s" % ((str(ex)))
            # print a stack trace in case of an exception -
            # this can be helpful for debugging the problem
            traceback.print_exc()
            return(-1)

    finally:
        # alway try to shut down pilots, otherwise jobs might end up
        # lingering in the queue
        print ("Terminating BigJob...")
        pilotjob.cancel()
        pilot_compute_service.cancel()


if __name__ == "__main__":
    sys.exit(main())
