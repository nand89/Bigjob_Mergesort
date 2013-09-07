import sys

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

def make_output(NUM_JOBS):

	# concatenate the strings together
        print ' * Performing a final MERGE of sorted outputs of all jobs to : ms_output.txt'
	final_array = []
        for x in range(0, NUM_JOBS):
        	temp_file = open('sorted%s.txt' % x, 'r')
		temp_array = temp_file.readline().split(',')
		temp_array = map(int, temp_array)		
		final_array = merge(final_array,temp_array)
	print ("Sorted output: ")
	print final_array
	final_output = open('ms_output.txt','w')
	final_string = ','.join(map(str,final_array))
	final_output.write(final_string)

if __name__ == "__main__":
	
	args = sys.argv[1:]
	NUM_JOBS = int(sys.argv[1])

	make_output(NUM_JOBS)
	sys.exit(0)	
