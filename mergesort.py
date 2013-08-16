#!/usr/bin/env python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

""" A simple merge sort function which is an example of Divide and Conquer 
    algorithm. Here we specify the merge-sort operation as a single big-job 
    operation. A random array with desired number of elements is generated 
    and split into parts and merge-sort is performed on each of them by 
    individual jobs. The result is again merged to obtain the
    final sorted output array.

    The parameters are as follows:
    input_size: the size of the array to be sorted
    
"""
__author__    = "Nandhini Venkatesan"

################################################################################
##

import sys

WORKDIR = '/home/nv121'

""" MAIN SORTING FUNCTION CALL """
def sort(input_size, num_jobs, job):

    # read the input file
    input_filename = WORKDIR + '/unsorted%s.txt' % job
    input_file = open(str(input_filename), 'r')
    input_list = input_file.readline().split(',')
    input_list = map(int, input_list)
    input_file.close()
    
    # mergesort operation
    #print input_list
    result = mergesort(input_list)
    #print result

    # write to the output file after checking if it is the last merge of a single big-job
    output_filename = 'sorted%s.txt' % job
    outputfile = open(str(output_filename), 'w')
    output_string = ','.join(map(str, result))
    outputfile.write(output_string)
    outputfile.close()

    return outputfile
    
""" MERGE FUNCTION """
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


""" MERGE_SORT FUNCTION """
def mergesort(unsorted_list):
    
    if len(unsorted_list) <= 1:
        return unsorted_list
    
    middle = int(len(unsorted_list)/2)
    
    # recursive mergesort
    left = mergesort(unsorted_list[:middle])
    right = mergesort(unsorted_list[middle:])

    return merge(left,right)
    

################################################################################
##

if __name__ == "__main__":

    args = sys.argv[1:]
    if len(args) < 3:
        print "Usage: python %s input_size num_jobs job" % __file__
        sys.exit(-1)
    
    input_size = int(sys.argv[1])
    num_jobs = int(sys.argv[2])
    job = int(sys.argv[3])

    if len(args) == 1:
        input_size = int(sys.argv[1])

    if len(args) == 2:
        num_jobs = int(sys.argv[2])

    if len(args) == 3:
        job = str(sys.argv[3])
        

    sort(input_size, num_jobs, job)

    sys.exit(0)
    
