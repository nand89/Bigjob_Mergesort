import random
import sys

def make_input(array_size):

	# create unsorted random list of numbers and store in 'unsorted.txt'
	unsorted_list = random.sample(range(10*array_size), array_size)
	print ("Unsorted input: ")
	print unsorted_list
	unsortedfile = open('ms_input.txt', 'w')
	unsorted_string = ','.join(map(str, unsorted_list))
	unsortedfile.write(unsorted_string)
	unsortedfile.close()

if __name__ == "__main__":

	
	args = sys.argv[1:]
	array_size = int(sys.argv[1])

	make_input(array_size)
	sys.exit(0)
