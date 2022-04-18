
from multiprocessing import Process, Array, Value
import psutil
import numpy as np

def merge(left, right):
    # If the first array is empty, then nothing needs
    # to be merged, and you can return the second array as the result
    if len(left) == 0:
        return right

    # If the second array is empty, then nothing needs
    # to be merged, and you can return the first array as the result
    if len(right) == 0:
        return left

    result = []
    index_left = index_right = 0

    # Now go through both arrays until all the elements
    # make it into the resultant array
    while len(result) < len(left) + len(right):
        # The elements need to be sorted to add them to the
        # resultant array, so you need to decide whether to get
        # the next element from the first or the second array
        if left[index_left] <= right[index_right]:
            result.append(left[index_left])
            index_left += 1
        else:
            result.append(right[index_right])
            index_right += 1

        # If you reach the end of either array, then you can
        # add the remaining elements from the other array to
        # the result and break the loop
        if index_right == len(right):
            result += left[index_left:]
            break

        if index_left == len(left):
            result += right[index_right:]
            break

    return result


def merge_sort(array):
    # If the input array contains fewer than two elements,
    # then return it as the result of the function
    if len(array) < 2:
        return array

    midpoint = len(array) // 2

    # Sort the array by recursively splitting the input
    # into two equal halves, sorting each half and merging them
    # together into the final result
    return merge(
        left=merge_sort(array[:midpoint]),
        right=merge_sort(array[midpoint:]))

def exit_sort(passes, sub_array_count):
  # Condition that all threads check for
  # Exit is when no proc is in forbidden state
  if passes < arr_size:
    return False
  return True

def is_forbidden_sort(touched, proc_id):
  # A proc is in forbidden state when it is sorting or here
  # When it is waiting for a neighbor proc to finish up
  if touched.value == proc_id:
    return False
  return True

def init(shared_left_, shared_middle_, shared_right_):
    global shared_left
    shared_left = shared_left_ # must be inherited, not passed as an argument

    global shared_middle
    shared_middle = shared_middle_ # must be inherited, not passed as an argument

    global shared_right
    shared_right = shared_right_ # must be inherited, not passed as an argument

def tonumpyarray(mp_arr):
  return np.frombuffer(mp_arr.get_obj(), dtype="int32")

def parse_arr_op(dir):
  if (dir == "right"):
    # We wish for the shared_right arr
    """synchronized shared_right"""
    with shared_middle.get_lock() and shared_right.get_lock():
      arr_op(shared_middle, shared_right)

  if (dir == "left"):
    # We wish for the shared_left arr
    """synchronized shared_left"""
    with shared_middle.get_lock() and shared_left.get_lock():
      arr_op(shared_left, shared_middle)

def is_sorted(x, key = lambda x: x):
  return all([key(x[i]) <= key(x[i + 1]) for i in range(len(x) - 1)])

def arr_op(arr_left, arr_right):
  """pre synchronized op"""
  np_arr_left = arr_left[:]
  np_arr_right = arr_right[:]
  #print("np_arr_left pre : ", np_arr_left)
  #print("np_arr_right pre : ", np_arr_left)
  if is_sorted(np_arr_left + np_arr_right):
    return
  np_arr_left = merge_sort(np_arr_left)
  np_arr_right = merge_sort(np_arr_right)
  len_left = len(np_arr_left)
  #print("np_arr_left post : ", np_arr_left)
  #print("np_arr_right post : ", np_arr_left)
  array = merge(np_arr_left, np_arr_right)
  arr_left[:] = array[:len_left]
  arr_right[:] = array[len_left:]
  return

def odd_even_sort(shared_left, shared_middle, shared_right, touched_arr, proc_id, sub_array_count):
  #print("Proc : ", proc_id, " len touched arr : ", len(touched_arr))
  # An array is partitioned such that each thread gets a left and right shared arr
  # as well as their never shared middle arr
  # Neighbor one left and one right
  init(shared_left, shared_middle, shared_right)
  passes = 0
  while not exit_sort(passes, sub_array_count):
    # We can't check our exit clause until we complete a cycle
    # Left
    touched_index = proc_id * 2
    #print("Proc : ", proc_id, " checking index ", touched_index)
    if touched_index == 0:
      parse_arr_op(dir = "left")
    else:
      this_touched = touched_arr[touched_index]
      while not is_forbidden_sort(this_touched, proc_id):
        continue
      parse_arr_op(dir = "left")
      this_touched.value = proc_id
    # Right
    # Need to ignore the far right and far left
    touched_index = (proc_id * 2) + 2
    #print("Proc : ", proc_id, " checking index ", touched_index)
    if touched_index >= (len(touched_arr) - 1):
      parse_arr_op(dir = "right")
    else:
      this_touched = touched_arr[touched_index]
      while not is_forbidden_sort(this_touched, proc_id):
        continue
      parse_arr_op(dir = "right")
      this_touched.value = proc_id
    passes += 1
    #print("Proc : ", proc_id, " completed pass ", passes)
  #print("Proc : ", proc_id, " exiting")


def spawner(array, num_processes):
  arr_size = len(array)
  # There are n+1 sub arrs that then split down the middle for locking purposes
  # The sub arrays must be divisable by 2 so
  # Leave the leftover beginning for bubble sort cycles after
  sub_array_count = 3 + ((num_processes - 1) * 2)
  sub_arrays_len = int(arr_size / sub_array_count)
  #sub_arrays_len = sub_arrays_len - (sub_arrays_len % 2)
  skip_partition = arr_size - (sub_arrays_len * sub_array_count)

  #print("num_processes : ", num_processes)
  #print("sub_array_count : ", sub_array_count)
  #print("sub_arrays_len : ", sub_arrays_len)
  #print("skip_partition : ", skip_partition)

  # Each split of the sub array needs it's own lock
  mp_sub_arrays = []
  mp_touched_arr = []
  index =  0
  for sub_array_num in range(sub_array_count):
    # shared, can be used from multiple processes
    extra = 0
    if int(skip_partition > 0):
      extra = 1
      skip_partition -= 1
    mp_sub_array = Array('i', array[index:(index + sub_arrays_len + extra)])
    mp_sub_arrays.append(mp_sub_array)
    # Make a flag for touched
    v = Value('i', -1)
    mp_touched_arr.append(v)
    index += sub_arrays_len + extra

  # Spawn in the procs
  sort_procs = []
  for proc_num in range(num_processes):
    index = proc_num * 2
    this_sort_proc = Process(target = odd_even_sort, args=(mp_sub_arrays[index], mp_sub_arrays[index + 1], mp_sub_arrays[index + 2], mp_touched_arr, proc_num, sub_array_count,))
    sort_procs.append(this_sort_proc)
  
  # Start proc list
  for this_sort_proc in sort_procs:
    this_sort_proc.start()
  
  # Join proc list
  for this_sort_proc in sort_procs:
    this_sort_proc.join()

  array_temp = []
  for arr in mp_sub_arrays:
    np_arr = np.frombuffer(arr.get_obj(), dtype=np.int32)
    array_temp.extend(np_arr)
  #print(array_temp)
  array[:] = array_temp

  #array = itertools.chain.from_iterable(mp_sub_arrays)

def llp_sort(array, num_processes = 4, ignore_cpu_count = False):
  if ignore_cpu_count == False:
    num_processes = min(psutil.cpu_count(logical=False), num_processes)
  if num_processes < 2:
    return_arr = merge_sort(array)
    return return_arr
  array_cp = array.copy()
  spawner(array_cp, num_processes = num_processes)
  return array_cp