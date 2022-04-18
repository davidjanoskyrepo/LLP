from multiprocessing import Process, Queue, Manager, cpu_count
from copy import copy
from collections import Counter
import psutil

def GSA(man_df, women_df):
  men_list = list(man_df.columns)
  women_list = list(women_df.columns)
  # dict to control which women each man can make proposals
  women_available = {man:women_list for man in men_list}
  # waiting list of men that were able to create pair on each iteration
  waiting_list = []
  # dict to store created pairs
  proposals = {}
  # while not all men have pairs
  while len(waiting_list)<len(men_list):
    # man makes proposals
    for man in men_list:
      if man not in waiting_list:
        # each man make proposal to the top women from it's list
        women = women_available[man]
        best_choice = man_df[man][man_df[man].index.isin(women)].idxmin()
        proposals[(man, best_choice)]=(man_df[man][best_choice],
                                            women_df[best_choice][man])
    # if women have more than one proposals 
    # she will choose the best option
    overlays = Counter([key[1] for key in proposals.keys()])
    # cycle to choose the best options
    for women in overlays.keys():
      if overlays[women]>1:
        # pairs to drop from proposals
        pairs_to_drop = sorted({pair: proposals[pair] for pair in proposals.keys() 
                if women in pair}.items(), 
              key=lambda x: x[1][1]
              )[1:]
        # if man was rejected by woman
        # there is no pint for him to make proposal 
        # second time to the same woman
        for p_to_drop in pairs_to_drop:
          del proposals[p_to_drop[0]]
          _women = copy(women_available[p_to_drop[0][0]])
          _women.remove(p_to_drop[0][1])
          women_available[p_to_drop[0][0]] = _women
    # man who successfully created pairs must be added to the waiting list 
    waiting_list = [man[0] for man in proposals.keys()]
  gold_standard = {key[1]:key[0] for key in proposals.keys()}
  return gold_standard

# Define our exit condition
def exit_man(tenative_acceptance_dict):
  # A man will exit when the accepted proposal list encompasses all men + women
  if "" in tenative_acceptance_dict.values():
    #print("not ready to exit man")
    return False
  return True

# Define our condition for additional computation loops
def is_forbidden_man(man, man_rejected):
  # A forbidden state for man is 1.) no outgoing proposals, 2.) no currently accepted proposals
  # aka need to make next proposal
  if man_rejected[man] == True:
    return True
  return False

# The algorithm for work done by this proc group
def gsa_algo_man(men, man_df, women_proposal_queues, man_rejected, tenative_acceptance_dict):
  men_rankings = [man_df.loc[:, man].to_dict() for man in men]
  while not exit_man(tenative_acceptance_dict):
    for index, man in enumerate(men):
      man_rankings = men_rankings[index]
      if is_forbidden_man(man, man_rejected):
        man_rejected[man] = False
        #print("Man {} working ...".format(man))
        # Find this man's next highest rated woman
        best_choice = min(man_rankings, key=man_rankings.get)
        # Put an outgoing proposal
        women_proposal_queues[best_choice].put(man)
        #print("Man {} proposed to Woman {}".format(man, best_choice))
        # Remove this woman from available
        del men_rankings[index][best_choice]
        #print(men_rankings)

# Define our exit condition
def exit_woman(tenative_acceptance_dict):
  # A woman will exit when the accepted proposal list encompasses all men + women
  if "" in tenative_acceptance_dict.values():
    #print(tenative_acceptance_dict)
    #print("not ready to exit woman")
    return False
  return True

# Define our condition for additional computation loops
def is_forbidden_woman(this_proposal_queue):
  # A forbidden state for woman is 1.) has incoming proposal
  if not this_proposal_queue.empty():
    # If we have incoming prop, check on it
    return True
  return False

# The algorithm for work done by this proc group
def gsa_algo_woman(women, women_df, women_proposal_queues, man_rejected, tenative_acceptance_dict):
  women_rankings = [women_df.loc[:, woman].to_dict() for woman in women]
  while not exit_woman(tenative_acceptance_dict):
    for index, woman in enumerate(women):
      woman_rankings = women_rankings[index]
      this_proposal_queue = women_proposal_queues[woman]
      if is_forbidden_woman(this_proposal_queue):
        #print("Woman {} working ...".format(woman))
        # Need to process at least one proposal
        # If this man would be higher ranked in woman eye
        # then swap matching
        current_man = tenative_acceptance_dict[woman]
        man = this_proposal_queue.get()
        #print("Woman {} recieved proposal from Man {}".format(woman, man))
        if current_man == "":
          # Need swap
          tenative_acceptance_dict[woman] = man
          #print("Woman {} accepted Man {} by default".format(woman, man))
        elif woman_rankings[current_man] > woman_rankings[man]:
          # Need swap
          tenative_acceptance_dict[woman] = man
          man_rejected[current_man] = True
          #print("Woman {} swapped to Man {} over Man {}".format(woman, man, current_man))
        else:
          man_rejected[man] = True
          #print("Woman {} rejected Man {} in favor of Man {}".format(woman, man, current_man))

def spawner(man_df, women_df, num_processes):
  men_list = list(man_df.columns)
  women_list = list(women_df.columns)
  #print("Number of cores : {}".format(cpu_count()))

  # Init acceptances as empty
  manager = Manager()
  women_proposal_queues = {woman:manager.Queue(maxsize = len(men_list)) for woman in women_list}
  tenative_acceptance_dict = manager.dict({woman:"" for woman in women_list})
  man_rejected = manager.dict({man:True for man in men_list})

  num_processes_men = int(num_processes / 2)

  bin_size = int(len(men_list)/num_processes_men)
  leftovers = int(len(men_list) - (bin_size * num_processes_men))
  skip = 0

  # Spawn a man thread for each man. They need to have a shared memory for their proposals with each woman
  man_procs = []
  for process_num in range(num_processes_men):
    extra = 0
    if leftovers > 0:
      extra = 1
      leftovers -= 1
    men = men_list[(process_num * bin_size) + skip:((process_num * bin_size) + bin_size + extra) + skip]
    proc = Process(target = gsa_algo_man, args=(men, man_df, women_proposal_queues, man_rejected, tenative_acceptance_dict,))
    man_procs.append(proc)
    skip += extra

  num_processes_women = int(num_processes / 2)

  bin_size = int(len(women_list)/num_processes_women)
  leftovers = int(len(women_list) - (bin_size * num_processes_women))
  skip = 0

  # Spawn a woman thread for each woman. They need to have a shared memory for their proposals with each woman
  women_procs = []
  for process_num in range(num_processes_women):
    extra = 0
    if leftovers > 0:
      extra = 1
      leftovers -= 1
    women = women_list[(process_num * bin_size) + skip:((process_num * bin_size) + bin_size + extra) + skip]
    proc = Process(target = gsa_algo_woman, args=(women, women_df, women_proposal_queues, man_rejected, tenative_acceptance_dict,))
    women_procs.append(proc)
    skip += extra
  
  # Start the proposal processes
  for proc in man_procs + women_procs:
    proc.start()
  
  # Wait for exit condition to be met
  for proc in man_procs + women_procs:
    proc.join()
  
  # Print out our final matches
  #print(tenative_acceptance_dict)
  # Golden is {'U': 'm', 'H': 'b', 'O': 'a', 'B': 'i'}
  shared_items = {k: tenative_acceptance_dict[k] for k in tenative_acceptance_dict}
  return shared_items
  #print(len(shared_items) == len(gold_standard))

def llp_GSA(man_df, women_df, num_processes = 4, ignore_cpu_count = False):
  if ignore_cpu_count == False:
    num_processes = min(psutil.cpu_count(logical = False), num_processes)
  num_processes = num_processes - (num_processes % 2)
  if num_processes < 2:
    return GSA(man_df, women_df)
  return spawner(man_df, women_df, num_processes = num_processes)
