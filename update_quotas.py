#
# This is free software. Enjoy it. Improve it. 
#
# bugs to jhcepas@gmail.com
# 

import sys
import os
import math

if sys.argv[-1] not in set(["create", "modify", "dryrun"]):
    print """
    Please, read the comments within the script if you have not done
    it yet.
    
    SELECT AND OPTION FROM BELLOW : 
    
     "create" = create queues and quotas (only needed for the first
                                          time or if TIMES keys or
                                          HGROUPS are changed in the script.)
    
     "modify" = only modify quotas (can be done on the fly)

     "dryrun" = prints the commands, but not execute

    """
    sys.exit()
SCRIPT_MODE = sys.argv[-1]

# Queues and Quotas template. Change whatever is necessary in these
# files, but maintain the keywords like "XXX!" untouched. 
TEMPLATE = open("queue.template").read()
QTEMPLATE = open("queue_quotas.template").read()

# Total number of slots in the cluster
MAXSLOTS = 440

# This defines the number of queues, their time restrictions (keys)
# and the proportion of slots they can use (values). Note that jobs
# with h_rt=12h can run in the 12h queue + all the higher times
# queues, so the number of slots available for a 12h jobs is the sum
# of all their suitable queues.
#
# - All values must sum up 1 (=100% slots).
#
# - Dictionary keys will be used to create the queues.
#
# In my case, jobs with unlimited time can use 50% of the available
# slots (0.5), jobs of 48h can use 62'5% of the slots (0.5+0.125), and
# so on. The granularity of time restrictions is up to you.
TIMES =  {3:     0.1, 
          6:     0.05,
          9:     0.05, 
          12:    0.05,
          24:    0.125,
          48:    0.125,
          99999: 0.50
          }


# In addition, the HGROUPS variable will be used to distribute the
# number of slots in each queue among several host groups. This is, if
# queue 12h can use up to 100 slots, such slots will never be
# allocated in the same type of host. By contrast, slots will be
# equally distributed among hostgroups.  Furthermore, the order of
# hostgroups in HGROUPS will be used to prioritize certain groups.
# This is important because different hosts provide different amount
# of resources, and I want big nodes to be used at last term for
# normal jobs to increase the chance of allocating big tasks (high
# memory or many slots) when necessary.
#
#           Hostgroup, cores,  mem,   how many nodes of this type
HGROUPS=[ ["@rgenes",   8,     "14",    20], 
          ["@bgenes",   8,     "96",    3], 
          ["@dell",    64,     "128",   4] 
          ]

# The next block of code autogenerates the commands to create the
# necessary queues and quotas according to TIMES and HGROUPS. You
# don't need to change anything.
seq = 0
limits = []
total_slots = 0
tm2slots = {}
for tm in sorted(TIMES):
    quota = TIMES[tm]
    tm_maxslots = 0
    for host, slots, mem, nnodes in HGROUPS:
        temp = TEMPLATE.replace("TIME!", str(tm))
        qname = "t%s_m%s_c%s.q" %(tm, mem, slots)
        temp = temp.replace("QNAME!", qname)
        temp = temp.replace("HOSTS!", host)
        temp = temp.replace("SLOTS!", str(slots))
        temp = temp.replace("SEQNUM!", str(seq))
        seq += 1
        print "CREATING", qname
        open("newqueue.tmp", "w").write(temp)

        if SCRIPT_MODE == "modify":
            os.system("qconf -Mq newqueue.tmp")
        elif SCRIPT_MODE == "create":
            os.system("qconf -dq %s" %qname)
            os.system("qconf -Aq newqueue.tmp")
        elif SCRIPT_MODE == "dryrun":
            print temp
            print "qconf -dq %s" %qname
            print "qconf -Aq newqueue.tmp"
            
        maxslots = int(round(quota*nnodes*slots))
        limits.append("limit\tqueues {%s}\tto slots=%s" % (qname, maxslots))
        tm_maxslots += maxslots 
    tm2slots[tm] = tm_maxslots


qtemp = QTEMPLATE.replace("LIMITS!", '\n'.join(limits))
open("newquotas.tmp", "w").write(qtemp)

print qtemp
for tm in sorted(TIMES):
    print tm, sum([tm2slots[t] for t in sorted(TIMES) if t>=tm])

while 1:
    try:
        if SCRIPT_MODE == "dryrun":
            print "qconf -drqs maxslots"
            print "qconf -Arqs newquotas.tmp"
            break
        elif raw_input("\nModify quotas as shown above? [yes,  Ctr-C to cancel]: ").strip() == "yes":
            if SCRIPT_MODE == "modify":
                os.system("qconf -Mrqs newquotas.tmp")
            elif SCRIPT_MODE == "create":
                os.system("qconf -drqs maxslots")
                os.system("qconf -Arqs newquotas.tmp")
            print "Done! Quotas modified"
            break
    except KeyboardInterrupt: 
        print "\nFine, quotas were NOT modified."
        break

