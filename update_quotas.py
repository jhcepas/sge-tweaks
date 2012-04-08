import os
import math
TEMPLATE = open("queue.template").read()

# In priority order
# Hostgroup, cores, mem, numero de nodos
HGROUPS=[ ["@rgenes", 8, "14", 20], 
          ["@bgenes", 8, "96", 3], 
          ["@dell", 64, "128", 4] 
          ]

MAXSLOTS = 440
TIMES =  {3: 0.1, 
          6: 0.05,
          9: 0.05, 
          12: 0.05,
          24: 0.125,
          48: 0.125,
          99999: 0.50
          }

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
        #os.system("qconf -dq %s" %qname)
        #os.system("qconf -Mq newqueue.tmp")
        maxslots = int(round(quota*nnodes*slots))
        limits.append("limit\tqueues {%s}\tto slots=%s" % (qname, maxslots))
        tm_maxslots += maxslots 

    tm2slots[tm] = tm_maxslots

QTEMPLATE = open("queue_quotas.template").read()
qtemp = QTEMPLATE.replace("LIMITS!", '\n'.join(limits))
open("newquotas.tmp", "w").write(qtemp)

print qtemp

for tm in sorted(TIMES):
    print tm, sum([tm2slots[t] for t in sorted(TIMES) if t>=tm])

while 1:
    try:
        if raw_input("\nModify quotas as shown above? [yes,  Ctr-C to cancel]: ").strip() == "yes":
            os.system("qconf -Mrqs newquotas.tmp")
            print "Done! Quotas modified"
            break
    except KeyboardInterrupt: 
        print "\nFine, quotas were NOT modified."
        break

