import os
TEMPLATE = open("queue.template").read()

# In priority order
# Hostgroup, cores, mem, numero de nodos
HGROUPS=[ ["@rgenes", 8, "14", 20], 
          ["@bgenes", 8, "96", 3], 
          ["@dell", 64, "128", 4] 
          ]

MAXSLOTS = 440
TIMES =  {3: 1.0, 
          6: 0.95,
          9: 0.90, 
          12: 0.80,
          24: 0.70,
          48: 0.60,
          99999: 0.50
          }

seq = 0
limits = []
for tm in sorted(TIMES):

    quota = TIMES[tm]
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
        os.system("qconf -dq %s" %qname)
        os.system("qconf -Aq newqueue.tmp")
        limits.append("limit\tqueues {%s}\tto slots=%s" % (qname, int(quota*nnodes*slots)))

QTEMPLATE = open("queue_quotas.template").read()
qtemp = QTEMPLATE.replace("LIMITS!", '\n'.join(limits))
open("newquotas.tmp", "w").write(qtemp)
os.system("qconf -drqs maxslots")
os.system("qconf -Arqs newquotas.tmp")

print qtemp


