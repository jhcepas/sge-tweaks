#!/usr/bin/env python
import commands
from string import strip
from collections import defaultdict 
import re

MEM_STRING_LEN = 15
MAXVMEMMATCH = re.compile("maxvmem=(\d+(\.\d+)?\w?)")
VMEMMATCH = re.compile("vmem=(\d+(\.\d+)?\w?)")
H_VMEMMATCH = re.compile("h_vmem=(\d+(\.\d+)?\w?)")
H_VMEMMATCH_m = re.compile("h_vmem=(\d+(\.\d+)?\w?)", re.MULTILINE)

mK = 1024
mM = 1024*1024
mG = 1024*1024*1024

mk = 1000
mm = 1000*1000
mg = 1000*1000*1000

DAY = 3600 * 24
HOUR = 3600
MINUTE = 60

def tm2sec(tm):
    try:
        return time.mktime(time.strptime(tm))
    except Exception: 
        return 0.0

def mem2bytes(mem):
    try: 
        bytes = float(mem)
    except ValueError: 
        mod = mem[-1]
        mem = mem[:-1]
        if mod == "K": 
            bytes = float(mem) * mK
        elif mod == "M":
            bytes = float(mem) * mM
        elif mod == "G":
            bytes = float(mem) * mG
        elif mod == "k": 
            bytes =  float(mem) * mk
        elif mod == "m":
            bytes = float(mem) * mm
        elif mod == "g":
            bytes = float(mem) * mg

    return bytes

def bytes2mem(bytes):
    if bytes > mG:
        return "%0.2fG" %(float(bytes)/mG)
    elif bytes > mM:
        return "%0.2fM" %(float(bytes)/mM)
    elif bytes > mK:
        return "%0.2fK" %(float(bytes)/mK)
    else:
        return bytes


PER_SLOT_MEM=False
mem_consumable_type = commands.getoutput('qconf -sc|grep h_vmem|awk \'{print $6}\'').strip()
if mem_consumable_type == "YES":
    PER_SLOT_MEM = True

#hosts = map(strip, commands.getoutput("qconf -sel").split("\n"))
# I had to use shorter host names, because they were truncated in qstat output
hosts = map(strip, commands.getoutput('qconf -sel|cut -f1 -d"."').split("\n"))

host2avail_mem = {}
host2avail_slots = {}
for h in hosts:
    hinfo = commands.getoutput("qconf -se %s" %h)
    mem_match = re.search(H_VMEMMATCH_m, hinfo)
    slots_match = re.search("[\s,]?slots=(\d+)", hinfo, re.MULTILINE)
    if mem_match:
        host2avail_mem[h] = mem2bytes(mem_match.groups()[0])
    if mem_match:
        host2avail_slots[h] = int(slots_match.groups()[0])


running_jobs = map(strip, commands.getoutput('qstat -s r -u \'*\'').split("\n"))
host2slots = defaultdict(int)
host2vmem = defaultdict(int)
host2usedmem = defaultdict(int)
job2info = {}
job2vmem = {}

job_task2info = {}

for x in running_jobs[2:]:
    #print x.split()
    try:
        jid, pri, name, user, status, date, tm,  queue, slots, task = fields = x.split()
    except ValueError:
        task = 1
        jid, pri, name, user, status, date, tm,  queue, slots = fields = x.split()
    
    slots = int(slots)
    task = int(task)
    jid = int(jid)
    if jid not in job2info:
        content = job2info[jid] = commands.getoutput("qstat -j %s" %jid)
        for line in content.split("\n"):
            usage_match = re.search("^usage\s+(\d+):(.+)", line)
            if line.startswith("hard resource_list:"):
                rt_match = re.search("h_rt=(\d+)", line)
                mem_match = re.search(H_VMEMMATCH, line)
                if rt_match:
                    job2vmem[jid] = mem2bytes(mem_match.groups()[0])

            elif usage_match:
                taskinfo = {}
                taskid = int(usage_match.groups()[0])
                mem_match = re.search(VMEMMATCH, line)
                maxmem_match = re.search(MAXVMEMMATCH, line)
                if maxmem_match:
                    taskinfo["maxvmem"] = mem2bytes(maxmem_match.groups()[0])
                if maxmem_match:
                    taskinfo["vmem"] = mem2bytes(mem_match.groups()[0])
                taskinfo["raw"] = line
                job_task2info[(jid, taskid)] = taskinfo
                   
                    
    ho = queue.split("@")[1].split(".")[0]
    #print jid, task, bytes2mem(job_task2info.get((jid, task), {}).get("vmem", 0))
    host2usedmem[ho] += job_task2info.get((jid, task), {}).get("vmem", 0)
    host2slots[ho]+= slots
    if PER_SLOT_MEM:
        host2vmem[ho] += (job2vmem[jid] * slots)
    else:
        host2vmem[ho] += job2vmem[jid]

header = "Host", "used", "tot.", "res.", "used", "tot.", 
for x in hosts:
    mem_factor_used = (host2usedmem.get(x, 0) * MEM_STRING_LEN) / host2avail_mem[x]
    mem_factor_unused = (host2vmem.get(x, 0) * MEM_STRING_LEN) / host2avail_mem[x]
    mem_factor_unused -= mem_factor_used

    mem_char_used = int(round(mem_factor_used))
    mem_char_unused = int(round(mem_factor_unused))
    mem_char_free = MEM_STRING_LEN- mem_char_used - mem_char_unused
        
    fields = [x.ljust(8), host2slots.get(x, 0), host2avail_slots[x],
              bytes2mem(host2vmem.get(x, 0)),
              bytes2mem(host2usedmem.get(x, 0)),
              bytes2mem(host2avail_mem[x]),
              "#" * mem_char_used +
              "~" * mem_char_unused +
              " " * mem_char_free, 
              "#"*host2slots.get(x, 0) + ("." * (host2avail_slots[x] - host2slots.get(x, 0))),
    ]
    print '\t'.join(map(str, fields))

print 
print "# : used"
print "~ : reserved but not used"    
print ". : empty"
