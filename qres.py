#!/usr/bin/env python
import commands
import sys
import os
import argparse
from string import strip
from collections import defaultdict 
import re

MEM_STRING_LEN = 15
MAXVMEMMATCH = re.compile("maxvmem=(\d+(\.\d+)?\w?)")
VMEMMATCH = re.compile("vmem=(\d+(\.\d+)?\w?)")
H_VMEMMATCH = re.compile("h_vmem=(\d+(\.\d+)?\w?)")
H_VMEMMATCH_m = re.compile("h_vmem=(\d+(\.\d+)?\w?)", re.MULTILINE)

# 
# Here some utils taken from my scripts
#
mK = 1024
mM = 1024*1024
mG = 1024*1024*1024

mk = 1000
mm = 1000*1000
mg = 1000*1000*1000

DAY = 3600 * 24
HOUR = 3600
MINUTE = 60

def get_hosts_in_queues(queues):
    all_hosts = set()
    for q in queues:
        qinfo = commands.getoutput('qconf -sq %s' %q)
        qinfo = qinfo.replace("\\\n", " ")
        match = re.search("hostlist\s(.+)", qinfo, re.MULTILINE)
        if match:
            for host in [h.strip() for h in match.groups()[0].split()]:
                if host.startswith("@"):
                    hginfo = commands.getoutput('qconf -shgrp %s' %host)
                    hginfo = hginfo.replace("\\\n", " ")
                    match = re.search("hostlist\s(.+)", hginfo, re.MULTILINE)
                    all_hosts.update([h.strip() for h in match.groups()[0].split()])
                else:
                    all_hosts.update([host])

    # update to keep just the hostname without domain
    for host in all_hosts:
        hostname = host.split('.')[0]
        all_hosts.remove(host)
        all_hosts.add(hostname)

    # convert to list to show nodes sorted
    all_hosts_list = []
    for host in all_hosts:
        all_hosts_list.append(host)

    return list(set(all_hosts_list))

def get_hosts_for_user(user):

    hosts = map(strip, commands.getoutput('qstat -s r -u ' + user + ' | grep -v job-ID |grep -v\
    \'\-\-\'|awk {\'print $8\'}|awk -F \"@\" {\'print $2\'} |awk -F \'.\'\
    {\'print $1\'}').split("\n"))
    
    return list(set(hosts))

def print_as_table(rows, header=None, fields=None, print_header=True, stdout=sys.stdout):
    """ Print >>Stdout, a list matrix as a formated table. row must be a list of
    dicts or lists."""
    if header is None:
        header = []
        
    def _str(i):
        if isinstance(i, float):
            return "%0.2f" %i
        else:
            return str(i)

    vtype = None
    for v in rows:
        if vtype != None and type(v)!=vtype:
            raise ValueError("Mixed row types in input")
        else:
            vtype = type(v)

    lengths  = {}
    if vtype == list or vtype == tuple:
        v_len = len(fields) if fields else len(rows[0])
        
        if header and len(header)!=v_len:
            raise Exception("Bad header length")

        # Get max size of each field
        if not fields:
            fields = range(v_len)
        
        for i,iv in enumerate(fields):
            header_length = 0
            if header != []:
                header_length = len(_str(header[i]))
            max_field_length = max( [ len(_str(r[iv])) for r in rows] )
            lengths[i] = max( [ header_length, max_field_length ] )

        if header and print_header:
            # Print >>Stdout, header names
            for i in xrange(len(fields)):
                print >>stdout, _str(header[i]).rjust(lengths[i])+" | ",
            print >>stdout, ""
            # Print >>Stdout, underlines
            for i in xrange(len(fields)):
                print >>stdout, "".rjust(lengths[i],"-")+" | ",
            print >>stdout, ""
        # Print >>Stdout, table lines
        for r in rows:
            for i,iv in enumerate(fields):
                print >>stdout, _str(r[iv]).rjust(lengths[i])+" | ",
            print >>stdout, ""

    elif vtype == dict:
        if header == []:
            header = rows[0].keys()
        for ppt in header:
            lengths[ppt] = max( [len(_str(ppt))]+[ len(_str(p.get(ppt,""))) for p in rows])
        if header:
            for ppt in header:
                print >>stdout, _str(ppt).rjust(lengths[ppt])+" | ",
            print >>stdout, ""
            for ppt in header:
                print >>stdout, "".rjust(lengths[ppt],"-")+" | ",
            print >>stdout, ""

        for p in rows:
            for ppt in header:
                print >>stdout, _str(p.get(ppt,"")).rjust(lengths[ppt])+" | ",
            print >>stdout, ""
            page_counter +=1


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


def get_options():
    '''
    parse option from command line call
    '''
    parser = argparse.ArgumentParser(description='Show your cluster usage')

    parser.add_argument('-u', dest='user', default='\'*\'', help='query\
    just this user')

    parser.add_argument('-q', dest='queue', help='query \
            just this queue')


    options = parser.parse_args()

    # check if user exists
    if options.user != '\'*\'':
        if (os.system('id ' + options.user + ' > /dev/null')) != 0:
            #parser.print_help()
            sys.exit()

    # check if queue exists
    if options.queue:
        if (os.system('qconf -sq ' + options.queue + ' > /dev/null')) != 0:
            sys.exit()
    

    return options



## 
## Main program starts here
##

options = get_options()
        
## Detect type of mem consumable
PER_SLOT_MEM=False
mem_consumable_type = commands.getoutput('qconf -sc|grep h_vmem|awk \'{print $6}\'').strip()
if mem_consumable_type == "YES":
    PER_SLOT_MEM = True

## detect host names

# I had to use shorter host names (without domain), because they were truncated in qstat output
# hosts = map(strip, commands.getoutput("qconf -sel").split("\n"))

# is user arg is passed only take the node where that
# user is running jobs
if options.user != '\'*\'':
    hosts = get_hosts_for_user(options.user)
    # if both user and queue args are passed...
    if options.queue:
        hostsInQueue = map(strip, get_hosts_in_queues([options.queue]))
        toRemove = []
        for x in hosts:
            if x not in hostsInQueue:
                toRemove.append(x)
        for x in toRemove:
            hosts.remove(x)
    hosts.sort()
else:
    # if just queue arg is passed...
    if options.queue:
        hosts = map(strip, get_hosts_in_queues([options.queue]))
        hosts.sort()
    # if no arg is passed take all exec nodes
    else:
        hosts = map(strip, commands.getoutput('qconf -sel|cut -f1 -d"."').split("\n"))
        hosts.sort()

## Detect running jobs
if options.queue:
    running_jobs = map(strip, commands.getoutput('qstat -s r -u ' +\
        options.user + ' -q ' + options.queue).split("\n"))
else:
    running_jobs = map(strip, commands.getoutput('qstat -s r -u ' +\
            options.user).split("\n"))

## detect consumables of each host
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


## Load info about running jobs
host2slots = defaultdict(int)
host2vmem = defaultdict(int)
host2usedmem = defaultdict(int)
job2info = {}
job2vmem = {}
job_task2info = {}

for x in running_jobs[2:]:
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
                if mem_match:
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
    host2usedmem[ho] += job_task2info.get((jid, task), {}).get("vmem", 0)
    host2slots[ho]+= slots
    if PER_SLOT_MEM:
        host2vmem[ho] += (job2vmem[jid] * slots)
    else:
        host2vmem[ho] += job2vmem[jid]


## show collected info
        
entries = []
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
    entries.append(fields)
    
header = "Host", "S.used", "S.tot.", "M.res.", "M.used", "M.tot.", "Mem graph", "Slots graph"

print_as_table(entries, header=header)

print 
print "# : used"
print "~ : reserved but not used"    
print ". : empty"
