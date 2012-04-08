#!/bin/sh
# Copyright (c) 2001, 2010, Oracle and/or its affiliates. All rights reserved.  

########################################################################### 
#
# Example of a JSV script that verifies if a special PE (here: mytestpe),
# which is configured with the $pe_slots strategy for requesting slots 
# on only one host, is used together with a core binding strategy to 
# ensure that all requests are using the core binding facility.
# 
# "mytestpe" should always be requested in case of a parallel job which should
# make use of more than one core 
#
# This script ensures that: 
#  
# - all non-parallel requests are allocating exactly one core (linear:1)
# - all parallel requests to the parallel environment "mytestpe" 
#   are allocating not more cores than (max-) slots.
# - in case if max-slots is lesser than requested cores, the 
#   max-slots is modified
# - non-parallel jobs with binding request of more than one core are 
#   rejected
# - requests to other parallel environments are rejected (they have 
#   to be configured here depending on they are using one host or more)
#
#
# Be careful:  Job verification scripts are started with sgeadmin 
#              permissions if they are executed within the master process
#
# Since this is a bash script (which could be slow) use this script 
# on client side.

PATH=/bin:/usr/bin

jsv_on_start()
{
   return
}

jsv_on_verify()
{
   if [ "`jsv_get_param binding_strategy`" != "" ]; then
      
      # ---------------------------------------------
      # a binding strategy was requested 
      # -> check if amount of cores is amount of slots 
      # ---------------------------------------------

      # get amount of cores requested for binding
      if [ "`jsv_get_param binding_strategy`" != "explicit" ]; then
         # for linear and striding "binding_amount" denotes the # of cores
         amount=`jsv_get_param binding_amount`
      else 
         # for the explicit request the numer of requestet cores are here
         amount=`jsv_get_param binding_exp_n`
      fi

      if [ "`jsv_get_param pe_name`" = "smp" ]; then

         # ------------------------------------------------
         # verify if requested slots equals requested cores
         # if not correct the parallel environment request
         # ------------------------------------------------
         max_slots=`jsv_get_param pe_max`
         min_slots=`jsv_get_param pe_min`

         # min and max slots must be equal in order to ensure 
         # that cores are matching slots
         if [ $max_slots != $min_slots ]; then
	         jsv_set_param pe_min $max_slots
         fi
 
         # correct the pe request more cores than slots are requested
         if [ $amount > $max_slots ]; then
            jsv_set_param pe_max $amount
            jsv_set_param pe_min $amount

            jsv_correct "Max slots was set to the amount of requested cores"
            return
         fi
      else

         # -------------------------------------------------------------
         # No parallel environment was requested -> ensure that not more 
         # than 1 core is requested 
         # -------------------------------------------------------------
         if [ x$amount != "x1" ]; then
            jsv_reject "Amount of requested cores != 1: Please request PE!"
            return
         fi 
	      jsv_correct "linear now"

      fi
         
   else
      
      # ------------------------------
      # core binding was not requested
      # ------------------------------

      if [ "`jsv_get_param pe_name`" = "" ]; then

         # -------------------------------------------
         # in case no parallel environment was chosen add a default
         # request of one processor core AND add the job the default
         # pe
         # -------------------------------------------
         jsv_set_param pe_name "smp"
         jsv_set_param pe_max "1"
         jsv_set_param pe_min "1"

         # set the binding strategy to linear (without given start point: linear_automatic)
         jsv_set_param binding_type "set"
         jsv_set_param binding_amount "1"
         jsv_set_param binding_strategy "linear_automatic"
	
         jsv_correct "Job was modified by JSV"
         return

      elif [ "`jsv_get_param pe_name`" = "smp" ]; then

         # --------------------------------------------
         # "mytestpe" was requested but no core binding 
         # -> set linear allocation with pe max slots 
         # --------------------------------------------
         
         # max amount of requested slots (mytestpe)
         # max_slots=`jsv_get_param pe_max`
         # if allocation rule is $pe_slots --> use max_slots for amount
         jsv_set_param binding_amount "1"

         jsv_set_param binding_type "set"
         jsv_set_param binding_strategy "linear_automatic"
 
         jsv_correct "Job was modified by JSV"
         return

       else
         
         # --------------------------------------------- 
         # some other parallel environment was requested
         # -> the binding request is valid for each host 
         # --------------------------------------------- 
         jsv_reject "Other PE..."

         return

       fi

     fi

   jsv_accept "Job is accepted"
}

. ${SGE_ROOT}/util/resources/jsv/jsv_include.sh

# main routine handling the protocol between client/master and JSV script
jsv_main

