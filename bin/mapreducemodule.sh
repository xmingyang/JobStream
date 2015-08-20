#!/bin/sh
ssh -p$1 $2@$3 "hadoop jar $4"
