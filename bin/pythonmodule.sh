#!/bin/sh
ssh -p$1 $2@$3 "python $4"
