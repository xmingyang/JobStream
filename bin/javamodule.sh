#!/bin/sh
ssh -p$1 $2@$3 "java -cp $4"
