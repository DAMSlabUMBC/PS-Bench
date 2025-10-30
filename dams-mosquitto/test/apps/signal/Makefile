R=../../..
include ${R}/config.mk

.PHONY: all check test ptest clean
.NOTPARALLEL:

all :

check : test

test :
	./signal-args.py

ptest :
	./test.py

clean:
