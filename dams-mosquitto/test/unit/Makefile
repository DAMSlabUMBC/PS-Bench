R=../..
include ${R}/config.mk

.PHONY: all check test test-compile ptest clean

all :

test-compile:
	$(MAKE) -C libcommon $@
	$(MAKE) -C broker $@
	$(MAKE) -C lib $@

check : test

ptest : test
test : test-compile
	$(MAKE) -C libcommon $@
	$(MAKE) -C broker $@
	$(MAKE) -C lib $@

reallyclean : clean
clean :
	$(MAKE) -C libcommon $@
	$(MAKE) -C broker $@
	$(MAKE) -C lib $@
