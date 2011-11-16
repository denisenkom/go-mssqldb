include $(GOROOT)/src/Make.inc
include $(GOROOT)/src/Make.cmd

% : %.go
	$(GC) $<
	$(LD) $@.$(O)
