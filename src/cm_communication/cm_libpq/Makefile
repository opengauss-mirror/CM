BASE=$(shell pwd)
CM_TOP_BUILDDIR = ../../..
BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
include $(BASE_BUILD_PATH)/Makefile.global
subdir = src/cm_communication/cm_libpq

DESTDIR = $(CM_TOP_BUILDDIR)/output
VERSION = 1

include $(BASE_BUILD_PATH)/common.mk

OBJS = ip.o pqcomm.o pqsignal.o

.NOTPARALLEL:
all: libcmpq.a libcmpq.so

libcmpq.so:
	$(CC) -fPIC -shared $(CXXFLAGS) $(CPPFLAGS) ip.cpp pqcomm.cpp pqsignal.cpp -o libcmpq.so.$(VERSION)
	rm -f libcmpq.so && \
	ln -s libcmpq.so.$(VERSION) libcmpq.so

libcmpq.a: $(OBJS)
	ar crs $@ $^

install:all
	
uninstall:
	rm -f '$(DESTDIR)$(libdir)/libcmpq.a'
	rm -f '$(DESTDIR)$(includedir)/cm-libpq-fe.h'

clean:
	rm -f $(OBJS) libcmpq.a libcmpq.so libcmpq.so.$(VERSION) *.depend

distclean: clean

maintainer-clean: distclean
