BASE=$(shell pwd)
subdir = src/cm_adapter/cm_etcdapi
CM_TOP_BUILDDIR = ../../..


BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
include $(BASE_BUILD_PATH)/Makefile.global

DESTDIR = $(CM_TOP_BUILDDIR)/output

VERSION = 1

include $(BASE_BUILD_PATH)/common.mk

.NOTPARALLEL:
all: libetcdapi.a libetcdapi.so

cm_etcdapi.o:
	$(CC) -c -fPIC -shared $(CXXFLAGS) $(CPPFLAGS) cm_etcdapi.cpp -o cm_etcdapi.o

libetcdapi.so: libetcdapi.a
	$(CC) -fPIC -shared $(CXXFLAGS) $(CPPFLAGS) *.o -o libetcdapi.so.$(VERSION) -lpthread
	rm -rf libetcdapi.so && \
	ln -s libetcdapi.so.$(VERSION) ./libetcdapi.so

libetcdapi.a: output cm_etcdapi.o
	ar -crv $@ *.o

output:
	ar -x ${3RD_PATH}/kernel/dependency/etcd/comm/lib/libclientv3.a

install: all

.PHONY: clean
clean distclean:
	rm -rf libetcdapi.so libetcdapi.so.$(VERSION) libetcdapi.a libclientv3.a *.o *depend
	rm -rf $(CM_TOP_BUILDDIR)/include/cm/cm_errcodes.h
	rm -rf cm_errcodes.h
.PHONY: distclean
distclean:clean

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libetcdapi.a'

librace.a: cm_etcdapi.o
	ar -x ${with_3rd}/kernel/dependency/etcd/comm/lib/libclientv3.a
	ar -crv $@ *.o
	mv $@ libetcdapi.a
