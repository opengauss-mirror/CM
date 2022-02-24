BASE=$(shell pwd)
CM_TOP_BUILDDIR = ../../..
BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
include $(BASE_BUILD_PATH)/Makefile.global
subdir = src/cm_communication/cm_feconnect

VERSION = 1

include $(BASE_BUILD_PATH)/common.mk

OBJS = fe-misc.o fe-connect.o fe-protocol.o ip.o pqexpbuffer.o

all: libcmclient.a libcmclient.so

libcmclient.so:
	$(CC) -fPIC -shared $(CXXFLAGS) $(CPPFLAGS) fe-misc.cpp fe-connect.cpp fe-protocol.cpp ip.cpp pqexpbuffer.cpp -o libcmclient.so.$(VERSION)
	rm -f libcmclient.so && \
	ln -sf libcmclient.so.$(VERSION) libcmclient.so

libcmclient.a: $(OBJS)
	ar crs $@ $^

install:all

utcmclient:
	$(CC) -fPIC -shared $(CXXFLAGS) $(CPPFLAGS) fe-misc.o pqexpbuffer.o -L$(SECURE_LIB_PATH) -l:libsecurec.a -o libutcmclient.so
	mv libutcmclient.so $(top_builddir)/../distribute/test/ut/lib/

clean:
	rm -f $(OBJS) libcmclient.a libcmclient.so libcmclient.so.$(VERSION) *.depend

distclean: clean

maintainer-clean: distclean
