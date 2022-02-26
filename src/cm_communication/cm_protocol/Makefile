BASE=$(shell pwd)
CM_TOP_BUILDDIR = ../../..
BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
include $(BASE_BUILD_PATH)/Makefile.global
subdir = src/cm_communication/cm_protocol

include $(BASE_BUILD_PATH)/Makefile.global

VERSION = 1

DESTDIR = $(CM_TOP_BUILDDIR)/output

include $(BASE_BUILD_PATH)/common.mk

OBJS = cm_cipher.o cm_error.o cm_spinlock.o cs_ssl.o

all: libcmprotocol.a libcmprotocol.so

libcmprotocol.so:
	$(CC) -fPIC -shared $(CXXFLAGS) $(CPPFLAGS) $(patsubst %.o,%.cpp,$(OBJS)) -o libcmprotocol.so.$(VERSION)
	rm -f libcmprotocol.so && \
	ln -sf libcmprotocol.so.$(VERSION) libcmprotocol.so

libcmprotocol.a: $(OBJS)
	ar crs $@ $^

install:all

clean:
	rm -f $(OBJS) libcmprotocol.a libcmprotocol.so libcmprotocol.so.$(VERSION) *.depend

distclean: clean

maintainer-clean: distclean
