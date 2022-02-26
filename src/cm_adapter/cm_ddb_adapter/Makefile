BASE=$(shell pwd)
subdir = src/cm_adapter/cm_ddb_adapter
CM_TOP_BUILDDIR = ../../..
OPEN_CM_SRC_PATH = $(CM_TOP_BUILDDIR)/src
BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
CM_ADAPTER_PATH = $(OPEN_CM_SRC_PATH)/cm_adapter
include $(BASE_BUILD_PATH)/Makefile.global


DESTDIR = $(CM_TOP_BUILDDIR)/output

VERSION = 1

include $(BASE_BUILD_PATH)/common.mk

OBJS = cm_ddb_adapter.o \
	$(CM_ADAPTER_PATH)/cm_dcc_adapter/libcmdccadapter.a \
	$(CM_ADAPTER_PATH)/cm_etcd_adapter/libcmetcdadapter.a

ifeq ($(ENABLE_ETCD), ON)
    OBJS += $(CM_ADAPTER_PATH)/cm_etcdapi/libetcdapi.a
endif

.NOTPARALLEL:
all: libcmddbadapter.a libcmddbadapter.so

libcmddbadapter.so:
	$(CC) -fPIC -shared $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) cm_ddb_adapter.cpp -o libcmddbadapter.so.$(VERSION)
	rm -rf  libcmddbadapter.so && \
	ln -sf libcmddbadapter.so.$(VERSION) ./libcmddbadapter.so

libcmddbadapter.a: $(OBJS)
	ar crs $@ $^

clean:
	rm -rf $(OBJS) libcmddbadapter.a libcmddbadapter.so libcmddbadapter.so.$(VERSION) *.depend

install: all

distclean:clean

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libcmddbadapter.a'
