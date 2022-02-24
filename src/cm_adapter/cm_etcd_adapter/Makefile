BASE=$(shell pwd)
subdir = src/cm_adapter/cm_etcd_adapter
CM_TOP_BUILDDIR = ../../..


BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
include $(BASE_BUILD_PATH)/Makefile.global

DESTDIR = $(CM_TOP_BUILDDIR)/output

VERSION = 1

include $(BASE_BUILD_PATH)/common.mk

ifeq ($(ENABLE_ETCD), ON)
    OBJS = cm_ddb_etcd.o cm_ddb_etcd_arbitrate.o
else
    OBJS = cm_ddb_etcd_stub.o
    CXXFLAGS += -fstack-protector-all
endif

.NOTPARALLEL:
all: libcmetcdadapter.a libcmetcdadapter.so

libcmetcdadapter.so:
	$(CC) -fPIC -shared $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) $(patsubst %.o,%.cpp,$(OBJS)) -o libcmetcdadapter.so.$(VERSION)
	rm -rf libcmetcdadapter.so && \
	ln -sf libcmetcdadapter.so.$(VERSION) libcmetcdadapter.so

libcmetcdadapter.a: $(OBJS)
	ar crs $@ $^

clean:
	rm -rf $(OBJS) libcmetcdadapter.a libcmetcdadapter.so libcmetcdadapter.so.$(VERSION) *.depend

install:all

distclean:clean

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libcmetcdadapter.a'
