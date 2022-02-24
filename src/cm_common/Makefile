BASE=$(shell pwd)
CM_TOP_BUILDDIR = ../..
OPEN_CM_SRC_PATH = $(CM_TOP_BUILDDIR)/src
COMM_LIB_PATH = $(OPEN_CM_SRC_PATH)/lib
CM_ADAPTER_PATH = $(OPEN_CM_SRC_PATH)/cm_adapter

BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build

include $(BASE_BUILD_PATH)/Makefile.global
subdir = src/cm_common

VERSION = 2

DESTDIR = $(CM_TOP_BUILDDIR)/output

CPPFLAGS += -I$(CJSON_DIRECTORY_INC) -I$(CGROUP_DIRECTORY_INC) -l:libsecurec.a

ifeq ($(ENABLE_MULTIPLE_NODES), NO)
    CXXFLAGS += -lcgroup
endif

SUBDIRS = $(COMM_LIB_PATH)/config \
	$(COMM_LIB_PATH)/alarm \
	$(COMM_LIB_PATH)/hotpatch

include $(BASE_BUILD_PATH)/common.mk

OBJS = cm_elog.o be_module.o cm_stringinfo.o cm_misc.o cm_misc_base.o cm_util.o\
	$(COMM_LIB_PATH)/pq/port/path.o \
	$(COMM_LIB_PATH)/config/libconfig.a

.NOTPARALLEL:
all: libcmcommon.a libcmcommon.so config.h

libcmcommon.so:
	$(CC) -fPIC -shared $(CXXFLAGS) $(CPPFLAGS) cm_elog.cpp cm_stringinfo.cpp cm_misc.cpp cm_misc_base.cpp be_module.cpp cm_util.cpp -o libcmcommon.so.$(VERSION)
	rm -f libcmcommon.so && \
	ln -sf libcmcommon.so.$(VERSION) libcmcommon.so

libcmcommon.a: $(OBJS)
	ar crs $@ $^

config.h:$(CM_TOP_BUILDDIR)/build/cmake/config.h.in
	ln -sf $(BASE)/../../build/cmake/config.h.in $(OPEN_CM_SRC_PATH)/include/cm/config.h

utcmcommon: $(OBJS)
	$(CC) -fPIC $(CPPFLAGS) $(CXXFLAGS) -c -o $@
	$(CC) -shared $(LDFLAGS) $(OBJS) -o libutcmcommon.so
	mv libutcmcommon.so $(CM_TOP_BUILDDIR)/test/ut/lib
	\cp $(CJSON_HOME)/lib/libcjson.so $(CM_TOP_BUILDDIR)/test/ut/lib
	\cp $(LIBCGROUP_LIB_PATH)/libcgroup.so $(CM_TOP_BUILDDIR)/test/ut/lib
	\cp $(LIBOPENSSL_LIB_PATH)/libssl.so $(CM_TOP_BUILDDIR)/test/ut/lib

clean:
	rm -f cm_elog.o be_module.o cm_stringinfo.o cm_misc.o cm_misc_base.o cm_cgroup.o libcmcommon.a libcmcommon.so libcmcommon.so.$(VERSION) $(OPEN_CM_SRC_PATH)/include/cm/config.h *.depend

install:all
	mkdir -p $(DESTDIR)/share/config
	cp -rf ${CJSON_DIRECTORY_LIB}/libcjson.so* $(DESTDIR)/lib
	cp -rf ${SSL_DIRECTORY_LIB}/libcrypto.so* $(DESTDIR)/lib
	cp -rf ${SSL_DIRECTORY_LIB}/libssl.so* $(DESTDIR)/lib

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libcmcommon.a'

distclean: clean

maintainer-clean: distclean
