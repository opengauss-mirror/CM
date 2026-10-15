BASE=$(shell pwd)
subdir = src/cm_adapter/cm_sharedisk_adapter
CM_TOP_BUILDDIR = ../../..
OPEN_CM_SRC_PATH = $(CM_TOP_BUILDDIR)/src
BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
CM_ADAPTER_PATH = $(OPEN_CM_SRC_PATH)/cm_adapter
include $(BASE_BUILD_PATH)/Makefile.global

DESTDIR = $(CM_TOP_BUILDDIR)/output

include $(BASE_BUILD_PATH)/common.mk

OBJS = cm_ddb_sharedisk.o cm_ddb_sharedisk_cmd.o cm_ddb_sharedisk_disklock.o

all: libcmsdapi.a

libcmsdapi.a: $(OBJS)
	ar crs $@ $^

install: all

clean:
	rm -rf $(OBJS) libcmsdapi.a *.depend
