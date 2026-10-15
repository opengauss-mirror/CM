BASE=$(shell pwd)
subdir = src/cm_adapter/cm_sharediskapi
CM_TOP_BUILDDIR = ../../..
OPEN_CM_SRC_PATH = $(CM_TOP_BUILDDIR)/src
BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
CM_ADAPTER_PATH = $(OPEN_CM_SRC_PATH)/cm_adapter
include $(BASE_BUILD_PATH)/Makefile.global

DESTDIR = $(CM_TOP_BUILDDIR)/output

include $(BASE_BUILD_PATH)/common.mk

OBJS = cm_disk_rw.o cm_voting_disk.o cm_vtable.o share_disk_api.o

all: libcmsharedisk.a

libcmsharedisk.a: $(OBJS)
	ar crs $@ $^

install: all

clean:
	rm -rf $(OBJS) libcmsharedisk.a *.depend
