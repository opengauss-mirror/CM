BASE=$(shell pwd)
CM_TOP_BUILDDIR = ../..

BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
include $(BASE_BUILD_PATH)/Makefile.global

ifeq ($(ENABLE_ETCD), ON)
    SUBDIRS = cm_etcdapi
endif
SUBDIRS += cm_etcd_adapter cm_dcc_adapter cm_ddb_adapter

# Supress parallel build to avoid depencies in the subdirectories.
.NOTPARALLEL:

$(recurse)
