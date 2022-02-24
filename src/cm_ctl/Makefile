BASE=$(shell pwd)
subdir = src/cm_ctl

BASE=$(shell pwd)
CM_TOP_BUILDDIR = ../..
OPEN_CM_SRC_PATH = $(CM_TOP_BUILDDIR)/src
BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
CM_ADAPTER_PATH = $(OPEN_CM_SRC_PATH)/cm_adapter
COMM_LIB_PATH = $(OPEN_CM_SRC_PATH)/lib
DESTDIR = $(CM_TOP_BUILDDIR)/output

include $(BASE_BUILD_PATH)/Makefile.global

SUBDIRS = $(OPEN_CM_SRC_PATH)/cm_communication/cm_feconnect \
	$(OPEN_CM_SRC_PATH)/cm_communication/cm_protocol \
	$(OPEN_CM_SRC_PATH)/cm_common \
	$(CM_ADAPTER_PATH)

include $(BASE_BUILD_PATH)/common.mk

OBJS = $(SUBDIROBJS) \
	$(COMM_LIB_PATH)/pq/port/cipher.o $(OPEN_CM_SRC_PATH)/cm_communication/cm_libpq/pqsignal.o \
	cm_ctl.o ctl_common.o ctl_start.o ctl_stop.o ctl_finishredo.o ctl_query.o ctl_query_base.o ctl_switchover.o ctl_restart.o ctl_misc.o ctl_guc.o ctl_help.o ctl_switch.o ctl_param_check.o\
	$(COMM_LIB_PATH)/hotpatch/client/libhotpatchclient.a \
	$(CM_ADAPTER_PATH)/cm_ddb_adapter/libcmddbadapter.a \
	$(CM_ADAPTER_PATH)/cm_dcc_adapter/libcmdccadapter.a \
	$(CM_ADAPTER_PATH)/cm_etcd_adapter/libcmetcdadapter.a

LIBS = -l:libsecurec.a -lpthread -lcjson -lm $(addprefix -l,$(G_BIN_EXT_LIBS))
LIBS += $(addprefix -l,$(DCC_LIBS))

ifeq ($(ENABLE_MULTIPLE_NODES), ON)
    OBJS += $(CM_TOP_BUILDDIR)/distribute/cm_ctl/ctl_distribute.o
    LIBS += -lcgroup
endif

ifeq ($(ENABLE_KRB), ON)
    LIBS += $(addprefix -l,$(KRB_LIBS)) -lgssrpc_gauss
endif

ifeq ($(ENABLE_ETCD), ON)
    OBJS += $(CM_ADAPTER_PATH)/cm_etcdapi/libetcdapi.a
endif

all: cm_ctl

cm_ctl: $(OBJS)
	$(CC) $(CXXFLAGS) $(call expand_subsys,$^) $(LIBS) $(LDFLAGS) -o $@$(X)

utcmctl: $(OBJS)
	$(CC) -fPIC -shared $(CXXFLAGS) $(OBJS) $(LIBS) $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) -o libutcmctl.so
	mv libutcmctl.so $(CM_TOP_BUILDDIR)/test/ut/lib

install:all
	mkdir -p $(DESTDIR)/bin
	cp -rf cm_ctl $(DESTDIR)/bin

uninstall:
	rm -f '$(DESTDIR)$(bindir)/cm_ctl$(X)'

clean distclean maintainer-clean:
	rm -f cm_ctl$(X) *.o

