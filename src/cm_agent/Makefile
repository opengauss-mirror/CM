BASE=$(shell pwd)
CM_TOP_BUILDDIR = ../..
OPEN_CM_SRC_PATH = $(CM_TOP_BUILDDIR)/src
COMM_LIB_PATH = $(OPEN_CM_SRC_PATH)/lib/
CM_ADAPTER_PATH = $(OPEN_CM_SRC_PATH)/cm_adapter
BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
CM_AGENT_LIBPQ_PATH = $(BASE)/client_adpts/libpq
CM_AGENT_SCRIPT_PATH = $(BASE)/client_adpts/script
DIST_CM_AGENT_PATH = $(DIST_PATH)/cm_agent

include $(BASE_BUILD_PATH)/Makefile.global
subdir = src/cm_agent

DESTDIR = $(CM_TOP_BUILDDIR)/output

SUBDIRS = $(OPEN_CM_SRC_PATH)/cm_communication/cm_feconnect \
	$(OPEN_CM_SRC_PATH)/cm_communication/cm_libpq \
	$(OPEN_CM_SRC_PATH)/cm_communication/cm_protocol \
	$(OPEN_CM_SRC_PATH)/cm_common \
	$(OPEN_CM_SRC_PATH)/cm_adapter

include $(BASE_BUILD_PATH)/common.mk

ifeq ($(ENABLE_MULTIPLE_NODES), ON)
    OBJS = $(DIST_CM_AGENT_PATH)/cma_cn_gtm_instance_management.o $(DIST_CM_AGENT_PATH)/cma_cn_gtm_work_threads_mgr.o $(DIST_CM_AGENT_PATH)/cma_cn_process_message.o \
        $(DIST_CM_AGENT_PATH)/cma_coordinator_utils.o $(DIST_CM_AGENT_PATH)/cma_coordinator.o $(DIST_CM_AGENT_PATH)/cma_datanode_scaling.o $(DIST_CM_AGENT_PATH)/cma_gtm_api.o \
        $(DIST_CM_AGENT_PATH)/cma_gtm.o $(DIST_CM_AGENT_PATH)/cma_instance_check.o $(DIST_CM_AGENT_PATH)/cma_process_messages_hadr.o $(DIST_CM_AGENT_PATH)/cma_repair_cn.o
    LIBS += -lcgroup $(addprefix -l,$(KRB_LIBS))
endif

ifeq ($(ENABLE_LIBPQ), ON)
    OBJS += $(CM_AGENT_LIBPQ_PATH)/cma_datanode_utils.o $(CM_AGENT_LIBPQ_PATH)/cma_datanode.o $(CM_AGENT_LIBPQ_PATH)/cma_libpq_api.o 
    CPPFLAGS += -I${OPEN_CM_SRC_PATH}/include/cm/cm_agent/clients/libpq
else
    OBJS +=$(CM_AGENT_SCRIPT_PATH)/cma_client_scripts.o
endif

ifeq ($(ENABLE_KRB), ON)
    LIBS += $(addprefix -l,$(KRB_LIBS)) -lgssrpc_gauss
endif

ifeq ($(ENABLE_MEMCHECK), ON)
    LDFLAGS += -rdynamic
endif

LIBS += -l:libsecurec.a -lpthread -lcjson -lm -lz -ldl $(addprefix -l,$(G_BIN_EXT_LIBS))
LIBS += $(addprefix -l,$(DCC_LIBS))
OBJS += $(SUBDIROBJS) \
	cma_global_params.o cma_alarm.o cma_common.o cma_connect.o cma_create_conn_cms.o \
	cma_instance_management.o cma_instance_management_ext.o cma_log_management.o cma_phony_dead_check.o \
	cma_process_messages.o cma_status_check.o cma_threads.o \
	cma_connect_client.o cma_process_messages_client.o cma_main.o \
	$(COMM_LIB_PATH)/config/libconfig.a \
	$(COMM_LIB_PATH)/alarm/libalarmclient.a \
	$(CM_ADAPTER_PATH)/cm_ddb_adapter/libcmddbadapter.a \
	$(CM_ADAPTER_PATH)/cm_dcc_adapter/libcmdccadapter.a \
	$(CM_ADAPTER_PATH)/cm_etcd_adapter/libcmetcdadapter.a

ifeq ($(ENABLE_ETCD), ON)
    OBJS += $(CM_ADAPTER_PATH)/cm_etcdapi/libetcdapi.a
endif

.NOTPARALLEL:
all: cm_agent

cm_agent: $(OBJS)
	$(CC) $(CXXFLAGS) $(LDFLAGS) $(call expand_subsys,$^) $(LIBS) -o $@
	
utcmagent: $(OBJS)
	$(CC) -fPIC -shared $(CXXFLAGS) $(LDFLAGS) $(call expand_subsys,$^) $(LIBS) -o libutcmagent.so
	mv libutcmagent.so $(CM_TOP_BUILDDIR)/test/ut/lib

install:all
	mkdir -p $(DESTDIR)/bin
	mkdir -p $(DESTDIR)/lib
	mkdir -p $(DESTDIR)/share/config
	cp -rf cm_agent $(DESTDIR)/bin
	cp -rf ${ZLIB_DIRECTORY_LIB}/libz.so* $(DESTDIR)/lib
	cp -rf cm_agent.centralized.conf.sample $(DESTDIR)/share/config/cm_agent.conf.sample

uninstall:
	rm -f '$(DESTDIR)$(bindir)/cm_agent$(X)' '$(DESTDIR)$(datadir)/cm_agent.conf.sample'

clean distclean maintainer-clean:
	rm -f cm_agent$(X) *.o *.so
