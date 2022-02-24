BASE=$(shell pwd)
CM_TOP_BUILDDIR = ../..
OPEN_CM_SRC_PATH = $(CM_TOP_BUILDDIR)/src
COMM_LIB_PATH = $(OPEN_CM_SRC_PATH)/lib/
CM_ADAPTER_PATH = $(OPEN_CM_SRC_PATH)/cm_adapter


BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
include $(BASE_BUILD_PATH)/Makefile.global
subdir = src/cm_monitor

DESTDIR = $(CM_TOP_BUILDDIR)/output

SUBDIRS = $(OPEN_CM_SRC_PATH)/cm_common \
	$(OPEN_CM_SRC_PATH)/cm_communication/cm_libpq \
	$(OPEN_CM_SRC_PATH)/cm_communication/cm_protocol

include $(BASE_BUILD_PATH)/common.mk

OBJS = $(SUBDIROBJS) \
	monitor_main.o \
	$(COMM_LIB_PATH)/config/libconfig.a\
	$(COMM_LIB_PATH)/alarm/libalarmclient.a \

LIBS += -l:libsecurec.a -lcjson -lcrypt -ldl -lm -lssl -lcrypto -lpthread -lrt $(addprefix -l,$(G_BIN_EXT_LIBS))

ifeq ($(ENABLE_MULTIPLE_NODES), ON)
    LIBS += -lcgroup
endif

ifeq ($(ENABLE_KRB), ON)
    LIBS += $(addprefix -l, $(KRB_LIBS))
endif

.NOTPARALLEL:
all: om_monitor

om_monitor: $(OBJS)
	$(CC) $(CXXFLAGS) $(LDFLAGS) $(call expand_subsys,$^) -Wl,-z,relro,-z,now -o $@ $(LIBS)

utcmmonitor: $(OBJS)
	$(CC) -fPIC -shared $(CXXFLAGS) $(LDFLAGS) $(call expand_subsys,$^) $(LIBS) -o libutcmmonitor.so
	mv libutcmmonitor.so $(CM_TOP_BUILDDIR)/test/ut/lib

install:all
	mkdir -p $(DESTDIR)/bin
	cp -rf om_monitor $(DESTDIR)/bin

uninstall:
	rm -f '$(DESTDIR)$(bindir)/om_monitor$(X)'

clean distclean maintainer-clean:
	rm -f om_monitor$(X) *.o