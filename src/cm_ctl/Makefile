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

# cipher 实现已迁至 src/cm_communication/cm_protocol/cm_cipher.cpp（经 cmprotocol 库链入），
# src/lib/pq/port/cipher.o 是过时引用。
OBJS = $(patsubst %.cpp,%.o,$(wildcard *.cpp)) $(OPEN_CM_SRC_PATH)/cm_communication/cm_libpq/pqsignal.o

CPPFLAGS += -I$(CJSON_DIRECTORY_INC)

LIBS += $(addprefix -l:,$(CM_BASE_LIBS))
LIBS += $(addprefix -l:,$(CM_ADAPTER_LIBS)) $(addprefix -l,$(DCC_LIBS))
LIBS += -l:libsecurec.a -lpthread -lcjson -lm -ldl -lz $(addprefix -l,$(G_BIN_EXT_LIBS))

ifeq ($(ENABLE_MULTIPLE_NODES), ON)
    OBJS += $(CM_TOP_BUILDDIR)/distribute/cm_ctl/ctl_distribute.o
    LIBS += -lcgroup
endif

ifeq ($(ENABLE_KRB), ON)
    LIBS += $(addprefix -l,$(KRB_LIBS)) -lgssrpc_gauss
endif


all: cm_ctl

cm_ctl: $(OBJS)
	$(CC) $(CXXFLAGS) $(LDFLAGS) $^ $(LIBS) $(RPATH_FLAG) $(PIE_LINK_OPT) -o $@

install:all
	mkdir -p $(DESTDIR)/bin
	cp -rf cm_ctl $(DESTDIR)/bin

clean:
	rm -f cm_ctl *.o
