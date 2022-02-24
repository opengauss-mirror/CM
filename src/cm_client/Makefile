BASE=$(shell pwd)
CM_TOP_BUILDDIR = ../..
OPEN_CM_SRC_PATH = $(CM_TOP_BUILDDIR)/src
COMM_LIB_PATH = $(OPEN_CM_SRC_PATH)/lib
BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
include $(BASE_BUILD_PATH)/Makefile.global
subdir = src/cm_client

DESTDIR = $(CM_TOP_BUILDDIR)/output

OBJS = $(OPEN_CM_SRC_PATH)/cm_common/cm_elog.o \
       	$(OPEN_CM_SRC_PATH)/cm_common/be_module.o \
       	$(OPEN_CM_SRC_PATH)/cm_common/cm_misc_base.o \
       	cm_client.o cm_client_api.o

CPPFLAGS += -I${OPEN_CM_SRC_PATH}/include/cm/cm_client
LIBS += -lpthread -l:libsecurec.a

.NOTPARALLEL:
all: libclient.a libclient.so

libclient.so:
	$(CC) -fPIC -shared $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS) $(patsubst %.o,%.cpp,$(OBJS)) $(LIBS) -o libclient.so

libclient.a: $(OBJS)
	ar crs $@ $^

utcmclient: $(OBJS)
	$(CC) -fPIC $(CPPFLAGS) $(CXXFLAGS) -c -o $@
	$(CC) -shared $(LDFLAGS) $(OBJS) -o libutcmclient.so
	mv libutcmclient.so $(CM_TOP_BUILDDIR)/test/ut/lib

install:all

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libclient.so' '$(DESTDIR)$(includedir)/cm_client_api.h'

clean:
	rm -f *.so *.a *.o