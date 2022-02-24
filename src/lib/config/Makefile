subdir = src/lib/config
CM_TOP_BUILDDIR = ../../..
BASE_BUILD_PATH = $(CM_TOP_BUILDDIR)/build
include $(CM_TOP_BUILDDIR)/build/Makefile.global

VERSION = 4

include $(BASE_BUILD_PATH)/common.mk

OBJS = cm_config.o

TARGET = libconfig.a

all: libconfig.a libconfig.so

libconfig.so:
	$(CC) -fPIC -shared $(CXXFLAGS) $(CPPFLAGS) cm_config.cpp -o libconfig.so.$(VERSION)
	rm -f libconfig.so && \
	ln -s libconfig.so.$(VERSION) libconfig.so

libconfig.a: $(OBJS)
	ar crs $@ $^

install:all

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

clean distclean maintainer-clean:
	rm -f $(OBJS) libconfig.a libconfig.so libconfig.so.$(VERSION) *.depend
