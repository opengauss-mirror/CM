CM_TOP_BUILDDIR = ../..
include $(CM_TOP_BUILDDIR)/build/Makefile.global

SUBDIRS = cm_feconnect cm_libpq cm_protocol

# Supress parallel build to avoid depencies in the subdirectories.
.NOTPARALLEL:

$(recurse)
