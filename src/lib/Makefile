CM_TOP_BUILDDIR = ../..
include $(CM_TOP_BUILDDIR)/build/Makefile.global

SUBDIRS = hotpatch config alarm

# Supress parallel build to avoid depencies in the subdirectories.
.NOTPARALLEL:

$(recurse)
