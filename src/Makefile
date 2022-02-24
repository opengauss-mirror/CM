CM_TOP_BUILDDIR = ..
include $(CM_TOP_BUILDDIR)/build/Makefile.global

SUBDIRS = lib cm_adapter cm_agent cm_client cm_common cm_communication cm_ctl cm_monitor cm_server

# Supress parallel build to avoid depencies in the subdirectories.
.NOTPARALLEL:

$(recurse)
