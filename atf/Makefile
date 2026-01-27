# 1. Inherit CM core variables
# (prioritize using externally passed CM variables;
# set default values if not provided to ensure compatibility)
CM_TOP_BUILDDIR ?= $(CURDIR)/../..
PROJECT_SOURCE_DIR ?= $(CM_TOP_BUILDDIR)
BUILD_TYPE ?= Debug
CM_COMMON_FLAGS ?= -std=c++17
BUILD_TUPLE ?= $(shell uname -p)
3RD_PATH ?= $(PROJECT_SOURCE_DIR)/binarylibs
3RD_DEPENDENCY_ROOT ?= $(3RD_PATH)/kernel/dependency
LIB_MODE ?= comm
SSL_DIRECTORY_INC ?= $(3RD_DEPENDENCY_ROOT)/openssl/$(LIB_MODE)/include
SSL_DIRECTORY_LIB ?= $(3RD_DEPENDENCY_ROOT)/openssl/$(LIB_MODE)/lib
COMM_INC ?= $(PROJECT_SOURCE_DIR)/src/include \
            $(PROJECT_SOURCE_DIR)/src/include/cm \
            $(SSL_DIRECTORY_INC)

# 2. Distinguish compilation modes (Debug/Release, consistent with CM)
ifeq ($(BUILD_TYPE), Debug)
    BUILD_MODE = Debug
    OPTIMIZE_LEVEL = -O0 -g
    ifeq ($(ENABLE_MEMCHECK), ON)
        MEMCHECK_FLAGS = -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer
        MEMCHECK_LIBS = asan rt dl
    endif
else ifeq ($(BUILD_TYPE), Release)
    BUILD_MODE = Release
    OPTIMIZE_LEVEL = -O2 -g3
    ENABLE_MEMCHECK = OFF
else
    $(error unsupported BUILD_TYPE: $(BUILD_TYPE), only Debug/Release allowed)
endif

# 3. CM-style secure compilation/linking options
ifeq ($(BUILD_TUPLE), x86_64)
    OS_OPTIONS = -msse4.2 -mcx16 -DUSE_SSE42_CRC32C_WITH_RUNTIME_CHECK
else ifeq ($(BUILD_TUPLE), aarch64)
    OS_OPTIONS = -march=armv8-a+crc
    ifeq ($(ENABLE_MULTIPLE_NODES)_$(BUILD_TYPE), ON_Release)
        OS_OPTIONS += -march=armv8-a+crc+lse
    endif
endif

SECURE_OPTIONS = -fno-common -fstack-protector-strong -fPIE -fwrapv
WARNING_OPTIONS = -Wall -Wextra -Werror -Wendif-labels -Wformat-security \
                  -Wmissing-format-attribute -Wno-attributes -Wno-unused-but-set-variable \
                  -Wno-write-strings -Wpointer-arith
OPTIMIZE_OPTIONS = -pipe -fno-aggressive-loop-optimizations -fno-expensive-optimizations \
                   -fno-omit-frame-pointer -fno-strict-aliasing -freg-struct-return
SECURE_LINK_OPTS = -Wl,-z,noexecstack -Wl,-z,relro,-z,now -pie

# 4. Integrate compilation/linking options
CXX = g++
CXXFLAGS = $(CM_COMMON_FLAGS) $(OS_OPTIONS) $(SECURE_OPTIONS) $(WARNING_OPTIONS) \
           $(OPTIMIZE_OPTIONS) $(OPTIMIZE_LEVEL) $(MEMCHECK_FLAGS) \
           -MMD -I. $(addprefix -I,$(COMM_INC))
LDFLAGS = $(SECURE_LINK_OPTS) -lpthread -lssl -lcrypto -ljemalloc \
          -L$(SSL_DIRECTORY_LIB) $(addprefix -l,$(MEMCHECK_LIBS))

# 5. Path and target definitions (adapted to the new permission logic)
PREFIX ?= /usr/local/atf
LOG_DIR = /var/log/atf
RUN_DIR = /var/run/atf
DIST_DIR = dist
SERVER_SRCS = atf_main.cpp atf_common.cpp
SERVER_OBJS = $(patsubst %.cpp, $(DIST_DIR)/%.o, $(SERVER_SRCS))
SERVER_BIN = $(DIST_DIR)/atf

define ensure_dist_dir
	@mkdir -p $(DIST_DIR)
endef

# 6. Core objectives
all: $(SERVER_BIN)
	@echo "Compilation completed! Server binary is in $(DIST_DIR)/atf (BUILD_TYPE: $(BUILD_TYPE))"

$(SERVER_BIN): $(SERVER_OBJS)
	$(ensure_dist_dir)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(DIST_DIR)/%.o: %.cpp
	$(ensure_dist_dir)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

# Installation objective: Only copy files;
# service registration is handled by install.sh (root)
install: all
	@echo " Starting installation to $(PREFIX) (BUILD_TYPE: $(BUILD_TYPE))..."
	@mkdir -p $(PREFIX)/bin
	@mkdir -p $(PREFIX)/conf
	@mkdir -p $(PREFIX)/ssl
	@mkdir -p $(PREFIX)/scripts
	@mkdir -p $(LOG_DIR)
	@mkdir -p $(RUN_DIR)

	# Verify that the source binary exists; otherwise, report an error
	@if [ ! -f $(SERVER_BIN) ]; then echo "Error: $(SERVER_BIN) not found!"; exit 1; fi
	# Force overwrite old files
	@cp -fv $(SERVER_BIN) $(PREFIX)/bin/
	@chmod +x $(PREFIX)/bin/atf

	@if [ ! -f $(PREFIX)/conf/atf_config.conf ]; then \
		cp -fv ./conf/atf_config.conf $(PREFIX)/conf/; \
		echo "Copied default config file to $(PREFIX)/conf/"; \
	else \
		echo "Config file already exists, skipping..."; \
	fi

	@if [ -d ssl ]; then \
		cp -rfv ssl/* $(PREFIX)/ssl/; \
		echo "Copied SSL files to $(PREFIX)/ssl/"; \
	fi

	@cp -fv atf_start.sh $(PREFIX)/scripts/
	@cp -fv atf_stop.sh $(PREFIX)/scripts/
	@chmod +x $(PREFIX)/scripts/atf_start.sh
	@chmod +x $(PREFIX)/scripts/atf_stop.sh
	@echo "Copied non-root start/stop scripts to $(PREFIX)/scripts/"

	# Force overwrite symbolic links
	@ln -sfv $(PREFIX)/bin/atf /usr/local/bin/atf
	@ln -sfv $(PREFIX)/scripts/atf_start.sh /usr/local/bin/atf_start
	@ln -sfv $(PREFIX)/scripts/atf_stop.sh /usr/local/bin/atf_stop
	@echo "Created global links: atf, atf_start, atf_stop"

	@echo "========================================"
	@echo "Core files installed successfully!"
	@echo "Install path: $(PREFIX)"
	@echo "Config path: $(PREFIX)/conf/atf_config.conf"
	@echo "Non-root scripts: /usr/local/bin/atf_start, /usr/local/bin/atf_stop"
	@echo "========================================"

# Uninstallation objective: Only clean up files;
# service stopping is handled by uninstall.sh (root)
uninstall:
	@echo " Starting core files uninstallation..."

	@rm -f $(PREFIX)/bin/atf
	@rm -f /usr/local/bin/atf
	@rm -f /usr/local/bin/atf_start
	@rm -f /usr/local/bin/atf_stop

	@rm -rf $(PREFIX)/scripts
	@rm -rf $(PREFIX)/ssl

	@echo "========================================"
	@echo "Core files uninstalled!"
	@echo "Note: Service stop/removal is handled by uninstall.sh (root required)"
	@echo "========================================"

# Retain the original debugging/running objectives
valgrind-server: $(SERVER_BIN)
	@valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes $(SERVER_BIN)

run-server: $(SERVER_BIN)
	@$(SERVER_BIN)

clean:
	@rm -rf $(DIST_DIR)
	@rm -f *.d

distclean: clean
	@rm -rf $(PREFIX) >/dev/null 2>&1

-include $(SERVER_OBJS:.o=.d)

.PHONY: all clean distclean install uninstall run-server valgrind-server