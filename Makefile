KERNEL_NAME := $(shell uname -s)
PRIV = $(MIX_APP_PATH)/priv
BUILD = $(MIX_APP_PATH)/obj
LIB = $(PRIV)/duxdb.so

DUXDB_LIB_DIR ?=

CFLAGS = -I"$(ERTS_INCLUDE_DIR)" -fPIC -pedantic -Wall -Wextra -Werror \
	-Wno-unused-parameter -Wno-strict-prototypes

LDLIBS = -lduckdb

ifneq ($(filter dev test,$(MIX_ENV)),)
	CFLAGS += -g
else
	CFLAGS += -O3 -DNDEBUG
endif

ifeq ($(KERNEL_NAME), Darwin)
	DUXDB_PREFIX ?= $(shell brew --prefix duckdb 2>/dev/null)
	LDFLAGS += -dynamiclib -undefined dynamic_lookup -Wl,-rpath,@loader_path
	DUXDB_SHARED_LIB = libduckdb.dylib
else ifeq ($(KERNEL_NAME), Linux)
	LDFLAGS += -shared -Wl,-rpath,'$$ORIGIN'
	DUXDB_SHARED_LIB = libduckdb.so
else
	$(error Unsupported operating system $(KERNEL_NAME))
endif

DUXDB_CFLAGS ?= $(if $(DUXDB_PREFIX),-I"$(DUXDB_PREFIX)/include")
DUXDB_LDFLAGS ?= $(if $(DUXDB_PREFIX),-L"$(DUXDB_PREFIX)/lib")
BUNDLED_DUXDB_LIB = $(PRIV)/$(DUXDB_SHARED_LIB)

all: $(LIB)

ifneq ($(DUXDB_LIB_DIR),)
all: $(BUNDLED_DUXDB_LIB)

$(BUNDLED_DUXDB_LIB): $(DUXDB_LIB_DIR)/$(DUXDB_SHARED_LIB) | $(PRIV)
	@echo " CP $(notdir $@)"
	cp $< $@
endif

$(LIB): $(BUILD)/duxdb.o | $(PRIV)
	@echo " LD $(notdir $@)"
	$(CC) $< $(LDFLAGS) $(DUXDB_LDFLAGS) $(LDLIBS) -o $@

$(PRIV) $(BUILD):
	mkdir -p $@

$(BUILD)/duxdb.o: c_src/duxdb.c | $(BUILD)
	@echo " CC $(notdir $@)"
	$(CC) $(CFLAGS) $(DUXDB_CFLAGS) -c $< -o $@

clean:
	$(RM) $(LIB) $(PRIV)/libduckdb.so $(PRIV)/libduckdb.dylib
	$(RM) -r $(BUILD)

.PHONY: all clean

# Don't echo commands unless the caller exports "V=1"
${V}.SILENT:
