KERNEL_NAME := $(shell uname -s)
PRIV = $(MIX_APP_PATH)/priv
BUILD = $(MIX_APP_PATH)/obj
LIB = $(PRIV)/duck_nif.so

DUCK_NIF_LIB_DIR ?=

CFLAGS = -I"$(ERTS_INCLUDE_DIR)" -fPIC -pedantic -Wall -Wextra -Werror \
	-Wno-unused-parameter -Wno-strict-prototypes

LDLIBS = -lduckdb

ifneq ($(filter dev test,$(MIX_ENV)),)
	CFLAGS += -g
else
	CFLAGS += -O3 -DNDEBUG
endif

ifeq ($(KERNEL_NAME), Darwin)
	DUCKDB_PREFIX ?= $(shell brew --prefix duckdb 2>/dev/null)
	LDFLAGS += -dynamiclib -undefined dynamic_lookup -Wl,-rpath,@loader_path
	DUCKDB_SHARED_LIB = libduckdb.dylib
else ifeq ($(KERNEL_NAME), Linux)
	LDFLAGS += -shared -Wl,-rpath,'$$ORIGIN'
	DUCKDB_SHARED_LIB = libduckdb.so
else
	$(error Unsupported operating system $(KERNEL_NAME))
endif

DUCK_NIF_CFLAGS ?= $(if $(DUCKDB_PREFIX),-I"$(DUCKDB_PREFIX)/include")
DUCK_NIF_LDFLAGS ?= $(if $(DUCKDB_PREFIX),-L"$(DUCKDB_PREFIX)/lib")
BUNDLED_DUCKDB_LIB = $(PRIV)/$(DUCKDB_SHARED_LIB)

all: $(LIB)

ifneq ($(DUCK_NIF_LIB_DIR),)
all: $(BUNDLED_DUCKDB_LIB)

$(BUNDLED_DUCKDB_LIB): $(DUCK_NIF_LIB_DIR)/$(DUCKDB_SHARED_LIB) | $(PRIV)
	@echo " CP $(notdir $@)"
	cp $< $@
endif

$(LIB): $(BUILD)/duck_nif.o | $(PRIV)
	@echo " LD $(notdir $@)"
	$(CC) $< $(LDFLAGS) $(DUCK_NIF_LDFLAGS) $(LDLIBS) -o $@

$(PRIV) $(BUILD):
	mkdir -p $@

$(BUILD)/duck_nif.o: c_src/duck_nif.c | $(BUILD)
	@echo " CC $(notdir $@)"
	$(CC) $(CFLAGS) $(DUCK_NIF_CFLAGS) -c $< -o $@

clean:
	$(RM) $(LIB) $(PRIV)/libduckdb.so $(PRIV)/libduckdb.dylib
	$(RM) -r $(BUILD)

.PHONY: all clean

# Don't echo commands unless the caller exports "V=1"
${V}.SILENT:
