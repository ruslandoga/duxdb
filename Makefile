KERNEL_NAME := $(shell uname -s)
ARCH := $(shell uname -m)
PRIV = $(MIX_APP_PATH)/priv
BUILD  = $(MIX_APP_PATH)/obj
LIB = $(PRIV)/duxdb_nif.so

# TODO centralize this
DUCKDB_VERSION=v1.1.2

CFLAGS = -std=c23 -Ic_src -I"$(ERTS_INCLUDE_DIR)"
CXXFLAGS = -std=c++17 -Ic_src
LDFLAGS = -Wl,-rpath,$(BUILD) -L$(BUILD) -lduckdb

ifeq ($(MIX_ENV), dev)
    CFLAGS += -g
    CXXFLAGS += -g
else ifeq ($(MIX_ENV), test)
    CFLAGS += -g
    CXXFLAGS += -g
else
	CFLAGS += -O3 -DNDEBUG=1
	CXXFLAGS += -O3 -DNDEBUG=1
endif

ifeq ($(KERNEL_NAME), Linux)
	CXXFLAGS += -fPIC -fvisibility=hidden
	CFLAGS += -fPIC -fvisibility=hidden
	LDFLAGS += -fPIC -shared
	ifeq ($(ARCH), x86_64)
		DUCKDB_URL = https://github.com/duckdb/duckdb/releases/download/$(DUCKDB_VERSION)/libduckdb-linux-amd64.zip
	else ifeq ($(ARCH), aarch64)
		DUCKDB_URL = https://github.com/duckdb/duckdb/releases/download/$(DUCKDB_VERSION)/libduckdb-linux-aarch64.zip
	else
		$(error Unsupported architecture $(ARCH) on Linux)
	endif
endif
ifeq ($(KERNEL_NAME), Darwin)
	CXXFLAGS += -fPIC
	CFLAGS += -fPIC
	LDFLAGS += -dynamiclib -undefined dynamic_lookup
	DUCKDB_URL = https://github.com/duckdb/duckdb/releases/download/$(DUCKDB_VERSION)/libduckdb-osx-universal.zip
else
	$(error Unsupported operating system $(KERNEL_NAME))
endif

OBJS = $(BUILD)/libduckdb.dylib $(BUILD)/duxdb_nif.o

all: $(PRIV) $(BUILD) $(LIB)

$(LIB): $(OBJS)
	@echo "LD $(notdir $@)"
	$(CC) $(LDFLAGS) $(BUILD)/duxdb_nif.o -o $@

$(PRIV) $(BUILD):
	mkdir -p $@

$(BUILD)/libduckdb.dylib:
	@echo "Download: libduckdb.dylib $(DUCKDB_VERSION)"
	@curl -sS -LO $(DUCKDB_URL)
	@unzip -o $(notdir $(DUCKDB_URL)) -d $(BUILD)

$(BUILD)/duxdb_nif.o: c_src/duxdb_nif.c
	@echo "CC $(notdir $@)"
	$(CC) $(CFLAGS) -c c_src/duxdb_nif.c -o $@

clean:
	$(RM) $(LIB) $(BUILD)

.PHONY: all clean

# Don't echo commands unless the caller exports "V=1"
${V}.SILENT:
