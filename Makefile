KERNEL_NAME := $(shell uname -s)
PRIV = $(MIX_APP_PATH)/priv
BUILD = $(MIX_APP_PATH)/obj
LIB = $(PRIV)/duxdb.so

DUXDB_CFLAGS ?=
DUXDB_LDFLAGS ?=

CFLAGS = -Ic_src -I"$(ERTS_INCLUDE_DIR)" -fPIC -pedantic -Wall -Wextra -Werror \
	-Wno-unused-parameter -Wno-unused-variable -Wno-unused-function -Wno-unused-but-set-variable \
	-Wno-unused-value -Wno-unused-label -Wno-unused-result -Wno-unused-local-typedefs \
	-Wno-strict-prototypes

LDFLAGS = -lduckdb

ifeq ($(MIX_ENV), dev)
	CFLAGS += -g
else ifeq ($(MIX_ENV), test)
	CFLAGS += -g
else
	CFLAGS += -O3 -DNDEBUG
endif

ifeq ($(KERNEL_NAME), Darwin)
	LDFLAGS += -dynamiclib -undefined dynamic_lookup
else ifeq ($(KERNEL_NAME), Linux)
	LDFLAGS += -shared
else
	$(error Unsupported operating system $(KERNEL_NAME))
endif

all: $(PRIV) $(BUILD) $(LIB)

$(LIB): $(BUILD)/duxdb.o
	@echo " LD $(notdir $@)"
	$(CC) $(BUILD)/duxdb.o $(LDFLAGS) $(DUXDB_LDFLAGS) -o $@

$(PRIV) $(BUILD):
	mkdir -p $@

$(BUILD)/duxdb.o: c_src/duxdb.c
	@echo " CC $(notdir $@)"
	$(CC) $(CFLAGS) $(DUXDB_CFLAGS) -c c_src/duxdb.c -o $@

clean:
	$(RM) $(LIB) $(BUILD)

.PHONY: all clean

# Don't echo commands unless the caller exports "V=1"
${V}.SILENT:
