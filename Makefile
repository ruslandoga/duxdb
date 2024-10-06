KERNEL_NAME := $(shell uname -s)
PRIV = $(MIX_APP_PATH)/priv
BUILD  = $(MIX_APP_PATH)/obj
LIB = $(PRIV)/duxdb_nif.so

CFLAGS = -std=c23 -Ic_src -I"$(ERTS_INCLUDE_DIR)"
CXXFLAGS = -std=c++17 -Ic_src

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
endif
ifeq ($(KERNEL_NAME), Darwin)
	CXXFLAGS += -fPIC
	CFLAGS += -fPIC
	LDFLAGS += -dynamiclib -undefined dynamic_lookup
endif

OBJS = $(BUILD)/duckdb.o $(BUILD)/duxdb_nif.o

all: $(PRIV) $(BUILD) $(LIB)

$(LIB): $(OBJS)
	@echo " LD $(notdir $@)"
	$(CC) $(LDFLAGS) $(OBJS) -o $(LIB)

$(PRIV) $(BUILD):
	mkdir -p $@

$(BUILD)/duckdb.o: c_src/duckdb.cpp c_src/duckdb.hpp
	@echo " CXX $(notdir $@)"	
	$(CXX) $(CXXFLAGS) -c c_src/duckdb.cpp -o $(BUILD)/duckdb.o

$(BUILD)/duxdb_nif.o: c_src/duxdb_nif.c c_src/duckdb.h
	@echo " CC $(notdir $@)"
	$(CC) $(CFLAGS) -c c_src/duxdb_nif.c -o $(BUILD)/duxdb_nif.o

clean:
	$(RM) $(LIB) $(BUILD)

.PHONY: all clean

# Don't echo commands unless the caller exports "V=1"
${V}.SILENT:
