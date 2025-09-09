# KV Store Makefile
# Modern C23 build system with proper dependency tracking

# Project info
PROJECT = kvstore
VERSION = 1.0.0

# Compiler and tools
CC = gcc
AR = ar
INSTALL = install
MKDIR_P = mkdir -p

# Directories
SRCDIR = src
INCDIR = include
BUILDDIR = build
OBJDIR = $(BUILDDIR)/obj
BINDIR = $(BUILDDIR)/bin
LIBDIR = $(BUILDDIR)/lib

# Installation paths
PREFIX ?= /usr/local
INSTALL_BINDIR = $(PREFIX)/bin
INSTALL_LIBDIR = $(PREFIX)/lib
INSTALL_INCDIR = $(PREFIX)/include

# Compiler flags
CSTD = -std=c23
WARNINGS = -Wall -Wextra -Wpedantic -Wformat=2 -Wundef -Wshadow \
           -Wpointer-arith -Wcast-align -Wstrict-prototypes \
           -Wmissing-prototypes -Wwrite-strings -Wredundant-decls \
           -Wuninitialized -Wconversion -Wstrict-aliasing=2

INCLUDES = -I$(INCDIR)
DEFINES = -D_GNU_SOURCE
BASE_CFLAGS = $(CSTD) $(WARNINGS) $(INCLUDES) $(DEFINES)

# Build configurations
DEBUG_CFLAGS = $(BASE_CFLAGS) -g3 -O0 -DDEBUG -fsanitize=address,undefined
RELEASE_CFLAGS = $(BASE_CFLAGS) -O3 -DNDEBUG -flto -march=native
DEFAULT_CFLAGS = $(BASE_CFLAGS) -g -O2

# Default to optimized debug build
CFLAGS ?= $(DEFAULT_CFLAGS)

# Libraries
LIBS = -lreadline -lm

# Source files
LIB_SOURCES = $(wildcard $(SRCDIR)/*.c)
CLI_SOURCES = kv-cli.c
SERVER_SOURCES = kv-server.c

# Object files
LIB_OBJECTS = $(LIB_SOURCES:$(SRCDIR)/%.c=$(OBJDIR)/%.o)
CLI_OBJECTS = $(OBJDIR)/kv-cli.o
SERVER_OBJECTS = $(OBJDIR)/kv-server.o

# Targets
STATIC_LIB = $(LIBDIR)/lib$(PROJECT).a
SHARED_LIB = $(LIBDIR)/lib$(PROJECT).so
CLI_BINARY = $(BINDIR)/kv-cli
SERVER_BINARY = $(BINDIR)/kv-server

# Header files for installation
HEADERS = $(wildcard $(INCDIR)/*.h)

# Default target
.PHONY: all
all: $(STATIC_LIB) $(CLI_BINARY)

# Build configurations
.PHONY: debug release
debug:
	$(MAKE) CFLAGS="$(DEBUG_CFLAGS)" all

release:
	$(MAKE) CFLAGS="$(RELEASE_CFLAGS)" all

# Shared library target
.PHONY: shared
shared: $(SHARED_LIB)

# Server target
.PHONY: server
server: $(SERVER_BINARY)

# Complete build
.PHONY: full
full: $(STATIC_LIB) $(SHARED_LIB) $(CLI_BINARY) $(SERVER_BINARY)

# Static library
$(STATIC_LIB): $(LIB_OBJECTS) | $(LIBDIR)
	@echo "AR $@"
	@$(AR) rcs $@ $^

# Shared library (requires PIC compilation)
$(SHARED_LIB): $(LIB_SOURCES) | $(LIBDIR)
	@echo "CC $@ (shared)"
	@$(CC) $(CFLAGS) -fPIC -shared -o $@ $^ $(LIBS)

# CLI binary
$(CLI_BINARY): $(CLI_OBJECTS) $(STATIC_LIB) | $(BINDIR)
	@echo "LD $@"
	@$(CC) $(CFLAGS) -o $@ $< -L$(LIBDIR) -l$(PROJECT) $(LIBS)

# Server binary (if it exists)
$(SERVER_BINARY): $(SERVER_OBJECTS) $(STATIC_LIB) | $(BINDIR)
	@echo "LD $@"
	@$(CC) $(CFLAGS) -o $@ $< -L$(LIBDIR) -l$(PROJECT) $(LIBS)

# Object file compilation
$(OBJDIR)/%.o: $(SRCDIR)/%.c | $(OBJDIR)
	@echo "CC $<"
	@$(CC) $(CFLAGS) -c -o $@ $<

$(OBJDIR)/%.o: %.c | $(OBJDIR)
	@echo "CC $<"
	@$(CC) $(CFLAGS) -c -o $@ $<

# Directory creation
$(OBJDIR) $(BINDIR) $(LIBDIR):
	@$(MKDIR_P) $@

# Installation
.PHONY: install install-shared uninstall
install: $(STATIC_LIB) $(CLI_BINARY)
	@echo "Installing to $(PREFIX)..."
	@$(MKDIR_P) $(INSTALL_BINDIR) $(INSTALL_LIBDIR) $(INSTALL_INCDIR)
	@$(INSTALL) -m 755 $(CLI_BINARY) $(INSTALL_BINDIR)/
	@$(INSTALL) -m 644 $(STATIC_LIB) $(INSTALL_LIBDIR)/
	@if [ -n "$(HEADERS)" ]; then $(INSTALL) -m 644 $(HEADERS) $(INSTALL_INCDIR)/; fi
	@echo "Installation complete"

install-shared: $(SHARED_LIB) $(CLI_BINARY)
	@echo "Installing shared library to $(PREFIX)..."
	@$(MKDIR_P) $(INSTALL_BINDIR) $(INSTALL_LIBDIR) $(INSTALL_INCDIR)
	@$(INSTALL) -m 755 $(CLI_BINARY) $(INSTALL_BINDIR)/
	@$(INSTALL) -m 644 $(SHARED_LIB) $(INSTALL_LIBDIR)/
	@if [ -n "$(HEADERS)" ]; then $(INSTALL) -m 644 $(HEADERS) $(INSTALL_INCDIR)/; fi
	@ldconfig 2>/dev/null || true
	@echo "Shared library installation complete"

uninstall:
	@echo "Uninstalling from $(PREFIX)..."
	@rm -f $(INSTALL_BINDIR)/kv-cli
	@rm -f $(INSTALL_BINDIR)/kv-server
	@rm -f $(INSTALL_LIBDIR)/lib$(PROJECT).a
	@rm -f $(INSTALL_LIBDIR)/lib$(PROJECT).so
	@for hdr in $(notdir $(HEADERS)); do rm -f $(INSTALL_INCDIR)/$$hdr; done
	@echo "Uninstallation complete"

# Development targets
.PHONY: run run-batch test
run: $(CLI_BINARY)
	@$(CLI_BINARY)

run-batch: $(CLI_BINARY)
	@echo "set hello world\nget hello\nstats\nquit" | $(CLI_BINARY)

# Testing (placeholder - add actual tests)
test: $(CLI_BINARY)
	@echo "Running tests..."
	@echo "Tests not yet implemented"

# Distribution
.PHONY: dist
dist: clean
	@echo "Creating distribution tarball..."
	@$(MKDIR_P) $(PROJECT)-$(VERSION)
	@cp -r $(SRCDIR) $(INCDIR) *.c *.h Makefile README.md LICENSE \
		$(PROJECT)-$(VERSION)/ 2>/dev/null || true
	@tar -czf $(PROJECT)-$(VERSION).tar.gz $(PROJECT)-$(VERSION)
	@rm -rf $(PROJECT)-$(VERSION)
	@echo "Created $(PROJECT)-$(VERSION).tar.gz"

# Cleanup
.PHONY: clean distclean
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf $(BUILDDIR)
	@rm -f *.gcov *.gcda *.gcno coverage.info

distclean: clean
	@echo "Cleaning distribution files..."
	@rm -f *.tar.gz
	@rm -rf $(PROJECT)-*/

# Dependency tracking (automatic header dependencies)
DEPS = $(LIB_OBJECTS:.o=.d) $(CLI_OBJECTS:.o=.d) $(SERVER_OBJECTS:.o=.d)

$(OBJDIR)/%.d: $(SRCDIR)/%.c | $(OBJDIR)
	@$(CC) $(CFLAGS) -MM -MT $(@:.d=.o) $< > $@

$(OBJDIR)/%.d: %.c | $(OBJDIR)
	@$(CC) $(CFLAGS) -MM -MT $(@:.d=.o) $< > $@

# Include dependency files (ignore errors if they don't exist yet)
-include $(DEPS)

# Help
.PHONY: help
help:
	@echo "KV Store Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all         - Build static library and CLI (default)"
	@echo "  debug       - Debug build with sanitizers"
	@echo "  release     - Optimized release build"
	@echo "  shared      - Build shared library"
	@echo "  server      - Build server binary (if exists)"
	@echo "  full        - Build everything"
	@echo ""
	@echo "Installation:"
	@echo "  install     - Install static library and binaries"
	@echo "  install-shared - Install shared library and binaries"
	@echo "  uninstall   - Remove installed files"
	@echo ""
	@echo "Development:"
	@echo "  run         - Run CLI interactively"
	@echo "  run-batch   - Run CLI with test commands"
	@echo "  test        - Run test suite"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean       - Remove build artifacts"
	@echo "  distclean   - Remove all generated files"
	@echo "  dist        - Create source distribution"
	@echo ""
	@echo "Variables:"
	@echo "  PREFIX      - Installation prefix (default: /usr/local)"
	@echo "  CC          - Compiler (default: gcc)"

.PHONY: $(PHONY)
