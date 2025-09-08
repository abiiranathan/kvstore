# Makefile for KV Store
# Targets: library, CLI, tests, and examples

# Compiler and flags
CC = gcc
CFLAGS = -std=c23 -Wall -Wextra -Wpedantic -Wformat=2 -Wundef -Wshadow \
         -Wpointer-arith -Wcast-align -Wstrict-prototypes -Wmissing-prototypes \
         -Wwrite-strings -Wredundant-decls -Winline -Wno-long-long \
         -Wuninitialized -Wconversion -Wstrict-aliasing=2 -g3 -O2 \
		 -D_GNU_SOURCE

# Debug build flags
DEBUG_CFLAGS = -std=c23 -Wall -Wextra -g3 -O0 -DDEBUG -fsanitize=address,undefined

# Release build flags  
RELEASE_CFLAGS = -std=c23 -Wall -Wextra -O3 -DNDEBUG -flto

# Libraries
LIBS = -lreadline

# Directories
SRCDIR = src
INCDIR = include
BUILDDIR = build
OBJDIR = $(BUILDDIR)/obj
BINDIR = $(BUILDDIR)/bin
LIBDIR = $(BUILDDIR)/lib
TESTDIR = tests
EXAMPLEDIR = examples

# Source files
LIB_SOURCES = kvstore.c
CLI_SOURCES = kv.c
TEST_SOURCES = $(wildcard $(TESTDIR)/*.c)
EXAMPLE_SOURCES = $(wildcard $(EXAMPLEDIR)/*.c)

# Object files
LIB_OBJECTS = $(LIB_SOURCES:%.c=$(OBJDIR)/%.o)
CLI_OBJECTS = $(CLI_SOURCES:%.c=$(OBJDIR)/%.o)
TEST_OBJECTS = $(TEST_SOURCES:$(TESTDIR)/%.c=$(OBJDIR)/test_%.o)
EXAMPLE_OBJECTS = $(EXAMPLE_SOURCES:$(EXAMPLEDIR)/%.c=$(OBJDIR)/example_%.o)

# Targets
LIBRARY = $(LIBDIR)/libkvstore.a
SHARED_LIB = $(LIBDIR)/libkvstore.so
CLI_BINARY = $(BINDIR)/kv-cli
TEST_BINARIES = $(TEST_OBJECTS:$(OBJDIR)/test_%.o=$(BINDIR)/test_%)
EXAMPLE_BINARIES = $(EXAMPLE_OBJECTS:$(OBJDIR)/example_%.o=$(BINDIR)/%)

# Default target
.PHONY: all
all: $(LIBRARY) $(CLI_BINARY)

# Debug build
.PHONY: debug
debug: CFLAGS = $(DEBUG_CFLAGS)
debug: clean all

# Release build
.PHONY: release  
release: CFLAGS = $(RELEASE_CFLAGS)
release: clean all

# Static library
$(LIBRARY): $(LIB_OBJECTS) | $(LIBDIR)
	@echo "Creating static library: $@"
	$(AR) rcs $@ $^

# Shared library
$(SHARED_LIB): CFLAGS += -fPIC
$(SHARED_LIB): $(LIB_OBJECTS) | $(LIBDIR)
	@echo "Creating shared library: $@"
	$(CC) -shared -o $@ $^

# CLI binary
$(CLI_BINARY): $(CLI_OBJECTS) $(LIBRARY) | $(BINDIR)
	@echo "Linking CLI binary: $@"
	$(CC) $(CFLAGS) -o $@ $< -L$(LIBDIR) -lkvstore $(LIBS)

# Test binaries
$(BINDIR)/test_%: $(OBJDIR)/test_%.o $(LIBRARY) | $(BINDIR)
	@echo "Linking test binary: $@"
	$(CC) $(CFLAGS) -o $@ $< -L$(LIBDIR) -lkvstore

# Example binaries  
$(BINDIR)/%: $(OBJDIR)/example_%.o $(LIBRARY) | $(BINDIR)
	@echo "Linking example binary: $@"
	$(CC) $(CFLAGS) -o $@ $< -L$(LIBDIR) -lkvstore

# Object files for library
$(OBJDIR)/%.o: %.c kvstore.h | $(OBJDIR)
	@echo "Compiling: $<"
	$(CC) $(CFLAGS) -c -o $@ $<

# Object files for CLI
$(OBJDIR)/kv-cli.o: kv-cli.c kvstore.h | $(OBJDIR)
	@echo "Compiling CLI: $<"
	$(CC) $(CFLAGS) -c -o $@ $< 

# Object files for tests
$(OBJDIR)/test_%.o: $(TESTDIR)/%.c kvstore.h | $(OBJDIR)
	@echo "Compiling test: $<"
	$(CC) $(CFLAGS) -c -o $@ $<

# Object files for examples
$(OBJDIR)/example_%.o: $(EXAMPLEDIR)/%.c kvstore.h | $(OBJDIR)
	@echo "Compiling example: $<"
	$(CC) $(CFLAGS) -c -o $@ $<

# Create directories
$(OBJDIR) $(BINDIR) $(LIBDIR):
	mkdir -p $@

# Test targets
.PHONY: test tests
test tests: $(TEST_BINARIES)
	@echo "Running tests..."
	@for test in $(TEST_BINARIES); do \
		echo "Running $$test"; \
		$$test || exit 1; \
	done
	@echo "All tests passed!"

# Example targets
.PHONY: examples
examples: $(EXAMPLE_BINARIES)

# Shared library target
.PHONY: shared
shared: $(SHARED_LIB)

# Install targets
PREFIX ?= /usr/local
INSTALL_BINDIR = $(PREFIX)/bin
INSTALL_LIBDIR = $(PREFIX)/lib
INSTALL_INCDIR = $(PREFIX)/include

.PHONY: install
install: $(LIBRARY) $(CLI_BINARY) kvstore.h
	@echo "Installing to $(PREFIX)..."
	install -d $(INSTALL_BINDIR) $(INSTALL_LIBDIR) $(INSTALL_INCDIR)
	install -m 755 $(CLI_BINARY) $(INSTALL_BINDIR)/
	install -m 644 $(LIBRARY) $(INSTALL_LIBDIR)/
	install -m 644 kvstore.h $(INSTALL_INCDIR)/

.PHONY: install-shared
install-shared: $(SHARED_LIB) $(CLI_BINARY) kvstore.h
	@echo "Installing shared library to $(PREFIX)..."
	install -d $(INSTALL_BINDIR) $(INSTALL_LIBDIR) $(INSTALL_INCDIR)
	install -m 755 $(CLI_BINARY) $(INSTALL_BINDIR)/
	install -m 644 $(SHARED_LIB) $(INSTALL_LIBDIR)/
	install -m 644 kvstore.h $(INSTALL_INCDIR)/
	ldconfig

.PHONY: uninstall
uninstall:
	rm -f $(INSTALL_BINDIR)/kv-cli
	rm -f $(INSTALL_LIBDIR)/libkvstore.a
	rm -f $(INSTALL_LIBDIR)/libkvstore.so
	rm -f $(INSTALL_INCDIR)/kvstore.h

# Documentation
.PHONY: docs
docs: kvstore.h kv-cli.c
	@echo "Generating documentation..."
	doxygen Doxyfile 2>/dev/null || echo "Doxygen not found, skipping docs"

# Packaging
VERSION = $(shell grep KVSTORE_VERSION_MAJOR kvstore.h | cut -d' ' -f3).$(shell grep KVSTORE_VERSION_MINOR kvstore.h | cut -d' ' -f3).$(shell grep KVSTORE_VERSION_PATCH kvstore.h | cut -d' ' -f3)
TARNAME = kvstore-$(VERSION)

.PHONY: dist
dist: clean
	@echo "Creating distribution tarball..."
	mkdir -p $(TARNAME)
	cp -r *.c *.h Makefile README.md LICENSE $(TESTDIR) $(EXAMPLEDIR) $(TARNAME)/
	tar -czf $(TARNAME).tar.gz $(TARNAME)
	rm -rf $(TARNAME)
	@echo "Created $(TARNAME).tar.gz"

# Performance testing
.PHONY: benchmark
benchmark: release $(BINDIR)/benchmark
	@echo "Running benchmarks..."
	$(BINDIR)/benchmark

$(BINDIR)/benchmark: $(OBJDIR)/benchmark.o $(LIBRARY) | $(BINDIR)
	$(CC) $(CFLAGS) -o $@ $< -L$(LIBDIR) -lkvstore

$(OBJDIR)/benchmark.o: benchmark.c kvstore.h | $(OBJDIR)
	$(CC) $(CFLAGS) -c -o $@ $<

# Memory check with valgrind
.PHONY: memcheck
memcheck: debug $(CLI_BINARY)
	@echo "Running memory check..."
	echo "set test value\nget test\ndel test\nquit" | \
	valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all \
	--track-origins=yes $(CLI_BINARY) -b -

# Code coverage
.PHONY: coverage
coverage: CFLAGS += --coverage
coverage: test
	@echo "Generating coverage report..."
	gcov $(LIB_SOURCES)
	lcov --capture --directory . --output-file coverage.info 2>/dev/null || echo "lcov not found"
	genhtml coverage.info --output-directory coverage 2>/dev/null || echo "genhtml not found"

# Static analysis
.PHONY: analyze
analyze:
	@echo "Running static analysis..."
	cppcheck --enable=all --std=c23 *.c *.h 2>/dev/null || echo "cppcheck not found"
	clang-static-analyzer -std=c23 *.c 2>/dev/null || echo "clang static analyzer not found"

# Format code
.PHONY: format
format:
	@echo "Formatting code..."
	clang-format -i *.c *.h $(TESTDIR)/*.c $(EXAMPLEDIR)/*.c 2>/dev/null || echo "clang-format not found"

# Clean targets
.PHONY: clean
clean:
	@echo "Cleaning build directory..."
	rm -rf $(BUILDDIR)
	rm -f *.gcov *.gcda *.gcno coverage.info
	rm -rf coverage

.PHONY: distclean
distclean: clean
	rm -f *.tar.gz
	rm -rf kvstore-*/

# Development helpers
.PHONY: run
run: $(CLI_BINARY)
	$(CLI_BINARY)

.PHONY: run-batch  
run-batch: $(CLI_BINARY)
	echo "set hello world\nget hello\nstats\nquit" | $(CLI_BINARY) -b -

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all          - Build library and CLI (default)"
	@echo "  debug        - Build with debug flags"
	@echo "  release      - Build optimized release version" 
	@echo "  shared       - Build shared library"
	@echo "  test         - Build and run tests"
	@echo "  examples     - Build example programs"
	@echo "  benchmark    - Build and run performance tests"
	@echo "  install      - Install binaries and headers"
	@echo "  uninstall    - Remove installed files"
	@echo "  docs         - Generate documentation"
	@echo "  dist         - Create distribution tarball"
	@echo "  memcheck     - Run valgrind memory check"
	@echo "  coverage     - Generate code coverage report"
	@echo "  analyze      - Run static analysis"
	@echo "  format       - Format code with clang-format"
	@echo "  run          - Run CLI interactively"
	@echo "  run-batch    - Run CLI with sample commands"
	@echo "  clean        - Remove build files"
	@echo "  distclean    - Remove all generated files"
	@echo "  help         - Show this help"

# Dependency generation (for incremental builds)
-include $(LIB_OBJECTS:.o=.d)
-include $(CLI_OBJECTS:.o=.d)

%.d: %.c
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

