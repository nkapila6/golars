# Build every GIF under vhs/out/ from the checked-in tape files.
# Run from the repo root:
#
#   make -C vhs           # build all gifs
#   make -C vhs gif-sql   # build a single one
#   make -C vhs clean     # drop generated gifs
#
# Requires `vhs` (github.com/charmbracelet/vhs) on $PATH plus a
# freshly built `golars` + `golars-mcp`. The default `all` target
# rebuilds the binaries first so the gifs always match the code.

VHS       ?= vhs
GO        ?= go
ROOT      := $(abspath ..)
BIN_DIR   := $(ROOT)/bin
TAPES     := $(wildcard *.tape)
GIFS      := $(patsubst %.tape,out/%.gif,$(filter-out _common.tape,$(TAPES)))

.PHONY: all build clean $(patsubst %.tape,gif-%,$(filter-out _common.tape,$(TAPES)))

all: build $(GIFS) ## Build binaries + every GIF

build:
	@mkdir -p $(BIN_DIR)
	@echo "  build golars"
	@cd $(ROOT) && $(GO) build -o bin/golars ./cmd/golars
	@echo "  build golars-mcp"
	@cd $(ROOT) && $(GO) build -o bin/golars-mcp ./cmd/golars-mcp
	@echo "  PATH will include $(BIN_DIR) during vhs runs"

out/%.gif: %.tape _common.tape fixtures/*
	@mkdir -p out
	@echo "  vhs  $<"
	@cd $(ROOT) && PATH="$(BIN_DIR):$$PATH" $(VHS) vhs/$<

gif-%: %.tape
	@mkdir -p out
	@echo "  vhs  $<"
	@cd $(ROOT) && PATH="$(BIN_DIR):$$PATH" $(VHS) vhs/$<

clean: ## Remove every generated gif
	rm -f out/*.gif

list: ## List every tape file
	@printf "  %s\n" $(TAPES)
