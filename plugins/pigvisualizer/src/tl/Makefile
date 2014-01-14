OBJ_DIR = obj
OBJ = \
	$(OBJ_DIR)/jobstats.js \
	$(OBJ_DIR)/jobdetails.js

all: $(OBJ)

$(OBJ_DIR)/%.js: %.tl
	mkdir -p $(OBJ_DIR) && dustc --name=$(basename $<) $< $@

clean:
	rm -rf $(OBJ_DIR)

.SUFFIXES: .tl .js

.PHONY: all clean
