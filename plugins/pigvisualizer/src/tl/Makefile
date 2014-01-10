OBJ = \
	jobstats.js \
	jobdetails.js

all: $(OBJ)

.tl.js:
	dustc --name=$(basename $<) $< $@

clean:
	rm -f $(OBJ)

.SUFFIXES: .tl .js

.PHONY: all clean
