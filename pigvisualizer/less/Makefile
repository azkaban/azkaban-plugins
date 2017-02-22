LESSC = lessc
OBJ_DIR = obj
OBJ = \
	$(OBJ_DIR)/pigvisualizer.css

all: $(OBJ)

$(OBJ_DIR)/pigvisualizer.css: pigvisualizer.less
	$(LESSC) $< $@

clean:
	rm -rf $(OBJ_DIR)

.PHONY: all clean
