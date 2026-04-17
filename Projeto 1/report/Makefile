TEX = pdflatex
FLAGS = -output-directory=$(OUTPUT_DIR) -shell-restricted
OUTPUT_DIR = output
FILE_NAME = main

.PHONY: all check dir index compile install clean

all: check dir compile

# check:
# 	[ ! -f $(FILE_NAME).tex ] || { echo "$(FILE_NAME).tex not found. Exiting..."; exit 1; }

dir:
	mkdir -p $(OUTPUT_DIR)

index:
	# Manual option:
	if [ -f $(OUTPUT_DIR)/$(FILE_NAME).nlo ]; then \
		makeindex $(OUTPUT_DIR)/$(FILE_NAME).nlo -s nomencl.ist -o $(OUTPUT_DIR)/$(FILE_NAME).nls; \
	else \
		echo "No .nlo file found. Skipping indexing."; \
	fi

	# Dynamically handle .nlo files for indexing
	# find $(OUTPUT_DIR) -name "*.nlo" -exec makeindex {} -s nomencl.ist -o {}.nls \; 2>/dev/null \
	# 	|| echo "No .nlo files found. Skipping indexing."

compile: index
	$(TEX) $(FLAGS) $(FILE_NAME).tex

install:
	# Install the required packages
	sudo apt update -y
	sudo apt install -y \
		texlive-xetex \
		texlive-latex-recommended \
		ttf-mscorefonts-installer \
		texlive-fonts-extra \
		texlive-publishers \
		fonts-stix
	# Check if font is installed
	# sudo fc-cache -f && fc-match $(FONT_NAME) \
	# 	|| echo "$(FONT_NAME) font not found"

clean:
	rm -rf \
		$(OUTPUT_DIR)/*.log \
		$(OUTPUT_DIR)/*.aux \
		$(OUTPUT_DIR)/*.lof \
		$(OUTPUT_DIR)/*.tof \
		$(OUTPUT_DIR)/*.tot \
		$(OUTPUT_DIR)/*.tol \
		$(OUTPUT_DIR)/*.toc \
		$(OUTPUT_DIR)/*.out \
		$(OUTPUT_DIR)/*.ilg \
		$(OUTPUT_DIR)/*.nls \
		$(OUTPUT_DIR)/*.nlo \
		$(OUTPUT_DIR)/*.fdb_latexmk \
		$(OUTPUT_DIR)/*.fls
