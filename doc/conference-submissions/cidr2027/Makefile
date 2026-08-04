LATEXMK ?= latexmk
PYTHON ?= python3
RSVG_CONVERT ?= rsvg-convert
MAIN := paper
FIGURE_SVGS := figures/query-graph.svg figures/architecture.svg figures/tail-ccdf.svg
FIGURE_PDFS := $(FIGURE_SVGS:.svg=.pdf)
FIGURE_DATA := \
	../../../benchmarks/JOB-bench/results/cidr-confirmatory-sampled-policies-20261001/optimizer_slowdowns_1785324090644.csv \
	../../../benchmarks/JOB-bench/results/cidr-confirmatory-no-sampling-schedules-20261101/optimizer_slowdowns_1785332556151.csv

.PHONY: all clean figures

all: $(MAIN).pdf

figures: $(FIGURE_PDFS)

$(FIGURE_SVGS): scripts/generate_figures.py $(FIGURE_DATA)
	$(PYTHON) scripts/generate_figures.py

figures/%.pdf: figures/%.svg
	$(RSVG_CONVERT) -f pdf1.5 -o $@ $<

$(MAIN).pdf: $(MAIN).tex references.bib cidr-2027.cls $(FIGURE_PDFS)
	$(LATEXMK) -pdf -interaction=nonstopmode -halt-on-error $(MAIN).tex

clean:
	$(LATEXMK) -C $(MAIN).tex
