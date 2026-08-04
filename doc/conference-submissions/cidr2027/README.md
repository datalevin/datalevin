# CIDR 2027 LaTeX paper

This directory uses the official `cidr-2027.cls` published by the
[CIDR 2027 call for papers](https://www.cidrdb.org/cidr2027/cfp.html).
The upstream class has SHA-256:

```text
f5fb0613b16d34828b94a3d26a7f60baa3103e31605f2ae65a08a06d2f623480
```

The vendored copy swaps the adjacent `hyperref` and `hyperxmp` package-load
lines so that the 2021 class works with TeX Live 2024. No layout or CIDR
metadata is changed. The patched file has SHA-256:

```text
2dc599c9ba3d035bf1d60e49762f720eee81989796e97adb6e3eab40621bb974
```

Build the paper with:

```bash
make
```

CIDR 2027 is single-blind and limits submissions to six pages total,
including references and appendices.

`paper.tex` is now the paper-writing source of truth. `../../cidr2027.md`
remains useful as the longer experiment narrative, but changes are not
automatically synchronized. The current PDF is six pages including references
and contains three vector figures.

`scripts/generate_figures.py` regenerates the SVG sources from the accepted
experiment CSVs under `benchmarks/JOB-bench/results`. It uses only the Python
standard library and validates the expected trial counts, timeout counts, and
fixed-case values before writing a figure. `make` converts the SVGs to PDF 1.5
with `rsvg-convert` before compiling the paper; `make figures` rebuilds only
the figure PDFs.

The frozen experiment inputs and accepted outputs are described in the
[CIDR 2027 artifact README](../../../benchmarks/JOB-bench/artifacts/cidr2027/README.md).
