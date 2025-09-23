# rdfstats_stream.py — Streaming RDF Statistics for Billion-Triple Graphs

A lightweight, single-file Python tool for **streaming analysis of very large RDF graphs** (N-Triples), designed to work in **bounded memory** with **external sorting**. It produces compact CSV summaries (predicate distributions, per-predicate cardinalities and degrees), **co-occurrence matrices**, **association rules**, optional **graph centralities**, and **focused telemetry** for a user-defined set of “interesting” predicates (with basic aliasing, datatype/lang counts, and Top-N object IRIs). Optional PNG plots are generated if `matplotlib` is available.

> **Why this tool?** When your data is tens of gigabytes to billions of triples, loading into a triplestore just to compute descriptive statistics is impractical. This script streams, sorts externally on disk, and aggregates efficiently—so you can **measure before you model**.

➡️ [View the demo](https://domel.github.io/rdfstats_stream/)

---

## Features at a Glance

- **Streaming ingestion** of `.nt`, `.nt.gz`, `.nt.bz2`, `.nt.xz` (line-by-line).
- **Predicate distribution** with Shannon entropy (bits) and Gini index.
- **Per-predicate structure**: unique subjects/objects; average in/out degrees; % of subjects using a predicate.
- **Co-occurrences across subjects** (Top-K predicates) with **support, Jaccard, PMI, confidence, lift**.
- **Association rules** `A → B` filtered by minimum support/confidence.
- **Optional graph metrics** (NetworkX): degree centrality, weighted degree, PageRank on the predicate co-occurrence graph.
- **Focused analysis for “interesting” predicates** (CURIEs or IRIs), with **HTTP↔HTTPS aliasing**:
  - Summary table (aggregated over aliases),
  - Datatype and language distributions for literals,
  - Top-N object IRIs,
  - Co-occurrence and rules **restricted to interesting antecedents**.
- **PNG plots** (headless backend): histogram/bar of predicate counts, heatmaps of co-occurrence.
- **External sorting** with configurable chunk size and temporary directory.

---

## Installation

The script depends only on the Python standard library. Some features are optional:

- **Required:** Python 3.8+ (tested on 3.10+)
- **Optional (for plots):** `matplotlib`, `numpy`
- **Optional (for centralities):** `networkx`

Install optional packages if needed:
```bash
pip install matplotlib numpy networkx
```

---

## Quick Start

```bash
python rdfstats_stream.py   --input bigdata.nt.gz   --outdir ./results   --plots --plots-topN 30 --plots-dpi 150 --plots-curie-labels
```

Outputs CSVs and PNGs under `./results`. A final line prints:
```
[OK] Done. Results saved to: ./results
```

---

## Input Format

- **N-Triples only** (no named graphs). Objects may be IRIs, blank nodes, or literals (with `^^<datatype>` or `@lang`).
- Compressed inputs (`.gz`, `.bz2`, `.xz/.lzma`) are transparently streamed.

> Non-conforming lines are skipped with a warning. The N-Triples parser is tolerant (regex-based for S, P, and the rest as O).

---

## Command-Line Options

| Option | Default | Description |
|---|---:|---|
| `--input PATH` | — | **Required.** Path to `.nt` (optionally `.gz/.bz2/.xz`). |
| `--outdir DIR` | — | **Required.** Directory to write CSV/PNG outputs. |
| `--tmpdir DIR` | system tmp | Directory for external sort chunks (use a fast disk). |
| `--chunk-lines N` | `500000` | Lines per chunk before sorting to disk (memory/IO trade-off). |
| `--topk K` | `200` | Top-K predicates considered for co-occurrences (by global frequency). Aliases from `--predicates-file` are always included. |
| `--max-preds-per-subject N` | `50` | Limits pair generation per subject (prevents O(m²) blowups). When exceeded, only the first `N` sorted predicates are kept (note possible bias). |
| `--min-support S` | `0.01` | Minimum support for rules (fraction of subjects). |
| `--min-conf C` | `0.5` | Minimum confidence for rules. |
| `--unique-so` | off | Also compute **unique (s,o) pairs per predicate** (slower; needs extra sorting). |
| `--duplicate-excess` | off | Also compute **redundant duplicates**: sum over all (s,o) multiplicities minus 1 (slower). |
| `--predicates-file FILE` | — | One IRI/CURIE per line to mark **interesting predicates**. Lines may be `<IRI>`, `IRI` (auto-wrapped), or `prefix:local` (expanded via `--prefixes`). For interesting predicates, **HTTP↔HTTPS aliases** are merged. |
| `--top-values N` | `50` | Top-N **object IRIs** per interesting predicate (11\_*.csv). |
| `--prefixes FILE` | — | JSON map of prefixes (merged with built-ins). Used to expand CURIEs and for pretty plot labels. Example below. |
| `--plots` | off | Generate PNG plots in `--outdir`. |
| `--plots-topN N` | `30` | Number of predicates shown in Top-N plots / heatmaps. |
| `--plots-dpi D` | `150` | PNG DPI. |
| `--plots-curie-labels` | off | Prefer CURIE labels on plot axes when possible. |

### Built-in Prefixes
```json
{
  "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
  "xsd": "http://www.w3.org/2001/XMLSchema#",
  "owl": "http://www.w3.org/2002/07/owl#",
  "skos": "http://www.w3.org/2004/02/skos/core#",
  "schema": "http://schema.org/",
  "foaf": "http://xmlns.com/foaf/0.1/",
  "dc": "http://purl.org/dc/elements/1.1/",
  "dcterms": "http://purl.org/dc/terms/",
  "wikidata": "http://www.wikidata.org/prop/direct/"
}
```

You can extend/override via `--prefixes myprefixes.json`.

---

## “Interesting Predicates” File

A plain text file with one token per line:

- `<http://schema.org/name>`
- `schema:name`
- `rdf:type`
- `<https://www.w3.org/2000/01/rdf-schema#label>`

Blank lines and `#` comments are allowed. For each entry, the tool builds **HTTP/HTTPS aliases** so `<http://…>` and `<https://…>` aggregate to a single **display token** (always stored as `<IRI>`).

> All “interesting” outputs (files **8–13**) are reported **by display token** after aliasing.

---

## Outputs

All CSVs are written to `--outdir`. Columns are documented here for reproducibility.

### Global Predicate Distribution

1. **`1_predicate_distribution.csv`**  
   `predicate, count, share`  
   - `share = count / N_triples`.

2. **`1_meta_entropy_gini.csv`**  
   `N_triples, N_predicates, entropy_bits, gini`  
   - `entropy_bits = -Σ p_i log2 p_i`, where `p_i` is the share of predicate *i*.  
   - `gini = 1 - Σ p_i²` (0 = uniform, →1 = concentrated).

### Per-Predicate Structure (Global, no aliasing)

3. **`2_per_predicate_basic.csv`**  
   `predicate, count, share, unique_subjects, unique_objects, avg_out_degree_p, avg_in_degree_p, pct_subjects_with_p, unique_s_o_pairs, duplicate_excess`  
   - `avg_out_degree_p`: average multiplicity **per subject** of edges with this predicate.  
   - `avg_in_degree_p`: average multiplicity **per object** of edges with this predicate.  
   - `pct_subjects_with_p = 100 * unique_subjects / N_subjects_all`.  
   - `unique_s_o_pairs` and `duplicate_excess` are filled if `--unique-so` / `--duplicate-excess` were computed **globally**. If `--predicates-file` is used, these are instead reported in file **8** for interesting predicates.

### Co-Occurrences & Rules (Top-K by frequency, no aliasing)

4. **`3_predicate_cooccurrence.csv`**  
   `p_a, p_b, n_ab, n_a, n_b, support_ab, jaccard, pmi, conf(A->B), conf(B->A), lift(A->B), lift(B->A)`  
   - Computed across **subjects**: a subject contributes 1 to the set of its **distinct** predicates.  
   - `n_a`: #subjects using `p_a`. `n_b`: #subjects using `p_b`. `n_ab`: #subjects using both.  
   - `support_ab = n_ab / N_subjects_all`.  
   - `jaccard = n_ab / (n_a + n_b − n_ab)`.  
   - `conf(A→B) = n_ab / n_a`, `conf(B→A) = n_ab / n_b`.  
   - `lift(A→B) = conf(A→B) / (n_b / N_subjects_all)`.  
   - `PMI = log2( support_ab / ((n_a/N)*(n_b/N)) )`.

5. **`4_association_rules.csv`**  
   `p_a, p_b, support_ab, conf(A->B), lift(A->B), jaccard, pmi`  
   - Contains only pairs with `support_ab ≥ --min-support` and `conf(A->B) ≥ --min-conf`.

### Graph Metrics (Top-K, no aliasing)

6. **`5_per_predicate_graph_metrics.csv`** *(if `networkx` available)*  
   `predicate, deg_centrality, weighted_degree, pagerank`  
   - Graph over Top-K predicates; edge weights are co-occurrence counts `n_ab`.

### Optional Global (slow; requires extra sorting)

7. **`6_unique_so_per_predicate.csv`** *(if `--unique-so`)*  
8. **`7_duplicate_excess_per_predicate.csv`** *(if `--duplicate-excess`)*

### “Interesting” Predicate Views (after aliasing HTTP↔HTTPS; **by display token `<IRI>`**)

9. **`8_interesting_predicates_summary.csv`**  
   `predicate, count, share, unique_subjects, unique_objects, avg_out_degree_p, std_out_degree_p, min_out_degree_p, max_out_degree_p, avg_in_degree_p, std_in_degree_p, min_in_degree_p, max_in_degree_p, pct_subjects_with_p, unique_s_o_pairs, duplicate_excess, subj_IRI, subj_BNODE, obj_IRI, obj_BNODE, obj_LITERAL, literals_total, unique_datatypes, unique_langs`

10. **`9_interesting_predicates_datatypes.csv`**  
    `predicate, datatype, count, share_among_literals`

11. **`10_interesting_predicates_langs.csv`**  
    `predicate, lang, count, share_among_literals`

12. **`11_interesting_predicates_top_objects.csv`**  
    `predicate, object_iri, count, share_among_predicate`  
    - Top-N object IRIs per interesting predicate (`--top-values`).

13. **`12_interesting_predicates_cooccurrence.csv`**  
    As in (4), but pairs are aggregated **after aliasing**; only rows with at least one interesting predicate are kept.

14. **`13_interesting_predicates_rules.csv`**  
    As in (5), but **only rules with interesting antecedent** `A` are retained.

---

## Plots (if `--plots`)

- `plot_1a_predicate_count_histogram.png` — Histogram of predicate edge counts.  
- `plot_1b_top_predicates_bar_topN.png` — Bar chart of Top-N predicates by count.  
- `plot_3_cooccurrence_heatmap_topN.png` — Heatmap of `n_ab` with diagonal `n_a` for Top-N by support (subjects).  
- `plot_12_interesting_cooccurrence_heatmap.png` — Heatmap restricted to interesting predicates (after aliasing).

> Uses a headless backend (`Agg`), so plots render without a GUI and are saved directly to files.

---

## Examples

### 1) Minimal run (global statistics only)
```bash
python rdfstats_stream.py   --input data.nt.gz   --outdir ./results
```

### 2) Top-K co-occurrences with rules and plots
```bash
python rdfstats_stream.py   --input data.nt.gz   --outdir ./results   --topk 300 --min-support 0.005 --min-conf 0.6   --plots --plots-topN 40 --plots-curie-labels
```

### 3) Focus on interesting predicates with aliasing and object Top-N
`interesting.txt`:
```
# Labels and typing
rdfs:label
rdf:type
schema:name
<http://schema.org/identifier>
```

`prefixes.json`:
```json
{
  "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
  "rdf":  "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  "schema": "http://schema.org/"
}
```

Run:
```bash
python rdfstats_stream.py   --input data.nt.xz   --outdir ./results   --predicates-file interesting.txt   --top-values 100   --prefixes prefixes.json   --plots --plots-topN 30
```

### 4) Unique (s,o) and duplicate excess (global)
```bash
python rdfstats_stream.py   --input data.nt   --outdir ./results   --unique-so --duplicate-excess
```

---

## Performance & Scaling Tips

- **External sorting**: Tune `--chunk-lines` to match RAM/IO. Larger chunks = fewer temporary files but more RAM.
- **Disk**: Use a **fast SSD** for `--tmpdir`. Ensure enough free space for chunk files.
- **Top-K**: Co-occurrence cost grows with `K` and the per-subject predicate diversity. Reduce `--topk` if needed.
- **Per-subject cap**: `--max-preds-per-subject` (default 50) prevents quadratic blowups; increasing it yields more complete co-occurrences at a higher cost.
- **Parallelism**: The script is single-process; you can run multiple instances on disjoint files if your disk allows.
- **Headless plotting**: Plots render with `matplotlib`’s `Agg` backend; no GUI required.

---

## Semantics & Formulas (Reference)

- Let `N` be the number of **distinct subjects** observed.
- For predicate `p`:  
  `n_p` = #subjects that use `p` at least once.  
  For a pair `(a, b)`: `n_ab` = #subjects that use both `a` and `b`.

**Support:** `support_ab = n_ab / N`  
**Jaccard:** `n_ab / (n_a + n_b − n_ab)`  
**Confidence:** `conf(A→B) = n_ab / n_a`  
**Lift:** `lift(A→B) = conf(A→B) / (n_b / N)`  
**PMI:** `log2( (n_ab/N) / ((n_a/N)*(n_b/N)) )`

**Predicate share** (in all triples): `count(p) / N_triples`  
**Entropy (bits):** `−Σ p_i log2 p_i` over predicate shares `p_i`  
**Gini:** `1 − Σ p_i²`

---

## Caveats & Limitations

- **Format:** N-Triples only (no N-Quads / named graphs).  
- **Co-occurrence scope:** Across **subjects** only; co-occurrences across objects are not computed.  
- **Truncation bias:** If a subject has more than `--max-preds-per-subject` distinct predicates, only the first `N` (sorted lexicographically) are kept for pair generation—this can bias co-occurrence statistics on very dense subjects.  
- **Aliasing:** Only **HTTP↔HTTPS** variants are merged for “interesting” predicates. Other IRI variants (e.g., trailing slashes, `www.`) are not normalized.  
- **Datatype/lang detection:** Regex-based; covers standard cases (`"..."@lang`, `"..."^^<dt>`). Exotic lexical forms may be missed.  
- **Centralities:** Computed on the **Top-K predicate co-occurrence** graph only; not meaningful if `K` is very small or the graph is extremely sparse.

---

## Reproducibility Checklist

- Record: command line; script version (header shows `Version: 1.3`); Python version; `--chunk-lines`, `--topk`, `--max-preds-per-subject`; presence/versions of `matplotlib`, `numpy`, `networkx`; the exact `--prefixes` and `--predicates-file` contents; and the dataset hash/path.

---

## Acknowledgements & Citation

If you use this tool in academic work, please cite the accompanying paper (if available) or acknowledge **rdfstats_stream.py** by Dominik Tomaszuk. A BibTeX stub you can adapt:

```bibtex
@misc{rdfstats_stream,
  author       = {Dominik Tomaszuk},
  title        = {rdfstats\_stream.py: Streaming RDF Statistics for Billion-Triple Graphs},
  howpublished = {\url{https://github.com/domel/rdfstats_stream}},
  note         = {Version 1.3},
  year         = {2025}
}
```

---

## License

MIT License
