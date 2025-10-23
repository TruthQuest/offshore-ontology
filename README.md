# Learning RDF with the Panama Papers  
### Semantic Modeling, Validation, and Graph-Based Analysis

![Money Laundering Network Visualization](https://github.com/TruthQuest/offshore-ontology/blob/main/graphics/money_launder.png)

---

## Overview

This repository demonstrates how to construct and analyze a large-scale RDF knowledge graph from the **Panama Papers dataset**.  
It focuses on translating raw CSV disclosures into a structured, queryable graph using standard semantic web technologies such as RDF, OWL, and SHACL.

The project shows how semantic modeling can clarify and verify relationships in complex networks - specifically, entities, officers, and intermediaries involved in offshore corporate structures.

---

## Project Structure

| File | Description |
|------|--------------|
| **`panama_papers_001_build_rdf_basic.py`** | Builds RDF triples from the raw ICIJ data files. Assigns URIs, maps relationships, and adds provenance metadata. |
| **`panama_papers_002_convert_nt_ttl.py`** | Converts large N-Triples files to Turtle format efficiently, handling multi-gigabyte datasets. |
| **`panama_papers_003_rdf_analysis.py`** | Performs validation, graph traversal, relationship extraction, and identification of ownership chains and intermediaries. |
| **`panama_papers_004_find_overlap_sanctions_panama_papers.py`** | Cross-references entities in the RDF graph against known international sanctions lists. |
| **`learning_rdf_with_panama_papers.html`** | Documentation and narrative explanation of the semantic model and analysis results. |

---

## Methodology

### 1. Data Conversion and Normalization  
Converts ICIJ CSV data into RDF triples with consistent URIs and namespaces.  
Standardizes entity and relationship labels, types, and identifiers.

### 2. Semantic Validation  
Applies SHACL-style rules to verify structural consistency.  
Detects invalid triples, missing relationships, and ontology mismatches.

### 3. Graph Construction  
Builds two directed graphs using `networkx`:  
- **Ownership graph** - captures only ownership and shareholder relationships.  
- **Full relationship graph** - includes all known associations between entities, officers, and intermediaries.  

### 4. Analysis  
Implements core graph-based analyses:  
- Trace multi-level ownership chains.  
- Identify circular ownership or feedback loops.  
- Detect high-degree nodes functioning as intermediaries.  
- Calculate degree centrality and connectivity statistics.

### 5. Sanctions Cross-Check  
Compares entities extracted from the RDF graph against known sanctions datasets.  
Identifies intersections and potential compliance risks.

---

## Example Workflow
```bash
# Step 1: Generate RDF triples from raw data
python panama_papers_001_build_rdf_basic.py

# Step 2: Convert N-Triples to Turtle format
python panama_papers_002_convert_nt_ttl.py panama_papers.nt panama_papers.ttl

# Step 3: Run validation and graph analysis
python panama_papers_003_rdf_analysis.py

# Step 4: Identify entities overlapping with sanctions data
python panama_papers_004_find_overlap_sanctions_panama_papers.py
```

Each stage logs progress, validation outcomes, and high-level statistics.

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **RDF (Resource Description Framework)** | A graph-based model representing entities and relationships as subject-predicate-object triples. |
| **OWL (Web Ontology Language)** | Provides a formal schema and logical semantics for defining entity classes and properties. |
| **SHACL (Shapes Constraint Language)** | Enforces structural rules and data integrity constraints for RDF data. |
| **Graph Analysis** | Uses network-based methods to identify key entities, ownership depth, and connectivity patterns. |

---

## Technical Stack

| Component | Technology |
|-----------|------------|
| **Language** | Python 3.10+ |
| **Core Libraries** | rdflib, networkx, pandas, tqdm |
| **Data Formats** | RDF (N-Triples, Turtle), CSV, JSON |
| **Ontologies** | FOAF, PROV-O |
| **Processing** | Chunked streaming and memory-safe graph construction |

---

## Design Goals

- **Transparency** - All relationships are explicit, typed, and traceable to source records.
- **Reproducibility** - Deterministic RDF generation and consistent validation rules.
- **Performance** - Streaming reads, chunked parsing, and efficient in-memory processing.
- **Interpretability** - Readable schema design and documented relationship mapping.

---

## System Requirements and Runtime Performance

| Resource | Recommended Minimum |
|----------|---------------------|
| **Memory** | 16 GB RAM |
| **Processor** | Quad-core CPU |
| **Storage** | ~10 GB free disk space |
| **Runtime** | 45-90 minutes on standard hardware for full dataset |

The pipeline is optimized for linear scalability with respect to dataset size.  
All scripts include progress indicators, error handling, and logging output suitable for long-running batch jobs.

---

## License

© 2025 Eric Brattin. All rights reserved.  
This repository is provided for educational and demonstration purposes only.  
Reproduction, redistribution, or modification without written consent is prohibited.

---

## Author

**Eric Brattin**  
Managing Consultant
Washington, D.C.  
[LinkedIn](https://www.linkedin.com/in/ericbrattin/)

---

## Summary

This project demonstrates how semantic graph modeling can transform complex financial disclosures into structured, verifiable data assets.  
It combines data engineering, graph analysis, and metadata governance practices to illustrate how large-scale information can be made explainable, auditable, and machine-readable.
