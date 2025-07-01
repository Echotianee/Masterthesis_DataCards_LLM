

# Datacards-to-KG_Pipeline_1

This pipeline is part of a master's thesis project that uses large language models (LLMs) to extract semantic triples from dataset cards and populate a knowledge graph based on the OntoDM ontology. It supports different prompting strategies, including cleaned metadata, scraped raw text, and few-shot examples, using the Gemini language model.

---

##  Overview

The pipeline processes dataset cards in three main stages:

1. **Cleaning**: Preprocess metadata into a consistent format.
2. **Triple Extraction**: Use Gemini to extract RDF-style triples aligned to OntoDM.
3. **Export**: Output triples for downstream ingestion into a knowledge graph (e.g., Neo4j, GraphDB).

---

## File Descriptions

| File | Description |
|------|-------------|
| `data_cleaning.py` | Cleans raw or scraped dataset card metadata into a normalized format. |
| `gemini_ontodm_cleandata_prompt_1.py` | Sends cleaned metadata to Gemini and extracts OntoDM triples (version 1). |
| `gemini_ontodm_cleandata_prompt_2.py` | Alternative cleaned prompt strategy (version 2). |
| `gemini_ontodm_scraped_3.py` | Uses uncleaned scraped text with a generic prompt for triple extraction. |
| `gemini_ontodm_scraped_with_few_shots_4.py` | Applies few-shot prompting to improve triple extraction from raw text. |

---


#  NL_to_SPARQL_Pipeline_2

This pipeline translates natural language questions into executable SPARQL queries using Gemini-based prompting and evaluates the quality of those queries over RDF data stored in GraphDB. It enables semantic querying of knowledge graphs generated from structured metadata such as data cards.

---

## Contents

```bash
NL_to_SPARQL_Pipeline_2/
│
├── sql_graphdb.py                    # Core script for NL → SPARQL generation + execution
├── sql_graphdb2(groundtruth).py     # Script for comparing SPARQL responses to gold-standard answers
├── .env                              # (User-created) Gemini API Key + GraphDB connection info

#  GraphRAG-Based_Data_Search_Pipeline_3

This pipeline implements a **hybrid symbolic-neural data discovery system** inspired by the GraphRAG architecture. It enhances natural language question answering over RDF data by combining a **Neo4j knowledge graph** backend with a **FAISS-based semantic retriever**, enabling both precise symbolic execution and fuzzy neural retrieval over metadata-enriched datasets.
