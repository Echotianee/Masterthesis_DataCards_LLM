# KG_RAG.py

import os
import re
import logging
import pickle
import faiss
import numpy as np
import pandas as pd
from types import SimpleNamespace

from dotenv import load_dotenv
from neo4j import GraphDatabase, exceptions as neo4j_exceptions
from langchain_community.embeddings import HuggingFaceEmbeddings

from KG_query import generate_cypher
from csv_to_neo4j_loader import load_csv_to_neo4j

def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO)

    # — Load Neo4j credentials —
    uri  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pw   = os.getenv("NEO4J_PASSWORD", "Eic201709")
    load_csv_to_neo4j()

    # — Attempt a quick auth check —
    try:
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        # try opening & closing a session right away
        with driver.session() as session:
            session.run("RETURN 1")
    except neo4j_exceptions.AuthError:
        print(f"❌ Authentication failed when connecting to Neo4j at {uri}")
        print("   Please verify NEO4J_USER and NEO4J_PASSWORD in your .env")
        return
    except Exception as e:
        print(f"❌ Could not connect to Neo4j at {uri}: {e}")
        return
    
    project_root = os.path.dirname(os.path.abspath(__file__))

    # — Embedding model setup —
    hf = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
    embed_model = hf.client

    # — Load enhanced FAISS index and data —
    data_folder = os.path.join(project_root, "data_folder")
    
    # Updated path to match the new indexer output
    faiss_idx_path = os.path.join(data_folder, "passages.index")
    passages_path = os.path.join(data_folder, "passages.pkl")
    metadata_path = os.path.join(data_folder, "metadata.pkl")
    relationships_path = os.path.join(data_folder, "relationships.pkl")

    # Check if files exist
    if not os.path.exists(faiss_idx_path):
        print(f"❌ FAISS index not found at {faiss_idx_path}")
        print("   Please run the enhanced indexer first.")
        return

    logging.info("Loading enhanced FAISS index from %s", faiss_idx_path)
    index = faiss.read_index(faiss_idx_path)

    logging.info("Loading enriched passages from %s", passages_path)
    with open(passages_path, "rb") as f:
        texts = pickle.load(f)

    # Load metadata if available
    metadata = []
    if os.path.exists(metadata_path):
        logging.info("Loading passage metadata from %s", metadata_path)
        with open(metadata_path, "rb") as f:
            metadata = pickle.load(f)
    
    # Load relationships if available
    relationships_df = pd.DataFrame()
    if os.path.exists(relationships_path):
        logging.info("Loading relationships from %s", relationships_path)
        with open(relationships_path, "rb") as f:
            relationships_df = pickle.load(f)

    # — Enhanced retriever with metadata —
    def retrieve_docs(query: str, k: int = 5):
        """Enhanced retrieval with metadata and relationship context"""
        vec = embed_model.encode(query, convert_to_numpy=True)
        D, I = index.search(np.array([vec]), k)
        
        retrieved_docs = []
        for idx, score in zip(I[0], D[0]):
            doc_content = texts[idx]
            
            # Create enhanced document with metadata if available
            if metadata and idx < len(metadata):
                meta = metadata[idx]
                doc = SimpleNamespace(
                    page_content=doc_content,
                    metadata=meta,
                    similarity_score=float(score),
                    uri=meta.get('uri', ''),
                    label=meta.get('label', ''),
                    description=meta.get('description', '')
                )
            else:
                doc = SimpleNamespace(
                    page_content=doc_content,
                    similarity_score=float(score)
                )
            
            retrieved_docs.append(doc)
        
        return retrieved_docs

    def format_context_for_cypher(docs):
        """Format retrieved documents into context for Cypher generation"""
        context_parts = []
        
        for i, doc in enumerate(docs, 1):
            # Basic content
            context_parts.append(f"Document {i}: {doc.page_content}")
            
            # Add metadata if available
            if hasattr(doc, 'metadata') and doc.metadata:
                if doc.uri:
                    context_parts.append(f"  URI: {doc.uri}")
                if doc.label:
                    context_parts.append(f"  Label: {doc.label}")
            
            context_parts.append("")  # Empty line for separation
        
        return "\n".join(context_parts)

    def get_related_entities(query, docs):
        """Extract related entities and relationships for better context"""
        related_info = []
        
        for doc in docs:
            if hasattr(doc, 'uri') and doc.uri and not relationships_df.empty:
                # Find relationships involving this entity
                related_rels = relationships_df[
                    (relationships_df['source'] == doc.uri) | 
                    (relationships_df['target'] == doc.uri)
                ]
                
                if not related_rels.empty:
                    rel_summary = []
                    for _, rel in related_rels.head(3).iterrows():  # Limit to 3 most relevant
                        if rel['source'] == doc.uri:
                            rel_summary.append(f"{doc.label or 'Entity'} --{rel['type']}--> {rel['target'].split('#')[-1]}")
                        else:
                            rel_summary.append(f"{rel['source'].split('#')[-1]} --{rel['type']}--> {doc.label or 'Entity'}")
                    
                    if rel_summary:
                        related_info.append(f"Related to {doc.label or doc.uri}: {'; '.join(rel_summary)}")
        
        return related_info

    print("🔍 Enhanced RAG CLI with Relationship Context (type 'exit' to quit)")
    print(f"📊 Loaded {len(texts)} enriched passages with relationship context")
    if not relationships_df.empty:
        print(f"🔗 {len(relationships_df)} relationships available for context")
    print()

    while True:
        question = input("> ").strip()
        if question.lower() in ("exit", "quit"):
            break

        # 1) Enhanced retrieval
        docs = retrieve_docs(question, k=5)
        logging.info("Retrieved %d enriched passages", len(docs))
        
        # Format context for Cypher generation
        context = format_context_for_cypher(docs)
        
        # Get additional relationship context
        related_entities = get_related_entities(question, docs)
        if related_entities:
            context += "\n\nRelated Entities and Relationships:\n" + "\n".join(related_entities)

        # Show retrieved context (optional - for debugging)
        print(f"\n📋 Retrieved Context Summary:")
        for i, doc in enumerate(docs[:3], 1):  # Show top 3
            score = getattr(doc, 'similarity_score', 0)
            label = getattr(doc, 'label', 'Unknown')
            print(f"  {i}. {label} (similarity: {score:.3f})")
            # Show first 100 chars of content
            content_preview = doc.page_content[:100] + "..." if len(doc.page_content) > 100 else doc.page_content
            print(f"     {content_preview}")
        print()

        # 2) Generate Cypher with enhanced context
        cypher = generate_cypher(question, driver, context)

        print(f"⟶ Generated Cypher:\n{cypher}\n")

        # 3) Extract parameters (enhanced parameter extraction)
        params = {}
        
        # Existing parameter extraction
        if "$name" in cypher:
            m = re.search(r"'(.+?)'", question)
            if m:
                params["name"] = m.group(1)
        
        if "$license" in cypher:
            m = re.search(r"license (?:is|=) '(.+?)'", question, re.IGNORECASE)
            if m:
                params["license"] = m.group(1)
        
        # Enhanced parameter extraction based on retrieved entities
        for doc in docs:
            if hasattr(doc, 'label') and doc.label:
                # If the question mentions the label, it might be a parameter
                if doc.label.lower() in question.lower():
                    # This could be enhanced based on your specific use cases
                    pass

        # 4) Execute Cypher
        try:
            with driver.session() as session:
                result = session.run(cypher, params)
                records = list(result)
        except neo4j_exceptions.ClientError as e:
            print("❌ Cypher execution failed:", e)
            print("   This might be due to schema differences or parameter issues.")
            continue
        except Exception as e:
            print("❌ Unexpected error:", e)
            continue

        # 5) Enhanced result display
        if not records:
            print("No results found.\n")
        else:
            print("✅ Results:")
            for i, rec in enumerate(records, 1):
                if len(rec.values()) == 1:
                    print(f"  {i}. {next(iter(rec.values()))}")
                else:
                    # Multiple columns - show all
                    values = list(rec.values())
                    print(f"  {i}. {' | '.join(str(v) for v in values)}")
            print(f"\nFound {len(records)} result(s).\n")

    driver.close()
    print("👋 Goodbye!")

if __name__ == "__main__":
    main()