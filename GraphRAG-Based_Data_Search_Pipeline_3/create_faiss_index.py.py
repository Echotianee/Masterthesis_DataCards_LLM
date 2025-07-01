#!/usr/bin/env python3
"""
Enhanced FAISS Indexer for Neo4j Knowledge Graph
Creates FAISS index with metadata and relationship context
"""

import os
import pickle
import logging
import faiss
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase
from langchain_community.embeddings import HuggingFaceEmbeddings

def create_enhanced_index():
    """Create enhanced FAISS index with Neo4j data and metadata"""
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    
    # Neo4j connection
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "YidanThesis")
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    # Setup embedding model
    print("🔧 Loading embedding model...")
    hf = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
    embed_model = hf.client
    
    # Create data folder
    project_root = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(project_root, "data_folder")
    os.makedirs(data_folder, exist_ok=True)
    
    print("📊 Extracting data from Neo4j...")
    
    with driver.session() as session:
        # Get all nodes with their properties
        node_query = """
        MATCH (n)
        RETURN 
            id(n) as node_id,
            labels(n) as labels,
            properties(n) as props,
            n.uri as uri,
            n.name as name,
            n.description as description
        """
        
        nodes_result = session.run(node_query)
        nodes_data = []
        
        for record in nodes_result:
            node_info = {
                'node_id': record['node_id'],
                'labels': record['labels'],
                'props': dict(record['props']) if record['props'] else {},
                'uri': record['uri'],
                'name': record['name'],
                'description': record['description']
            }
            nodes_data.append(node_info)
        
        print(f"   Retrieved {len(nodes_data)} nodes")
        
        # Get all relationships
        rel_query = """
        MATCH (a)-[r]->(b)
        RETURN 
            a.uri as source,
            type(r) as relationship_type,
            b.uri as target,
            properties(r) as rel_props,
            a.name as source_name,
            b.name as target_name
        """
        
        rels_result = session.run(rel_query)
        relationships_data = []
        
        for record in rels_result:
            rel_info = {
                'source': record['source'],
                'type': record['relationship_type'],
                'target': record['target'],
                'source_name': record['source_name'],
                'target_name': record['target_name'],
                'properties': dict(record['rel_props']) if record['rel_props'] else {}
            }
            relationships_data.append(rel_info)
        
        print(f"   Retrieved {len(relationships_data)} relationships")
    
    driver.close()
    
    # Create enriched passages for embedding
    print("🔤 Creating enriched text passages...")
    
    passages = []
    metadata = []
    
    for node in nodes_data:
        # Create rich text representation of each node
        text_parts = []
        
        # Basic info
        if node['name']:
            text_parts.append(f"Name: {node['name']}")
        
        if node['labels']:
            text_parts.append(f"Type: {', '.join(node['labels'])}")
        
        if node['description']:
            text_parts.append(f"Description: {node['description']}")
        
        # Add other properties
        for key, value in node['props'].items():
            if key not in ['name', 'description', 'uri'] and value:
                text_parts.append(f"{key}: {value}")
        
        # Add relationship context
        related_info = []
        for rel in relationships_data:
            if rel['source'] == node['uri']:
                target_name = rel['target_name'] or rel['target'].split('#')[-1]
                related_info.append(f"has {rel['type']} {target_name}")
            elif rel['target'] == node['uri']:
                source_name = rel['source_name'] or rel['source'].split('#')[-1]
                related_info.append(f"is {rel['type']} of {source_name}")
        
        if related_info:
            text_parts.append(f"Relationships: {'; '.join(related_info[:5])}")  # Limit to 5 relationships
        
        # Create final passage
        passage = ". ".join(text_parts)
        passages.append(passage)
        
        # Store metadata
        meta = {
            'node_id': node['node_id'],
            'uri': node['uri'],
            'label': node['name'] or (node['labels'][0] if node['labels'] else 'Unknown'),
            'labels': node['labels'],
            'description': node['description'],
            'properties': node['props']
        }
        metadata.append(meta)
    
    print(f"   Created {len(passages)} enriched passages")
    
    # Create embeddings
    print("🧮 Computing embeddings...")
    embeddings = []
    batch_size = 32
    
    for i in range(0, len(passages), batch_size):
        batch = passages[i:i + batch_size]
        batch_embeddings = embed_model.encode(batch, convert_to_numpy=True)
        embeddings.extend(batch_embeddings)
        print(f"   Processed {min(i + batch_size, len(passages))}/{len(passages)} passages")
    
    embeddings = np.array(embeddings)
    
    # Create FAISS index
    print("🗂️ Building FAISS index...")
    dimension = embeddings.shape[1]
    index = faiss.IndexFlatIP(dimension)  # Inner product for cosine similarity
    
    # Normalize embeddings for cosine similarity
    faiss.normalize_L2(embeddings)
    index.add(embeddings)
    
    print(f"   Index created with {index.ntotal} vectors")
    
    # Save everything
    print("💾 Saving index and data...")
    
    # Save FAISS index
    faiss_path = os.path.join(data_folder, "passages.index")
    faiss.write_index(index, faiss_path)
    
    # Save passages
    passages_path = os.path.join(data_folder, "passages.pkl")
    with open(passages_path, "wb") as f:
        pickle.dump(passages, f)
    
    # Save metadata
    metadata_path = os.path.join(data_folder, "metadata.pkl")
    with open(metadata_path, "wb") as f:
        pickle.dump(metadata, f)
    
    # Save relationships as DataFrame
    relationships_df = pd.DataFrame(relationships_data)
    relationships_path = os.path.join(data_folder, "relationships.pkl")
    with open(relationships_path, "wb") as f:
        pickle.dump(relationships_df, f)
    
    print("✅ Enhanced FAISS index created successfully!")
    print(f"   📁 Files saved in: {data_folder}")
    print(f"   📊 {len(passages)} passages indexed")
    print(f"   🔗 {len(relationships_data)} relationships saved")
    print(f"   📝 Metadata for all entities saved")
    
    # Show some sample passages
    print("\n📝 Sample enriched passages:")
    for i, passage in enumerate(passages[:3]):
        print(f"   {i+1}. {passage[:150]}...")
    
    return True

if __name__ == "__main__":
    create_enhanced_index()