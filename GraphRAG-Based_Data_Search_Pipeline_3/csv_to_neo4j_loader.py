# csv_to_neo4j_loader.py

import os
import glob
import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase
from urllib.parse import urlparse
import re

def clean_label(uri_or_label):
    """Extract clean label from URI or return cleaned label"""
    if pd.isna(uri_or_label):
        return ""
    
    uri_str = str(uri_or_label)
    
    # If it's a URI, extract the fragment or last part
    if uri_str.startswith('http'):
        # Try to get fragment first (after #)
        if '#' in uri_str:
            return uri_str.split('#')[-1]
        # Otherwise get last part of path
        else:
            return uri_str.split('/')[-1]
    
    # If it's already a clean label, return as is
    return uri_str

def get_node_label_from_relationships(node_uri, rels_df):
    """Determine node label based on 'type' relationships"""
    # Find relationships where this node is the source and relationship type is 'type'
    type_rels = rels_df[
        (rels_df['source'] == node_uri) & 
        (rels_df['type'] == 'type')
    ]
    
    if not type_rels.empty:
        # Get the target of the type relationship
        target = type_rels.iloc[0]['target']
        return clean_label(target)
    
    # Default fallback
    return "Entity"

def load_csv_to_neo4j():
    """Load CSV data into Neo4j database"""
    load_dotenv()
    
    # Neo4j connection
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "YidanThesis")
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    # File paths
    file_root = os.path.dirname(os.path.abspath(__file__))
    csv_dir = os.path.join(file_root, "ttl_outputs_enrichment_scraped_csv")
    
    # Load all CSV files
    print("🔄 Loading CSV files...")
    
    # Load nodes
    nodes_files = glob.glob(os.path.join(csv_dir, "*_nodes.csv"))  # Looking for *_nodes.csv  
    nodes_dfs = [pd.read_csv(f) for f in nodes_files]
    all_nodes = pd.concat(nodes_dfs, ignore_index=True)
    print(f"   Loaded {len(all_nodes)} nodes")
    
    # Load relationships
    rels_files = glob.glob(os.path.join(csv_dir, "*_rels.csv"))
    rels_dfs = [pd.read_csv(f) for f in rels_files]
    all_rels = pd.concat(rels_dfs, ignore_index=True)
    print(f"   Loaded {len(all_rels)} relationships")
    
    # Clear existing data
    print("🧹 Clearing existing Neo4j data...")
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    
    # Create nodes with proper labels
    print("📝 Creating nodes...")
    
    with driver.session() as session:
        node_count = 0
        
        for _, node in all_nodes.iterrows():
            node_uri = node['id']
            node_label_text = node['label']
            
            # Determine the Neo4j label for this node based on its type relationships
            neo4j_label = get_node_label_from_relationships(node_uri, all_rels)
            
            # Clean the label text for the name property
            clean_name = clean_label(node_label_text)
            
            # Create the node
            cypher = f"""
            CREATE (n:`{neo4j_label}` {{
                uri: $uri,
                label: $label,
                name: $name
            }})
            """
            
            try:
                session.run(cypher, {
                    'uri': node_uri,
                    'label': node_label_text,
                    'name': clean_name
                })
                node_count += 1
                
                if node_count % 50 == 0:
                    print(f"   Created {node_count} nodes...")
                    
            except Exception as e:
                print(f"   ⚠️  Error creating node {node_uri}: {e}")
        
        print(f"✅ Created {node_count} nodes")
    
    # Create relationships
    print("🔗 Creating relationships...")
    
    with driver.session() as session:
        rel_count = 0
        
        for _, rel in all_rels.iterrows():
            source_uri = rel['source']
            target_uri = rel['target']
            rel_type = clean_label(rel['type'])
            
            # Skip 'type' relationships as we used them for node labels
            if rel['type'] == 'type':
                continue
            
            # Clean relationship type name (Neo4j doesn't like some characters)
            rel_type_clean = re.sub(r'[^a-zA-Z0-9_]', '_', rel_type)
            
            cypher = f"""
            MATCH (source {{uri: $source_uri}})
            MATCH (target {{uri: $target_uri}})
            CREATE (source)-[r:`{rel_type_clean}`]->(target)
            SET r.original_type = $original_type
            """
            
            try:
                result = session.run(cypher, {
                    'source_uri': source_uri,
                    'target_uri': target_uri,
                    'original_type': rel['type']
                })
                
                if result.consume().counters.relationships_created > 0:
                    rel_count += 1
                
                if rel_count % 50 == 0:
                    print(f"   Created {rel_count} relationships...")
                    
            except Exception as e:
                print(f"   ⚠️  Error creating relationship {source_uri} -> {target_uri}: {e}")
        
        print(f"✅ Created {rel_count} relationships")
    
    # Verify the load
    print("\n📊 Verifying loaded data...")
    with driver.session() as session:
        # Count nodes by label
        labels_result = session.run("CALL db.labels()")
        labels = [record[0] for record in labels_result]
        
        print("   Node labels and counts:")
        for label in labels:
            count_result = session.run(f"MATCH (n:`{label}`) RETURN count(n) as count")
            count = count_result.single()["count"]
            print(f"     {label}: {count} nodes")
        
        # Count relationships
        rel_types_result = session.run("CALL db.relationshipTypes()")
        rel_types = [record[0] for record in rel_types_result]
        
        print("   Relationship types and counts:")
        for rel_type in rel_types[:10]:  # Show first 10
            count_result = session.run(f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) as count")
            count = count_result.single()["count"]
            print(f"     {rel_type}: {count} relationships")
    
    driver.close()
    print("\n✅ CSV data successfully loaded into Neo4j!")

def suggest_test_questions():
    """Suggest questions that should work with the loaded data"""
    print("\n" + "="*60)
    print("💡 SUGGESTED TEST QUESTIONS:")
    print("="*60)
    
    questions = [
        "What datasets are available?",
        "List all features.",
        "How many features does the WorldBankDataset have?",
        "What are the task specifications?",
        "Show me all datasets with their features.",
        "What purposes are there?",
        "Which features have float data type?",
        "What licenses are used?",
        "Show features related to economic analysis.",
        "List all modalities."
    ]
    
    for i, question in enumerate(questions, 1):
        print(f"{i:2d}. {question}")
    
    print("\nStart with simple questions like #1 or #2 to test the system!")

if __name__ == "__main__":
    print("🚀 CSV to Neo4j Loader")
    print("="*60)
    
    try:
        load_csv_to_neo4j()
        suggest_test_questions()
        
        print("\n📋 NEXT STEPS:")
        print("1. Run your KG_RAG.py again")
        print("2. Try the suggested questions above")
        print("3. Check if Cypher generation works better now")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 TROUBLESHOOTING:")
        print("1. Make sure Neo4j is running")
        print("2. Check your .env file has correct Neo4j credentials")
        print("3. Verify CSV files exist in ttl_outputs_enrichment_scraped_csv/")