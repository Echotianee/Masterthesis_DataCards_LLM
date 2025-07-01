# neo4j_graph_visualizer.py

import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter, defaultdict
from dotenv import load_dotenv
from neo4j import GraphDatabase, exceptions as neo4j_exceptions

def analyze_neo4j_database():
    """Analyze what's actually in your Neo4j database"""
    load_dotenv()
    
    # Load Neo4j credentials
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pw = os.getenv("NEO4J_PASSWORD", "YidanThesis")
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        
        with driver.session() as session:
            print("🔍 Analyzing Neo4j Database Structure\n")
            
            # 1. Check what labels exist
            print("1. NODE LABELS IN DATABASE:")
            result = session.run("CALL db.labels()")
            labels = [record["label"] for record in result]
            print(f"   Found {len(labels)} labels: {labels}")
            
            # 2. Check what relationship types exist
            print("\n2. RELATIONSHIP TYPES IN DATABASE:")
            result = session.run("CALL db.relationshipTypes()")
            rel_types = [record["relationshipType"] for record in result]
            print(f"   Found {len(rel_types)} relationship types: {rel_types}")
            
            # 3. Count nodes by label
            print("\n3. NODE COUNTS BY LABEL:")
            for label in labels:
                result = session.run(f"MATCH (n:`{label}`) RETURN count(n) as count")
                count = result.single()["count"]
                print(f"   {label}: {count} nodes")
            
            # 4. Count relationships by type
            print("\n4. RELATIONSHIP COUNTS BY TYPE:")
            for rel_type in rel_types:
                result = session.run(f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) as count")
                count = result.single()["count"]
                print(f"   {rel_type}: {count} relationships")
            
            # 5. Sample some nodes to see their properties
            print("\n5. SAMPLE NODES (first 5 of each label):")
            for label in labels[:3]:  # Limit to first 3 labels to avoid spam
                print(f"\n   Sample {label} nodes:")
                result = session.run(f"MATCH (n:`{label}`) RETURN n LIMIT 5")
                for i, record in enumerate(result, 1):
                    node = record["n"]
                    props = dict(node)
                    print(f"     {i}. {props}")
            
            # 6. Check database schema
            print("\n6. DATABASE SCHEMA:")
            result = session.run("CALL db.schema.visualization()")
            schema_info = list(result)
            if schema_info:
                print("   Schema visualization available")
            else:
                print("   No schema constraints found")
            
            # 7. Get some sample relationships
            print("\n7. SAMPLE RELATIONSHIPS:")
            result = session.run("""
                MATCH (a)-[r]->(b) 
                RETURN labels(a)[0] as source_label, type(r) as rel_type, labels(b)[0] as target_label, 
                       a, r, b
                LIMIT 10
            """)
            
            for i, record in enumerate(result, 1):
                source_label = record["source_label"]
                rel_type = record["rel_type"]
                target_label = record["target_label"]
                source_props = dict(record["a"])
                target_props = dict(record["b"])
                
                print(f"   {i}. ({source_label})-[{rel_type}]->({target_label})")
                print(f"      Source: {source_props}")
                print(f"      Target: {target_props}")
                print()
            
        driver.close()
        return labels, rel_types
        
    except neo4j_exceptions.AuthError:
        print(f"❌ Authentication failed when connecting to Neo4j at {uri}")
        return [], []
    except Exception as e:
        print(f"❌ Could not connect to Neo4j at {uri}: {e}")
        return [], []

def analyze_csv_files():
    """Analyze what's in your CSV files"""
    print("\n" + "="*60)
    print("🔍 Analyzing CSV Files Structure\n")
    
    file_root = os.path.dirname(os.path.abspath(__file__))
    csv_dir = os.path.join(file_root, "ttl_outputs_enrichment_scraped_csv")
    
    if not os.path.exists(csv_dir):
        print(f"❌ CSV directory not found: {csv_dir}")
        return
    
    import glob
    
    # Analyze nodes files
    nodes_files = glob.glob(os.path.join(csv_dir, "nodes_*.csv"))
    print(f"1. NODES FILES: Found {len(nodes_files)} files")
    
    if nodes_files:
        # Load all nodes
        nodes_dfs = [pd.read_csv(f) for f in nodes_files]
        all_nodes = pd.concat(nodes_dfs, ignore_index=True)
        
        print(f"   Total nodes: {len(all_nodes)}")
        print(f"   Columns: {list(all_nodes.columns)}")
        
        # Analyze types/labels in nodes
        if 'label' in all_nodes.columns:
            label_counts = all_nodes['label'].value_counts()
            print(f"\n   Top 10 Labels from CSV:")
            for label, count in label_counts.head(10).items():
                print(f"     {label}: {count}")
        
        # Show sample nodes
        print(f"\n   Sample nodes:")
        for i, (_, row) in enumerate(all_nodes.head(5).iterrows(), 1):
            print(f"     {i}. {dict(row)}")
    
    # Analyze relationship files
    rels_files = glob.glob(os.path.join(csv_dir, "rels_*.csv"))
    print(f"\n2. RELATIONSHIPS FILES: Found {len(rels_files)} files")
    
    if rels_files:
        # Load all relationships
        rels_dfs = [pd.read_csv(f) for f in rels_files]
        all_rels = pd.concat(rels_dfs, ignore_index=True)
        
        print(f"   Total relationships: {len(all_rels)}")
        print(f"   Columns: {list(all_rels.columns)}")
        
        # Analyze relationship types
        if 'type' in all_rels.columns:
            rel_type_counts = all_rels['type'].value_counts()
            print(f"\n   Top 10 Relationship Types from CSV:")
            for rel_type, count in rel_type_counts.head(10).items():
                print(f"     {rel_type}: {count}")
        
        # Show sample relationships
        print(f"\n   Sample relationships:")
        for i, (_, row) in enumerate(all_rels.head(5).iterrows(), 1):
            print(f"     {i}. {dict(row)}")

def compare_csv_vs_neo4j():
    """Compare what's in CSV vs what's in Neo4j"""
    print("\n" + "="*60)
    print("🔍 Comparing CSV Files vs Neo4j Database\n")
    
    # This function would need to be implemented based on your specific data structure
    # For now, let's provide some guidance
    
    print("POTENTIAL ISSUES TO CHECK:")
    print("1. Are the CSV files being loaded into Neo4j correctly?")
    print("2. Do the node labels in CSV match the labels in Neo4j?")
    print("3. Are there any data transformation steps between CSV and Neo4j?")
    print("4. Check if URIs from CSV are being converted to node labels properly")
    print("\nRecommendation: Run the CSV analysis and Neo4j analysis above to compare")

def create_simple_visualization():
    """Create a simple networkx visualization from CSV data"""
    print("\n" + "="*60)
    print("📊 Creating Simple Graph Visualization from CSV\n")
    
    file_root = os.path.dirname(os.path.abspath(__file__))
    csv_dir = os.path.join(file_root, "ttl_outputs_enrichment_scraped_csv")
    
    try:
        import glob
        
        # Load relationships
        rels_files = glob.glob(os.path.join(csv_dir, "rels_*.csv"))
        if not rels_files:
            print("❌ No relationship files found for visualization")
            return
        
        rels_dfs = [pd.read_csv(f) for f in rels_files]
        all_rels = pd.concat(rels_dfs, ignore_index=True)
        
        # Create networkx graph
        G = nx.DiGraph()
        
        # Add edges (this creates nodes automatically)
        for _, row in all_rels.head(50).iterrows():  # Limit to first 50 for readability
            source = row['source'].split('#')[-1] if '#' in str(row['source']) else str(row['source'])
            target = row['target'].split('#')[-1] if '#' in str(row['target']) else str(row['target'])
            rel_type = row['type'].split('#')[-1] if '#' in str(row['type']) else str(row['type'])
            
            G.add_edge(source, target, relationship=rel_type)
        
        # Create visualization
        plt.figure(figsize=(15, 10))
        pos = nx.spring_layout(G, k=1, iterations=50)
        
        # Draw nodes
        nx.draw_networkx_nodes(G, pos, node_color='lightblue', 
                              node_size=1000, alpha=0.7)
        
        # Draw edges
        nx.draw_networkx_edges(G, pos, edge_color='gray', 
                              arrows=True, arrowsize=20, alpha=0.5)
        
        # Draw labels
        nx.draw_networkx_labels(G, pos, font_size=8, font_weight='bold')
        
        # Draw edge labels (relationship types)
        edge_labels = nx.get_edge_attributes(G, 'relationship')
        nx.draw_networkx_edge_labels(G, pos, edge_labels, font_size=6)
        
        plt.title("Knowledge Graph Visualization (Sample from CSV)")
        plt.axis('off')
        plt.tight_layout()
        
        # Save the plot
        output_path = os.path.join(file_root, "graph_figures", "graph_visualization.png")
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"📊 Graph visualization saved to: {output_path}")
        
        # Show basic stats
        print(f"Graph Statistics:")
        print(f"  Nodes: {G.number_of_nodes()}")
        print(f"  Edges: {G.number_of_edges()}")
        print(f"  Connected Components: {nx.number_weakly_connected_components(G)}")
        
        plt.show()
        
    except Exception as e:
        print(f"❌ Error creating visualization: {e}")

def main():
    print("🔍 Neo4j Graph Database and CSV Analysis Tool\n")
    
    # Analyze Neo4j database
    neo4j_labels, neo4j_rels = analyze_neo4j_database()
    
    # Analyze CSV files
    analyze_csv_files()
    
    # Compare them
    compare_csv_vs_neo4j()
    
    # Create visualization
    create_simple_visualization()
    
    print("\n" + "="*60)
    print("📋 SUMMARY AND RECOMMENDATIONS:")
    print("1. Check if your CSV data has been properly loaded into Neo4j")
    print("2. Verify that node labels match between CSV and Neo4j")
    print("3. Consider running a proper ETL process to load CSV into Neo4j")
    print("4. The generated Cypher queries need to match your actual Neo4j schema")

if __name__ == "__main__":
    main()