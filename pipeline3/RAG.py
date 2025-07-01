"""
RAG.py: Convert RDF triples from Turtle into CSV files for Neo4j import.
Enhanced version to process entire folders of TTL files.
"""
import rdflib
import pandas as pd
from rdflib.namespace import RDF
import os
import glob
from pathlib import Path

def extract_csv(turtle_path: str, node_csv: str, rel_csv: str):
    """Convert a single TTL file to node and relationship CSV files."""
    g = rdflib.Graph()
    g.parse(turtle_path, format='turtle')

    nodes = []
    edges = []
    for s, p, o in g:
        # treat all subjects and objects as nodes
        nodes.append((str(s), s.split('#')[-1]))
        if isinstance(o, rdflib.URIRef):
            nodes.append((str(o), o.split('#')[-1]))
            edges.append((str(s), p.split('#')[-1], str(o)))

    nodes_df = pd.DataFrame(nodes, columns=['id', 'label']).drop_duplicates()
    edges_df = pd.DataFrame(edges, columns=['source', 'type', 'target'])

    nodes_df.to_csv(node_csv, index=False)
    edges_df.to_csv(rel_csv, index=False)

def process_folder(input_folder: str, output_folder: str):
    """Process all TTL files in input folder and create CSV files in output folder."""
    # Create output directory if it doesn't exist
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    # Find all TTL files in the input folder
    ttl_pattern = os.path.join(input_folder, "*.ttl")
    ttl_files = glob.glob(ttl_pattern)
    
    if not ttl_files:
        print(f"No TTL files found in {input_folder}")
        return
    
    print(f"Found {len(ttl_files)} TTL files to process")
    
    for ttl_file in ttl_files:
        # Get the base filename without extension
        base_name = Path(ttl_file).stem
        
        # Create output CSV filenames
        nodes_csv = os.path.join(output_folder, f"{base_name}_nodes.csv")
        rels_csv = os.path.join(output_folder, f"{base_name}_rels.csv")
        
        try:
            print(f"Processing: {ttl_file}")
            extract_csv(ttl_file, nodes_csv, rels_csv)
            print(f"  -> Created: {nodes_csv}")
            print(f"  -> Created: {rels_csv}")
        except Exception as e:
            print(f"Error processing {ttl_file}: {str(e)}")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Turtle → CSV for Neo4j (batch processing)')
    parser.add_argument('--turtle', help='Single TTL file to process')
    parser.add_argument('--nodes', default='nodes.csv', help='Output nodes CSV file (single file mode)')
    parser.add_argument('--rels', default='rels.csv', help='Output relationships CSV file (single file mode)')
    parser.add_argument('--input-folder', help='Folder containing TTL files to process')
    parser.add_argument('--output-folder', help='Folder to store output CSV files')
    
    args = parser.parse_args()
    
    if args.input_folder and args.output_folder:
        # Batch processing mode
        process_folder(args.input_folder, args.output_folder)
    elif args.turtle:
        # Single file mode (original functionality)
        extract_csv(args.turtle, args.nodes, args.rels)
    else:
        print("Please provide either:")
        print("  --turtle <file> for single file processing, or")
        print("  --input-folder <folder> --output-folder <folder> for batch processing")