# KG_query.py - Improved version

import re
import os
from dotenv import load_dotenv
import google.generativeai as genai
from neo4j import GraphDatabase

# Load environment variables and configure Gemini
load_dotenv()
genai.configure(api_key=os.getenv('GEMINI_API_KEY', "yor_api_key_here"))
gemini = genai.GenerativeModel(os.getenv('GEMINI_MODEL_NAME', "models/gemini-1.5-flash"))

# ─── Updated Few-Shot Examples ─────────────────────────────────────
few_shot_snippets = [
    {
        "type": "listing",
        "question": "What datasets are available?",
        "cypher": "MATCH (d:Dataset) RETURN d.name AS name, d.label AS label"
    },
    {
        "type": "listing", 
        "question": "List all features.",
        "cypher": "MATCH (f:Feature) RETURN f.name AS feature_name, f.label AS label"
    },
    {
        "type": "counting",
        "question": "How many features does WorldBankDataset have?",
        "cypher": "MATCH (d:Dataset {name: 'WorldBankDataset'})-[:hasFeature]->(f:Feature) RETURN COUNT(f) AS feature_count"
    },
    {
        "type": "relationship",
        "question": "What features does WorldBankDataset have?",
        "cypher": "MATCH (d:Dataset {name: 'WorldBankDataset'})-[:hasFeature]->(f:Feature) RETURN f.name AS feature_name"
    },
    {
        "type": "filtering",
        "question": "Which features have float data type?",
        "cypher": "MATCH (f:Feature)-[:hasDataType]->(t) WHERE t.name = 'float' RETURN f.name AS feature_name"
    },
    {
        "type": "listing",
        "question": "What task specifications are there?",
        "cypher": "MATCH (t:TaskSpecification) RETURN t.name AS task_name, t.label AS label"
    }
]

def introspect_schema(driver: GraphDatabase) -> str:
    """Get detailed schema information from Neo4j"""
    with driver.session() as session:
        # Get node labels with counts
        labels_result = session.run("CALL db.labels()")
        labels = [record[0] for record in labels_result]

        # Get relationship types with counts  
        rels_result = session.run("CALL db.relationshipTypes()")
        relationships = [record[0] for record in rels_result]

        # Get property keys
        props_result = session.run("CALL db.propertyKeys()")
        properties = [record[0] for record in props_result]

        # Get sample nodes for each label to understand structure
        schema_details = []
        for label in labels[:5]:  # Limit to first 5 labels
            try:
                sample_result = session.run(f"MATCH (n:`{label}`) RETURN n LIMIT 3")
                samples = [dict(record["n"]) for record in sample_result]
                
                count_result = session.run(f"MATCH (n:`{label}`) RETURN count(n) as count")
                count = count_result.single()["count"]
                
                schema_details.append(f"- {label} ({count} nodes)")
                if samples:
                    sample_props = set()
                    for sample in samples:
                        sample_props.update(sample.keys())
                    schema_details.append(f"  Properties: {', '.join(sorted(sample_props))}")
                    
            except Exception as e:
                schema_details.append(f"- {label} (error: {e})")

        # Build comprehensive schema description
        schema = "=== NODE LABELS ===\n" + "\n".join(schema_details)
        schema += "\n\n=== RELATIONSHIP TYPES ===\n" + "\n".join(f"- {rel}" for rel in relationships)
        schema += "\n\n=== PROPERTIES ===\n" + "\n".join(f"- {prop}" for prop in properties)
        
        return schema

def generate_cypher(question: str, driver: GraphDatabase, context: str = "") -> str:
    """
    Generate a Cypher query based on the question and actual Neo4j schema.
    """
    schema_block = introspect_schema(driver)
    context_block = context.strip() or "No additional context."

    # Build the prompt for Gemini
    prompt = f"""
You are a Cypher expert for Neo4j. Generate a single, well-formed Cypher query.

IMPORTANT RULES:
1. Use only the labels and relationships that exist in the schema below
2. Node properties include: uri, label, name (use 'name' for readable identifiers)
3. Common relationship patterns from the data:
   - Dataset -[:hasFeature]-> Feature
   - Feature -[:hasDataType]-> DataType
   - TaskSpecification -[:is_specified_input_of]-> Dataset
   - Entity -[:type]-> Category (but this was converted to node labels)
4. When looking for datasets, use: MATCH (d:Dataset)
5. When looking for features, use: MATCH (f:Feature)
6. When looking for task specifications, use: MATCH (t:TaskSpecification)
7. Use the 'name' property for human-readable names
8. Use parameterized queries when filtering by specific values

SCHEMA INFORMATION:
{schema_block}

EXAMPLES:
{chr(10).join(f"Q: {ex['question']}{chr(10)}A: {ex['cypher']}" for ex in few_shot_snippets)}

CONTEXT FROM RETRIEVAL:
{context_block}

QUESTION: {question}

Generate ONLY the Cypher query, no explanations:
"""

    # Generate content
    try:
        resp = gemini.generate_content(prompt).text
        
        # Extract the Cypher query - try multiple patterns
        patterns = [
            r'```cypher\s*(.*?)\s*```',
            r'```\s*(MATCH.*?)\s*```',
            r'(MATCH.*?)(?:\n|$)',
            r'(CREATE.*?)(?:\n|$)',
            r'(RETURN.*?)(?:\n|$)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, resp, re.DOTALL | re.IGNORECASE)
            if match:
                cypher = match.group(1).strip()
                # Clean up the query
                cypher = re.sub(r'\s+', ' ', cypher)  # Normalize whitespace
                return cypher
        
        # If no pattern matches, return the response as-is (cleaned up)
        cleaned_resp = re.sub(r'\s+', ' ', resp.strip())
        return cleaned_resp
        
    except Exception as e:
        print(f"Error generating Cypher: {e}")
        return "MATCH (n) RETURN count(n) as total_nodes"  # Safe fallback

def test_cypher_generation():
    """Test the Cypher generation with sample questions"""
    load_dotenv()
    
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "YidanThesis")
    
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    test_questions = [
        "What datasets are available?",
        "List all features.",
        "How many features does WorldBankDataset have?",
        "What task specifications are there?",
        "Which features have float data type?"
    ]
    
    print("🧪 Testing Cypher Generation:")
    print("="*50)
    
    for question in test_questions:
        print(f"\nQ: {question}")
        cypher = generate_cypher(question, driver)
        print(f"Generated Cypher: {cypher}")
        
        # Test execution
        try:
            with driver.session() as session:
                result = session.run(cypher)
                records = list(result)
                print(f"✅ Executed successfully, returned {len(records)} results")
                
                # Show first few results
                for i, record in enumerate(records[:3]):
                    print(f"   {i+1}. {dict(record)}")
                    
        except Exception as e:
            print(f"❌ Execution failed: {e}")
    
    driver.close()

if __name__ == "__main__":
    test_cypher_generation()