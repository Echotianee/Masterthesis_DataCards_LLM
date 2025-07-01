import re
import os
import requests
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

class Config:
    GRAPHDB_ENDPOINT = "http://wangyidans-MacBook-Pro.local:7200/repositories/Thesis"
    GEMINI_API_KEY = "your_api_key_here"  # Replace with your actual API key
    GEMINI_MODEL_NAME = "models/gemini-1.5-flash"

    PREFIXES = """
PREFIX mcro: <http://purl.obolibrary.org/obo/MCRO_>
PREFIX prov1: <https://www.w3.org/ns/prov#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""

genai.configure(api_key=Config.GEMINI_API_KEY)
gemini = genai.GenerativeModel(Config.GEMINI_MODEL_NAME)


class OntoDMQuerySystem:
    def __init__(self):
        self.session = requests.Session()

    def get_schema_context(self) -> str:
        return """
Known OntoDM Structure:
- ontodm:Dataset              — represents a dataset
- ontodm:Feature              — individual data fields
- ontodm:TaskSpecification    — task like classification, regression
- ontodm:Modality             — data modality (e.g. tabular)
- ontodm:hasName              — name of a resource
- ontodm:hasDescription       — description of a resource
- ontodm:has_part             — links models/datasets to components
- ontodm:hasDataType          — feature type like integer, text
- dcterms:license             — license URL

Common Path Patterns:
- ?x ontodm:hasName ?name
- ?x ontodm:hasDescription ?desc
- ?x a ontodm:Dataset
- ?dataset ontodm:has_part ?feature
- ?feature ontodm:hasDataType ?datatype
- ?dataset dcterms:license ?license
"""

    def generate_sparql(self, question: str) -> str:
        prompt = f"""You are an expert SPARQL query assistant.
        PREFIX ontodm: <https://purl.org/ontodm#>
        {self.get_schema_context()}

       
        Convert the following natural language question into a SPARQL query targeting the named graph <http://example.org/graph/enrichmentwithexamples>.
        Rules:
        1. Always include relevant PREFIX declarations at the top (e.g., ontodm, dc, dcterms).
        2. Use only FROM <http://example.org/graph/enrichmentwithexamples> — do not use GRAPH {{}} blocks.
        3. Place the FROM clause immediately after the SELECT clause and before the WHERE block.
        4. Use rdf:type (or a) to filter by class when accessing properties (e.g., ?x a ontodm:Feature before ontodm:hasDataType).
        5. Chain properties using semicolons when they share the same subject (e.g.,?s a ontodm:Dataset ; ontodm:hasName ?name ; dc:license ?license .).
        6. Use FILTER(CONTAINS(LCASE(STR(?var)), "keyword")) when matching dataset or domain names, especially partial or lowercase values.
        7. Avoid mixing FROM and GRAPH — only use FROM.
        8. Use COUNT, GROUP BY, and ORDER BY properly when aggregating results.
        9. When the question refers to a dataset using a partial or informal name (e.g., “Spotify”, “Bank”, “Cancer”), expand it using a predefined mapping (e.g., "Spotify" → "200K+ Spotify Songs Light Dataset").
        10.Follow the structure and formatting style of the examples provided, including spacing, indentation, and use of OPTIONAL and FILTER clauses.
        11.For tasks or features, use appropriate class filtering (ontodm:Task, ontodm:Feature, etc.) and include descriptions or purposes if mentioned.
        12.Prioritize clarity and consistency with the queries used in folders like ttl_outputs_enrichment_prompts_with_examples and ttl_outputs_enrichment_scraped.
        16. For “How does X support Y?” questions:\n"
    "    • Anchor Y by its full IRI (no name FILTER).\n"
    "    • Use ontodm:is_specified_input_of ex:Y (one-way).\n"
    "    • Do NOT use ontodm:hasTask (it does not exist).",

Question: {question}

SPARQL:
        """

        for _ in range(3):
            try:
                response = gemini.generate_content(prompt)
                match = re.search(r"```sparql(.*?)```", response.text, re.DOTALL)
                if match:
                    query = match.group(1).strip()
                    if not query.startswith("PREFIX"):
                        query = f"PREFIX ontodm: <https://purl.org/ontodm#>\nPREFIX dcterms: <http://purl.org/dc/terms/>\nFROM <http://example.org/graph/file4>\n" + query
                    elif "FROM" not in query:
                        query = query.replace("WHERE {", "FROM <http://example.org/graph/file4>\nWHERE {")
                    return query
            except Exception as e:
                print(f"Gemini Error: {e}")
        return ""


    def _execute_query(self, query: str) -> dict:
        """Execute SPARQL query and return JSON results."""
        headers = {
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/sparql-query",
            "User-Agent": "OntoDM-KG-Query-System"
        }

        try:
            response = self.session.post(
                Config.GRAPHDB_ENDPOINT,
                headers=headers,
                data=query,
                timeout=15
            )
            return response.json() if response.ok else {"error": f"{response.status_code}: {response.text}"}
        except Exception as e:
            return {"error": str(e)}

    def format_results(self, results: dict) -> str:
        """Format the SPARQL query results."""
        if "error" in results:
            return f"Error: {results['error']}"

        bindings = results.get('results', {}).get('bindings', [])
        if not bindings:
            return "No results found."

        primary_var = next(iter(bindings[0].keys())) if bindings else None
        output = []

        for row in bindings:
            if primary_var:
                entity_uri = row[primary_var]['value']
                entity = entity_uri.split('/')[-1] if '/' in entity_uri else entity_uri

                details = [
                    f"{k}: {v['value'].split('#')[-1]}"
                    for k, v in row.items()
                    if k != primary_var and 'value' in v
                ]
                line = f"{entity}"
                if details:
                    line += f" ({', '.join(details)})"
                output.append(line)

        return f"Found {len(output)} result(s):\n" + "\n".join(output)

    def interactive_query(self):
        """Interactive mode for asking natural language questions."""
        print("🔍 OntoDM Text-to-SPARQL Query System (type 'exit' to quit)")
        while True:
            question = input("\n🧠 Ask a question: ").strip()
            if question.lower() in ['exit', 'quit']:
                break

            print("\n📄 Generating SPARQL Query...")
            sparql = self.generate_sparql(question)

            if not sparql:
                print("⚠️ Failed to generate a valid SPARQL query.")
                continue

            print("\n📄 SPARQL Query:\n")
            print(sparql)

            print("\n🚀 Sending to GraphDB...")
            results = self._execute_query(sparql)
            print("\n📦 Result:")
            print(self.format_results(results))



if __name__ == "__main__":
    kg_system = OntoDMQuerySystem()
    kg_system.interactive_query()