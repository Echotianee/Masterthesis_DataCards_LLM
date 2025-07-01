import re
import os
import requests
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

class Config:
    GRAPHDB_ENDPOINT = "http://wangyidans-MacBook-Pro.local:7200/repositories/Thesis"
    GEMINI_API_KEY = "your_api_key_here"  # Replace with your actual API key
    GEMINI_MODEL_NAME = "models/gemini-1.5-flash"

# Initialize the Gemini client with temperature control
client = genai.Client(api_key=Config.GEMINI_API_KEY)


class OntoDMQuerySystem:
    def __init__(self):
        self.session = requests.Session()

    def get_schema_context(self) -> str:
        return """
Known OntoDM Structure:
- ontodm:Dataset
- ontodm:Feature
- ontodm:TaskSpecification
- ontodm:Modality
- ontodm:hasName
- ontodm:hasDescription
- ontodm:has_part
- ontodm:hasFeature
- ontodm:hasTask
- ontodm:is_specified_input_of
- ontodm:hasDataType
- dcterms:license

Common Path Patterns:
- ?x ontodm:hasName ?name
- ?x a ontodm:Dataset
- ?dataset ontodm:has_part ?feature
- ?dataset ontodm:hasFeature ?feature
- ?dataset ontodm:hasTask ?task
- ?dataset dcterms:license ?license
"""

    def _build_prompt(self, question: str, graph_iri: str) -> str:
        few_shot = f"""
### Example 1
NL: "What is the modality of the Spotify dataset?"
SPARQL:
PREFIX ontodm: <https://purl.org/ontodm#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX ex: <http://example.org/>

SELECT ?modality
WHERE {{
  GRAPH <{graph_iri}> {{
    ex:Dataset_200kSpotifySongs a ontodm:Dataset ;
      ontodm:hasModality ?modality .
  }}
}}

### Example 2
NL: "Which datasets have license CC-BY-4.0?"
SPARQL:
PREFIX ontodm: <https://purl.org/ontodm#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX ex: <http://example.org/>

SELECT ?ds ?name
WHERE {{
  GRAPH <{graph_iri}> {{
    ?ds a ontodm:Dataset ;
        dcterms:license <https://creativecommons.org/licenses/by/4.0/> ;
        ontodm:hasName ?name .
  }}
}}
"""

        rules = [
            "You are an expert SPARQL query assistant.",
            self.get_schema_context(),
            "Rules:",
            "1. Always include relevant PREFIX declarations.",
            "2. Do NOT use FROM; wrap your triple patterns in a GRAPH block.",
            "3. Use rdf:type (or `a`) for class filtering.",
            "4. Chain predicates with semicolons when they share the same subject.",
            "5. Use FILTER(CONTAINS(LCASE(STR(?var)), \"keyword\")): matching is case-insensitive.",
            "6. Use SELECT DISTINCT when appropriate.",
            "7. Combine multiple parts with commas: `ontodm:has_part ?p1, ?p2`.",
            "8. Use COUNT, GROUP BY, ORDER BY for aggregates.",
            "9. Expand partial names via predefined mappings.",
            "10. Follow the formatting style of provided examples.",
            "11. For tasks/features, use `ontodm:hasName` (not `rdfs:label`).",
            "12. Prioritize clarity and consistency with your examples.",
            "13. To inspect a Dataset’s parts, use `ontodm:has_part` + `ontodm:hasName`.",
            "14. If the question asks “support” (e.g. “How does X support Y?”),",
            "    use `ontodm:is_specified_input_of` (or `is_specified_output_of`) instead of `has_part`.",
            "15. When selecting parts of a specific dataset by name, first bind and filter its name:",
    "    ?dataset a ontodm:Dataset ;",
    "             ontodm:hasName    ?name ;",
    "             ontodm:has_part   ?part .",
    "    FILTER(CONTAINS(LCASE(STR(?name)), \"<keyword>\"))",
            """16. For “Which X has more/less Y?” questions, use a single aggregated query:
- SELECT ?entity ?name (COUNT(?feature) AS ?feature_count)
- WHERE { … FILTER(CONTAINS(..., "a") || CONTAINS(..., "b")) }
- GROUP BY ?entity ?name
- ORDER BY DESC(?feature_count)
Do NOT use UNION for such comparisons.""",
            "17. Whenever you use an aggregate (e.g. `COUNT(?x)`) in SELECT, include",
            "    `GROUP BY` over every other projected variable and optionally `ORDER BY DESC(...)`.",
            "18. When a field may lack a description, use `OPTIONAL { ?x ontodm:hasDescription ?desc }`.",
            "19. For “How does X support Y?” questions:",
            "    • Anchor Y by its full IRI (no name FILTER).",
            "    • Bind the TaskSpecification to `?taskSpec`, include",
            "      `ontodm:hasName` and `ontodm:hasDescription`.",
            "    • Use `ontodm:is_specified_input_of ex:Y` (not `hasTask`).",
            "20. In every OPTIONAL { … } block, always repeat the subject variable, e.g.:\n"
            "    OPTIONAL { ?subject predicate object }",
            "21.Use `ontodm:has_part` to link datasets to their features.",
            "",
            few_shot,
            f"\n### Now convert:\nNL: \"{question}\"\nSPARQL:\n"
        ]
        return "\n".join(rules)

    def generate_sparql(self, question: str) -> str:
        graph_iri = "http://example.org/graph/enrichmentwithexamples"
        prompt = self._build_prompt(question, graph_iri)

        # Generate via the new client API with temperature control
        result = client.models.generate_content(
            model=Config.GEMINI_MODEL_NAME,
            contents=[prompt],
            config=types.GenerateContentConfig(
                temperature=0.0,
                max_output_tokens=1024,
                candidate_count=1
            )
        )
        raw = result.text

        # Extract SPARQL (from first PREFIX through the final })
        match = re.search(r'(PREFIX[\s\S]*\})', raw)
        query = match.group(1).strip() if match else raw.strip()

        # Strip any accidental FROM clauses
        query = re.sub(r'\bFROM\b\s*<[^>]+>', '', query, flags=re.IGNORECASE)

        # Wrap WHERE block in GRAPH if missing
        if "GRAPH <" not in query.split("WHERE", 1)[1]:
            query = (
                query
                .replace("WHERE {", f"WHERE {{\n  GRAPH <{graph_iri}> {{")
                .rstrip()
                + "\n  }\n}"
            )

        # Auto-append GROUP BY/ORDER BY when COUNT(...) present without GROUP BY
        if "COUNT(" in query.upper() and "GROUP BY" not in query.upper():
            header = re.search(r"SELECT\s+(.*?)\s*\(COUNT", query, re.IGNORECASE)
            if header:
                vars_part = header.group(1).strip()
                query = query.rstrip() + f"\nGROUP BY {vars_part}\nORDER BY DESC(?feature_count)"

        return query

    def _execute_query(self, query: str) -> dict:
        headers = {
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/sparql-query",
            "User-Agent": "OntoDM-KG-Query-System"
        }
        response = self.session.post(
            Config.GRAPHDB_ENDPOINT,
            headers=headers,
            data=query,
            timeout=15
        )
        return response.json() if response.ok else {"error": f"{response.status_code}: {response.text}"}

    def format_results(self, results: dict) -> str:
        if "error" in results:
            return f"Error: {results['error']}"
        bindings = results.get('results', {}).get('bindings', [])
        if not bindings:
            return "No results found."
        primary = next(iter(bindings[0].keys()))
        lines = []
        for row in bindings:
            uri = row[primary]['value']
            name = uri.split('/')[-1]
            details = ", ".join(
                f"{k}: {v['value'].split('#')[-1]}"
                for k, v in row.items() if k != primary
            )
            lines.append(f"{name} ({details})" if details else name)
        return "Found {} result(s):\n{}".format(len(lines), "\n".join(lines))

    def interactive_query(self):
        print("🔍 OntoDM Text-to-SPARQL Query System (type 'exit' to quit')")
        while True:
            q = input("\n🧠 Ask a question: ").strip()
            if q.lower() in ["exit", "quit"]:
                break
            print("\n📄 Generating SPARQL Query...")
            sparql = self.generate_sparql(q)
            print("\n📄 SPARQL Query:\n", sparql)
            print("\n🚀 Sending to GraphDB...")
            res = self._execute_query(sparql)
            print("\n📦 Result:\n", self.format_results(res))


if __name__ == "__main__":
    kg = OntoDMQuerySystem()
    kg.interactive_query()









