import os
import json
import google.generativeai as genai

# === CONFIG ===
CLEAN_FOLDER = "clean_data"
OUTPUT_FOLDER = "ttl_outputs_enrichment"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

GEMINI_MODEL = "models/gemini-1.5-flash"
GEMINI_API_KEY = "your_api_key_here"  # Replace with your actual API key
ONTODM_URI = "https://purl.org/ontodm"

# === Gemini Setup ===
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(GEMINI_MODEL)

# === Prompt Builder ===
def build_prompt(metadata: dict) -> str:
    dataset_name = metadata.get("dataset_name", "UnnamedDataset").replace('"', '')
    dataset_description = metadata.get("dataset_description", "").replace('"', '')

    fields_json = json.dumps(metadata.get("fields", []), indent=2)

    prompt = f"""
You are an ontology expert. Given a dataset and its metadata, generate RDF triples in Turtle syntax using the OntoDM ontology.

Ontology URI: {ONTODM_URI}

### Instructions:
- Define the dataset as an instance of `ontodm:Dataset`.
- Use these properties for the dataset:
  - `ontodm:hasName`
  - `ontodm:hasDescription`
  - `ontodm:hasTask` (e.g., "emotion classification", "data visualization", "regression modeling")
  - `ontodm:hasApplicationDomain` (e.g., "music analysis", "healthcare", "education")
  - `ontodm:hasModality` (e.g., "audio", "text", "tabular")
- For each field, define it as `ontodm:Feature`.
  - Use `ontodm:hasName`, `ontodm:hasDescription`, `ontodm:hasDataType`, `ontodm:hasSourceColumn`.

### Datatype mappings:
  - sc:Integer → xsd:integer
  - sc:Float → xsd:float
  - sc:Boolean → xsd:boolean
  - sc:Text → xsd:string

### Example:
Example:
@prefix ontodm: <{ONTODM_URI}#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://example.org/dataset/> .

:Dataset a ontodm:Dataset ;
    ontodm:hasName "Emotion-Based Music Dataset" ;
    ontodm:hasDescription "Tracks with emotion annotations extracted from lyrics using LLMs" ;
    ontodm:hasTask "emotion classification" ;
    ontodm:hasApplicationDomain "music analysis" ;
    ontodm:hasModality "audio" .


"""
    
    prompt += f"""
### Now generate RDF triples for the dataset below.

Dataset Name: {dataset_name}
Dataset Description: {dataset_description}

Fields JSON:
{fields_json}

Output RDF triples:
"""
    return prompt

    return prompt

# === Main Loop ===
def main():


    files = [f for f in os.listdir(CLEAN_FOLDER) if f.endswith(".json")]
    print(f"Found {len(files)} cleaned metadata files...")

    for file in files:
        file_path = os.path.join(CLEAN_FOLDER, file)
        with open(file_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        print(f"→ Extracting RDF for: {file}")

        prompt = build_prompt(metadata)
        response = model.generate_content(prompt)

        rdf_text = response.text.strip()
        # Clean up Markdown-style code blocks and redundant prefixes
        rdf_text = rdf_text.replace("```turtle", "").replace("```", "").strip()

        dataset_id = metadata.get("dataset_name", file.replace(".json", "")).replace(" ", "_").replace("/", "_")
        output_ttl_path = os.path.join(OUTPUT_FOLDER, f"{dataset_id}.ttl")

        with open(output_ttl_path, "w", encoding="utf-8") as out_f:
            out_f.write(rdf_text)

        print(f"✅ Saved RDF triples to: {output_ttl_path}")

if __name__ == "__main__":
    main()
