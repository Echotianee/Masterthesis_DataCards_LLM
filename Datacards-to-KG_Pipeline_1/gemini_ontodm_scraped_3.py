import os
import json
import google.generativeai as genai

# === CONFIG ===
CLEAN_FOLDER = "scraped_metadata"
OUTPUT_FOLDER = "ttl_outputs_enrichment_scraped"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

GEMINI_MODEL = "models/gemini-1.5-flash"
GEMINI_API_KEY = "your_api_key_here"  # Replace with your actual API key
ONTODM_URI = "https://purl.org/ontodm"

# === Gemini Setup ===
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(GEMINI_MODEL)

# === Prompt Builder ===
def build_prompt(metadata: dict, license_name: str) -> str:
    raw_text = json.dumps(metadata, indent=2)

    prompt = f"""
You are an expert ontology population assistant. Your task is to generate RDF triples in Turtle format using the OntoDM ontology (https://purl.org/ontodm#) and Dublin Core Terms (https://purl.org/dc/terms/), given any unstructured dataset metadata.

---

### Ontology URI:
https://purl.org/ontodm#

### Use only these OntoDM classes:
- ontodm:Dataset
- ontodm:Feature
- ontodm:TaskSpecification
- ontodm:ApplicationDomain
- ontodm:Purpose
- ontodm:Modality

### Use only these properties:
- ontodm:hasName
- ontodm:hasDescription
- ontodm:hasDataType
- ontodm:is_specified_input_of
- ontodm:is_specified_output_of
- dcterms:license 

---

### Instructions:
- Parse and interpret the raw metadata below.
- Do not assume any fixed structure—extract concepts even from free-text.
- Map fields, tasks, modalities, purposes, and licenses as best as possible.
- Output only RDF triples in valid Turtle (.ttl) syntax.
- Skip unknown or unmappable fields.

---

### Input Metadata:
{raw_text}

---

### Output RDF Triples:
"""
    return prompt


    return prompt

# === Main Loop ===
def main():
    files = [f for f in os.listdir(CLEAN_FOLDER) if f.endswith(".json")]
    print(f"Found {len(files)} cleaned metadata files...")

    for file in files:
        file_path = os.path.join(CLEAN_FOLDER, file)

        # Load JSON
        with open(file_path, "r", encoding="utf-8") as f:
            metadata_raw = json.load(f)

        # Parse embedded JSON string
        metadata = metadata_raw

        # Extract license mapping
        license_name = ""
        licenses = metadata.get("licenses", [])
        if licenses:
            license_name = licenses[0].get("name", "")

        license_list = metadata.get("licenses", [])
        if license_list:
            license_name = license_list[0].get("name", "")
            if "CC0" in license_name:
                license_url = "https://creativecommons.org/publicdomain/zero/1.0/"
            elif "Community Data License" in license_name:
                license_url = "https://cdla.dev/sharing-1-0/"

        

        print(f"→ Extracting RDF for: {file}")

        prompt = build_prompt(metadata, license_name)

        response = model.generate_content(prompt)

        rdf_text = response.text.strip()
        rdf_text = rdf_text.replace("turtle", "").replace("", "").strip()
        rdf_text = response.text.strip()

        # Remove triple backticks if present
        if rdf_text.startswith("```"):
            rdf_text = rdf_text.strip("`").strip()
        # Also strip possible language label e.g. ```turtle
        if rdf_text.lower().startswith("turtle"):
            rdf_text = rdf_text[len("turtle"):].strip()

        dataset_name = metadata.get("title", file.replace(".json", "")).replace(" ", "_").replace("/", "_")
        output_ttl_path = os.path.join(OUTPUT_FOLDER, f"{dataset_name}.ttl")

        with open(output_ttl_path, "w", encoding="utf-8") as out_f:
            out_f.write(rdf_text)

        print(f"✅ Saved RDF triples to: {output_ttl_path}")


if __name__ == "__main__":
    main()