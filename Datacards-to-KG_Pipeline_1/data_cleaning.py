import os
import json
import re
from typing import List, Dict

# === CONFIG ===
RAW_FOLDER = "kaggle_datacards/meta_data"
CLEAN_FOLDER = "clean_data"
os.makedirs(CLEAN_FOLDER, exist_ok=True)

# === Metadata Cleaning Functions ===
import re

def clean_metadata(metadata: Dict) -> Dict:
    datatype_map = {
        "sc:Text": "xsd:string",
        "sc:Integer": "xsd:integer",
        "sc:Float": "xsd:float",
        "sc:Boolean": "xsd:boolean"
    }

    def remove_emojis(text: str) -> str:
        # Regex for wide unicode ranges covering emojis/symbols
        emoji_pattern = re.compile("[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U0001F900-\U0001F9FF]+", flags=re.UNICODE)
        return emoji_pattern.sub("", text).strip()

    # Clean dataset-level info
    dataset_name_raw = metadata.get("name", "").strip()
    dataset_description_raw = metadata.get("description", "").strip()
    dataset_name = remove_emojis(dataset_name_raw)
    dataset_description = clean_description(dataset_description_raw)

    cleaned_fields = []

    for recordset in metadata.get("recordSet", []):
        for field in recordset.get("field", []):
            name = field.get("name", "").strip()
            raw_description = field.get("description", "").strip()
            raw_type = field.get("dataType", ["sc:Text"])[0]

            description = clean_description(raw_description or name)
            datatype = datatype_map.get(raw_type, "xsd:string")

            try:
                source_column = field["source"]["extract"]["column"]
            except KeyError:
                source_column = name

            uri_suffix = re.sub(r"[^a-zA-Z0-9_]", "_", name) + "Field"

            cleaned_fields.append({
                "name": name,
                "description": description,
                "dataType": datatype,
                "source_column": source_column,
                "uri_suffix": uri_suffix
            })

    return {
        "dataset_name": dataset_name,
        "dataset_description": dataset_description,
        "fields": cleaned_fields
    }

def clean_description(desc: str) -> str:
    # Remove emojis & symbols
    desc = re.sub(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U0001F900-\U0001F9FF]+", "", desc, flags=re.UNICODE)
    # Remove horizontal rules, --- or ----- lines
    desc = re.sub(r"^-{2,}$", "", desc, flags=re.MULTILINE)
    # Remove markdown separators like "---"
    desc = re.sub(r"-{3,}", " ", desc)
    # Clean special characters (keep words, digits, punctuation)
    desc = re.sub(r"[^\w\s.,;:()\[\]%-]", "", desc)
    # Normalize whitespaces
    desc = re.sub(r"\s+", " ", desc)
    return desc.strip()




def clean_description(desc: str) -> str:
    desc = re.sub(r"[^\w\s.,;:()\[\]%-]", "", desc)
    desc = re.sub(r"\s+", " ", desc)
    return desc.strip()

# === Batch Processing ===
def process_all_metadata():
    files = [f for f in os.listdir(RAW_FOLDER) if f.endswith(".json")]
    print(f"Found {len(files)} files in {RAW_FOLDER}...")

    for file in files:
        with open(os.path.join(RAW_FOLDER, file), "r", encoding="utf-8") as f:
            metadata = json.load(f)

        cleaned = clean_metadata(metadata)

        out_path = os.path.join(CLEAN_FOLDER, file)
        with open(out_path, "w", encoding="utf-8") as out_f:
            json.dump(cleaned, out_f, indent=2)

        print(f"✅ Cleaned & saved: {out_path}")

if __name__ == "__main__":
    process_all_metadata()
