import argparse
import csv
from pathlib import Path
import xml.etree.ElementTree as ET

INPUT_PATH = "blt_fgdc"
OUTPUT_DIR = "blt_fgdc"
SUFFIX = "_attributes.csv"


def _text_or_empty(elem):
    if elem is None or elem.text is None:
        return ""
    return " ".join(elem.text.split())


def _collect_domain_text(attr_elem):
    attrdomv = attr_elem.find("attrdomv")
    if attrdomv is None:
        return ""

    pieces = []
    for node in attrdomv.iter():
        if node is attrdomv:
            continue
        if node.text and node.text.strip():
            pieces.append(" ".join(node.text.split()))

    seen = set()
    unique_pieces = []
    for piece in pieces:
        if piece not in seen:
            unique_pieces.append(piece)
            seen.add(piece)

    return " | ".join(unique_pieces)


def extract_attributes(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]

    rows = []
    for attr in root.findall(".//attr"):
        label = _text_or_empty(attr.find("attrlabl"))
        definition = _text_or_empty(attr.find("attrdef"))
        source = _text_or_empty(attr.find("attrdefs"))
        domain = _collect_domain_text(attr)

        if not any([label, definition, source, domain]):
            continue

        rows.append([label, definition, source, domain])

    return rows


def resolve_input_paths(input_path):
    input_path = Path(input_path)
    if input_path.is_dir():
        return sorted(input_path.glob("*.xml"))
    if input_path.is_file():
        return [input_path]
    return []


def write_csv(rows, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "Attribute_Label",
                "Attribute_Definition",
                "Attribute_Definition_Source",
                "Attribute_Domain",
            ]
        )
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Extract FGDC attribute tables from XML metadata."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=None,
        help="Path to an FGDC XML file or a directory containing XML files.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory for CSV files. Defaults to the input file's directory.",
    )
    parser.add_argument(
        "--suffix",
        default=SUFFIX,
        help="Suffix to append to output CSV filenames.",
    )
    args = parser.parse_args()

    input_path = args.input or INPUT_PATH
    if not input_path:
        raise SystemExit(
            "No input path provided. Set INPUT_PATH in the script or pass a path on the command line."
        )

    xml_files = resolve_input_paths(input_path)
    if not xml_files:
        raise SystemExit(f"No XML files found at: {input_path}")

    output_dir_value = args.output_dir
    if output_dir_value is None:
        output_dir_value = OUTPUT_DIR or None

    output_dir = Path(output_dir_value) if output_dir_value else None

    for xml_path in xml_files:
        rows = extract_attributes(xml_path)
        output_base = xml_path.stem + args.suffix
        csv_path = (output_dir or xml_path.parent) / output_base
        write_csv(rows, csv_path)
        print(f"Wrote {len(rows)} rows to {csv_path}")


if __name__ == "__main__":
    main()
