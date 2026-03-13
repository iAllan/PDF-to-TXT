import re
import sys
from collections import Counter

def clean_output_file(input_file, output_file=None, max_newlines=2):
    """
    Remove <!-- image --> tags and collapse multiple newlines.
    If output_file is None, overwrite the original.
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Remove all <!-- image --> tags
    cleaned = re.sub(r'<!-- image -->', '', content)

    # 2. Collapse consecutive newlines (3 or more) to max_newlines
    #    This regex matches 3 or more newlines (including carriage returns)
    cleaned = re.sub(r'\n{3,}', '\n' * max_newlines, cleaned)

    out_path = output_file if output_file else input_file
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(cleaned)

    print(f"Cleaned file saved to {out_path}")

def analyze_output(output_file, expected_pages=None):
    with open(output_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split by page markers (adjust regex to match your format)
    page_marker = r'^--- Page (\d+) ---$'
    parts = re.split(page_marker, content, flags=re.MULTILINE)
    # parts will be: [text_before, page_num1, page_text1, page_num2, page_text2, ...]
    pages = {}
    for i in range(1, len(parts)-1, 2):
        page_num = int(parts[i])
        raw_text = parts[i+1].strip()

        # ----- Sanitization: remove image tags -----
        sanitized_text = re.sub(r'<!-- image -->', '', raw_text).strip()
        # ------------------------------------------

        pages[page_num] = sanitized_text

    print(f"Found {len(pages)} pages in output (after removing image tags).")

    # Basic stats
    lengths = [len(t) for t in pages.values()]
    avg_len = sum(lengths)/len(lengths) if lengths else 0
    print(f"Average text length per page: {avg_len:.1f} chars")
    print(f"Min length: {min(lengths)} chars, Max length: {max(lengths)} chars")

    # Identify potentially problematic pages
    suspicious = []
    for num, text in pages.items():
        if len(text) < 100:
            suspicious.append((num, "too short", len(text)))
        elif len(text.split()) < 10:
            suspicious.append((num, "too few words", len(text.split())))
        # Add more heuristics as needed

    if suspicious:
        print("\nSuspicious pages:")
        for num, reason, value in suspicious:
            print(f"  Page {num}: {reason} ({value})")
    else:
        print("\nAll pages appear reasonable.")

    if expected_pages:
        if len(pages) == expected_pages:
            print(f"✅ Page count matches expected {expected_pages}.")
        else:
            print(f"❌ Expected {expected_pages} pages, but found {len(pages)}.")

    return pages

if __name__ == "__main__":
    output_file = sys.argv[1] if len(sys.argv) > 1 else "module_1_extracted_range_1-20.txt"
    expected = 20  # adjust as needed
    clean_output_file(output_file)
    analyze_output(output_file, expected)