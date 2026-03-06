import re
import sys
from collections import Counter

def analyze_output(output_file, expected_pages=None):
    with open(output_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split by page markers (adjust regex to match your format)
    # If you used "--- Page X ---" as separator:
    page_marker = r'^--- Page (\d+) ---$'
    parts = re.split(page_marker, content, flags=re.MULTILINE)
    # parts will be: [text_before, page_num1, page_text1, page_num2, page_text2, ...]
    pages = {}
    for i in range(1, len(parts)-1, 2):
        page_num = int(parts[i])
        page_text = parts[i+1].strip()
        pages[page_num] = page_text

    print(f"Found {len(pages)} pages in output.")

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
    output_file = sys.argv[1] if len(sys.argv) > 1 else "extracted_text.txt"
    expected = None
    # If you know the total pages, set expected = 1000
    analyze_output(output_file, expected)