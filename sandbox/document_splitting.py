import fitz  # PyMuPDF
import re
import os
from logger_tt import getLogger

log = getLogger(__name__)

def find_toc_page(file_path, toc_keywords):
    """
    Automatically locate the TOC (table of contents) page in the PDF by searching for a keyword.

    Args:
        file_path (str): Path to the input PDF file.
        toc_keyword (str): Keyword to identify the TOC page.

    Returns:
        int: The page number of the TOC, or -1 if not found.
    """
    # Open the PDF document
    doc = fitz.open(file_path)

    # Search for the TOC keyword in the document
    for page_number in range(len(doc)):
        page_text = doc[page_number].get_text("text")
        for kw in toc_keywords:
            if kw in page_text:
                return page_number

    return -1  # Return -1 if TOC page is not found


def detect_chapters_with_toc_skip(file_path, toc_page_end):
    """
    Detects chapters in a PDF by skipping TOC and filtering valid chapters.

    Args:
        file_path (str): Path to the input PDF file.
        toc_page_end (int): Page number after which to start detecting chapters.

    Returns:
        dict: Valid chapter titles with their starting pages.
    """
    # Open the PDF document
    doc = fitz.open(file_path)

    # Define a regex to detect valid chapter headings
    chapter_pattern = re.compile(r"^\d+\s+[A-Za-zÅÄÖåäö]+.*$", re.MULTILINE)

    # Detect chapter headings and their start pages
    chapter_start_pages = {}
    for page_number in range(toc_page_end, len(doc)):  # Skip TOC and earlier pages
        page_text = doc[page_number].get_text("dict")
        blocks = page_text['blocks']
        lines = []
        for b in blocks:
            if 'lines' in b:
                for l in b['lines']:
                    for s in l['spans']:
                        if s['size']>9: # To skip footnotes
                            lines.append(s['text'])

        #lines = page_text.split("\n")  # Process line-by-line
        for line in lines:
            match = chapter_pattern.match(line.strip())
            if match:
                chapter_title = match.group().strip()
                # Exclude invalid patterns
                if "Dnr" not in chapter_title and "mg" not in chapter_title:
                    if chapter_title not in chapter_start_pages:
                        chapter_start_pages[chapter_title] = page_number

    # Sequential filtering to ensure correct order
    valid_chapters = {}
    expected_chapter_number = 1
    for chapter, start_page in sorted(chapter_start_pages.items(), key=lambda x: x[1]):
        match_number = re.match(r"^(\d+)\s", chapter)
        if match_number:
            chapter_number = int(match_number.group(1))
            if chapter_number == expected_chapter_number:
                valid_chapters[chapter] = start_page
                expected_chapter_number += 1

    return valid_chapters

def skip_footer(page):
    rect = page.rect
    height = 50
    clip = fitz.Rect(0, height, rect.width, rect.height-height)
    return page.get_text(clip=clip)

def ensure_directory_exists(directory_path):
    """
    Ensures that the specified directory exists.
    If it doesn't, it will be created.

    Args:
        directory_path (str): Path to the directory.
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def split_preamble_and_chapters_safe(file_path, output_dir, toc_keywords=["Innehåll","Content"]):
    """
    Splits the PDF into a preamble and chapter files, ensuring directories exist.

    Args:
        file_path (str): Path to the input PDF file.
        output_dir (str): Directory to save the output files.
        toc_keyword (str): Keyword to identify the TOC page.

    Returns:
        list: Paths to the generated files (preamble and chapters).
    """
    # Ensure output directory exists
    ensure_directory_exists(output_dir)

    # Find the TOC page
    toc_page = find_toc_page(file_path, toc_keywords)
    if toc_page == -1:
        log.info("TOC keyword not found in the document {}.".format(file_path))
        return False

    # Save the preamble (pages before the TOC)
    doc = fitz.open(file_path)
    preamble_doc = fitz.open()
    preamble_doc.insert_pdf(doc, from_page=0, to_page=toc_page - 1)
    preamble_file = f"{output_dir}/Preamble.pdf"
    preamble_doc.save(preamble_file)
    preamble_doc.close()

    # Detect valid chapters starting after the TOC
    valid_chapters = detect_chapters_with_toc_skip(file_path, toc_page + 1)

    # Adjust chapter ranges to handle shared pages
    adjusted_chapters = {}
    sorted_chapters = sorted(valid_chapters.items(), key=lambda x: x[1])

    for i, (chapter_title, start_page) in enumerate(sorted_chapters):
        if i < len(sorted_chapters) - 1:
            # End at the start of the next chapter, even if it's the same page
            _, next_start_page = sorted_chapters[i + 1]
            end_page = next_start_page - 1 if next_start_page > start_page else start_page
        else:
            # Last chapter ends at the last page of the document
            end_page = len(doc) - 1
        adjusted_chapters[chapter_title] = (start_page, end_page)

    # Save each chapter into separate PDF files
    output_files = []
    for chapter_title, (start_page, end_page) in adjusted_chapters.items():
        chapter_doc = fitz.open()
        chapter_doc.insert_pdf(doc, from_page=start_page, to_page=end_page)
        # Sanitize chapter title for filenames
        sanitized_title = re.sub(r"[^\w\s-]", "", chapter_title).strip().replace(" ", "_")
        output_file = f"{output_dir}/Chapter_{sanitized_title}.pdf"
        chapter_doc.save(output_file)
        output_files.append(output_file)
        chapter_doc.close()

    # Return all generated files (preamble first, then chapters)
    return [preamble_file] + output_files