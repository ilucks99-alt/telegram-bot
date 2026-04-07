import os


def extract_text_from_txt(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def extract_text_from_pdf(path: str) -> str:
    from PyPDF2 import PdfReader

    reader = PdfReader(path)
    parts = []

    for page in reader.pages:
        txt = page.extract_text() or ""
        if txt.strip():
            parts.append(txt)

    return "\n".join(parts).strip()


def extract_text_from_docx(path: str) -> str:
    from docx import Document

    doc = Document(path)
    lines = []

    for p in doc.paragraphs:
        if p.text and p.text.strip():
            lines.append(p.text.strip())

    return "\n".join(lines).strip()


def extract_text_from_file(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()

    if ext == ".pdf":
        return extract_text_from_pdf(path)
    if ext == ".docx":
        return extract_text_from_docx(path)
    if ext == ".txt":
        return extract_text_from_txt(path)

    raise ValueError(f"지원하지 않는 파일 형식입니다: {ext}")