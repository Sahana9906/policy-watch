from fastapi import UploadFile


MAX_PDF_SIZE_BYTES = 20 * 1024 * 1024
PDF_CONTENT_TYPES = {
    "application/pdf",
    "application/octet-stream",
}


class FileValidationError(ValueError):
    pass


async def validate_pdf_upload(file: UploadFile) -> bytes:
    if not file.filename:
        raise FileValidationError("PDF file must include a filename.")

    if file.content_type and file.content_type not in PDF_CONTENT_TYPES:
        raise FileValidationError("Uploaded file must be a PDF.")

    content = await file.read()
    if not content:
        raise FileValidationError("Uploaded PDF is empty.")

    if len(content) > MAX_PDF_SIZE_BYTES:
        raise FileValidationError("Uploaded PDF exceeds the 20MB size limit.")

    if not content.startswith(b"%PDF"):
        raise FileValidationError("Uploaded file does not appear to be a valid PDF.")

    return content

