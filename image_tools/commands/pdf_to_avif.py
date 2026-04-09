"""PDF をページ単位で AVIF に書き出す。"""

from pathlib import Path

import fitz  # PyMuPDF
from PIL import Image

from image_tools import settings as app_settings
from image_tools.settings import missing_settings_message

try:
    import pillow_avif  # noqa: F401
except ImportError:
    pass


def _paths_and_options():
    s = app_settings.load_settings()
    raw = s.get("PDF2AVIF_INPUT_DIR")
    if raw is None or (isinstance(raw, str) and not str(raw).strip()):
        raise SystemExit(missing_settings_message("PDF2AVIF_INPUT_DIR"))
    input_dir = Path(raw)
    out = s["PDF2AVIF_OUTPUT_DIR"]
    output_dir = Path(out) if out else input_dir / "output"
    dpi = int(s.get("PDF2AVIF_DPI", 200))
    quality = int(s.get("PDF2AVIF_QUALITY", 60))
    return input_dir, output_dir, dpi, quality


def convert_pdf_to_avif(pdf_path: Path, output_dir: Path, dpi: int, quality: int) -> str:
    try:
        folder_name = pdf_path.stem
        target_folder = output_dir / folder_name
        target_folder.mkdir(parents=True, exist_ok=True)

        print(f"処理中: {pdf_path.name}")

        doc = fitz.open(pdf_path)
        zoom = dpi / 72
        mat = fitz.Matrix(zoom, zoom)

        for i, page in enumerate(doc):
            pix = page.get_pixmap(matrix=mat)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            output_file = target_folder / f"{folder_name}_{i + 1:03d}.avif"
            img.save(output_file, "AVIF", quality=quality)

        doc.close()
        return f"完了: {pdf_path.name}"
    except Exception as e:
        return f"エラー ({pdf_path.name}): {e}"


def main() -> None:
    input_dir, output_dir, dpi, quality = _paths_and_options()
    pdf_files = list(input_dir.glob("*.pdf"))

    if not pdf_files:
        print("PDFファイルが見つかりませんでした。")
        return

    for pdf_file in pdf_files:
        print(convert_pdf_to_avif(pdf_file, output_dir, dpi, quality))


if __name__ == "__main__":
    main()
