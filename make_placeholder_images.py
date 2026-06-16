"""Generate placeholder branded images for PMax asset group.
Created so asset group API creation meets minimum image requirements.
User will replace these with real brand images in the UI.
"""
import io, os
from PIL import Image, ImageDraw, ImageFont

OUT_DIR = os.path.dirname(__file__)

BRAND_GREEN = (34, 68, 48)
CREAM = (245, 239, 223)

def _font(size):
    for path in (
        "/System/Library/Fonts/Supplemental/Georgia.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
    ):
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            pass
    return ImageFont.load_default()

def make(size, text_lines, path, bg=BRAND_GREEN, fg=CREAM):
    W, H = size
    im = Image.new("RGB", size, bg)
    d = ImageDraw.Draw(im)
    # Main title
    y = H // 2 - (len(text_lines) * 48) // 2
    for i, (txt, sz) in enumerate(text_lines):
        f = _font(sz)
        bbox = d.textbbox((0, 0), txt, font=f)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        d.text(((W - tw) // 2, y), txt, fill=fg, font=f)
        y += th + 16
    im.save(path, "PNG", optimize=True)
    return path

def main():
    landscape = make(
        (1200, 628),
        [("Dr. Ruth Roberts", 84), ("Holistic Pet Wellness", 56)],
        os.path.join(OUT_DIR, "pmax_landscape.png"),
    )
    square = make(
        (1200, 1200),
        [("Dr. Ruth Roberts", 96), ("Holistic Pet", 80), ("Wellness", 80)],
        os.path.join(OUT_DIR, "pmax_square.png"),
    )
    # Logo: cream background, brand text
    logo = make(
        (1200, 1200),
        [("Dr. Ruth", 160), ("Roberts", 160), ("DVM", 110)],
        os.path.join(OUT_DIR, "pmax_logo.png"),
        bg=CREAM, fg=BRAND_GREEN,
    )
    print(landscape)
    print(square)
    print(logo)

if __name__ == "__main__":
    main()
