"""
Generate surge.ico — redesigned icon.

Visual design:
  • Deep charcoal rounded-square background with a subtle purple glow border
  • Bold purple lightning bolt with a white specular highlight on the top-left edge
  • Soft purple radial glow behind the bolt
  • Drop shadow layer for depth at large sizes
  • All 7 standard Windows icon sizes: 16 24 32 48 64 128 256
"""
import io, math, struct
from PIL import Image, ImageDraw, ImageFilter

SIZES = [16, 24, 32, 48, 64, 128, 256]

# ── Palette ───────────────────────────────────────────────────────────────────
BG_OUTER   = (10,  10,  12, 255)   # very dark background
BG_INNER   = (20,  20,  24, 255)   # slightly lighter centre
ACCENT     = (108,  99, 255, 255)  # surge purple #6C63FF
ACCENT_LT  = (155, 148, 255, 255)  # lighter purple for highlight edge
GLOW_CLR   = (108,  99, 255,  55)  # semi-transparent purple for glow
BORDER_CLR = ( 90,  80, 220, 160)  # border ring
WHITE_HI   = (255, 255, 255, 180)  # specular highlight on bolt


def _lerp_color(a, b, t):
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(4))


def _bolt_pts(size: int):
    """
    Redesigned bolt — wider, more dynamic.
    Defined on a 0–1 unit grid then scaled.

        Top-right tip
         ↓
         *
        /|
       / |
      *--+   ← mid-right kink
      |
      +--*   ← mid-left kink
      |   \\
      |    \\
      *     ← bottom-left tip
    """
    raw = [
        (0.68, 0.02),   # top tip — pushed up and right
        (0.22, 0.52),   # mid-left top — wider spread
        (0.48, 0.49),   # mid-right top (inner notch)
        (0.32, 0.98),   # bottom tip — pushed to bottom edge
        (0.78, 0.51),   # mid-right bottom — wider spread
        (0.52, 0.53),   # mid-left bottom (inner notch)
    ]
    return [(x * size, y * size) for x, y in raw]


def _highlight_pts(size: int):
    """Thin bright strip on the top-left face of the bolt."""
    raw = [
        (0.68, 0.02),
        (0.22, 0.52),
        (0.30, 0.52),
        (0.72, 0.05),
    ]
    return [(x * size, y * size) for x, y in raw]


def make_frame(size: int) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    r = max(2, size // 8)

    # ── 1. Background rounded square ─────────────────────────────────────
    draw.rounded_rectangle([0, 0, size - 1, size - 1], radius=r, fill=BG_OUTER)

    # Subtle inner gradient — draw concentric slightly-lighter rounded rect
    inset = max(1, size // 16)
    draw.rounded_rectangle(
        [inset, inset, size - 1 - inset, size - 1 - inset],
        radius=max(2, r - inset),
        fill=BG_INNER
    )

    # ── 2. Red glow behind bolt (blurred circle) ──────────────────────────
    if size >= 32:
        glow = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        gd   = ImageDraw.Draw(glow)
        cx, cy = size * 0.5, size * 0.52
        gr = size * 0.38
        gd.ellipse([cx - gr, cy - gr, cx + gr, cy + gr], fill=GLOW_CLR)
        glow = glow.filter(ImageFilter.GaussianBlur(radius=size * 0.14))
        img  = Image.alpha_composite(img, glow)
        draw = ImageDraw.Draw(img)

    # ── 3. Bolt drop-shadow (large sizes only) ────────────────────────────
    if size >= 48:
        shadow_layer = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        sd = ImageDraw.Draw(shadow_layer)
        offset = max(1, size // 32)
        shadow_pts = [(x + offset, y + offset) for x, y in _bolt_pts(size)]
        sd.polygon(shadow_pts, fill=(0, 0, 0, 120))
        shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(radius=size * 0.04))
        img  = Image.alpha_composite(img, shadow_layer)
        draw = ImageDraw.Draw(img)

    # ── 4. Main bolt ──────────────────────────────────────────────────────
    bolt_pts = _bolt_pts(size)
    draw.polygon(bolt_pts, fill=ACCENT)

    # ── 5. Specular highlight strip ───────────────────────────────────────
    if size >= 24:
        hi_pts = _highlight_pts(size)
        draw.polygon(hi_pts, fill=WHITE_HI)

    # ── 6. Thin accent border around the whole tile ───────────────────────
    if size >= 32:
        bw = max(1, size // 48)
        draw.rounded_rectangle(
            [bw, bw, size - 1 - bw, size - 1 - bw],
            radius=r - bw,
            outline=BORDER_CLR,
            width=bw
        )

    # Clip to rounded rectangle mask so corners are transparent
    mask = Image.new("L", (size, size), 0)
    md   = ImageDraw.Draw(mask)
    md.rounded_rectangle([0, 0, size - 1, size - 1], radius=r, fill=255)
    img.putalpha(mask)

    return img


def build_ico(out_path: str):
    frames = [make_frame(sz) for sz in SIZES]

    raw: list[bytes] = []
    for frame in frames:
        buf = io.BytesIO()
        frame.save(buf, format="PNG")
        raw.append(buf.getvalue())

    n = len(SIZES)
    header_size = 6 + 16 * n
    offsets: list[int] = []
    offset = header_size
    for data in raw:
        offsets.append(offset)
        offset += len(data)

    with open(out_path, "wb") as f:
        f.write(struct.pack("<HHH", 0, 1, n))
        for i, sz in enumerate(SIZES):
            w = sz if sz < 256 else 0
            h = sz if sz < 256 else 0
            f.write(struct.pack("<BBBBHHII",
                w, h, 0, 0, 1, 32, len(raw[i]), offsets[i]
            ))
        for data in raw:
            f.write(data)

    total = sum(len(d) for d in raw)
    print(f"Written: {out_path}  ({total // 1024} KB,  {n} sizes: {SIZES})")


if __name__ == "__main__":
    import os
    os.chdir(r"D:\SurgeApp")
    build_ico("surge.ico")
    print("Done. Preview the icon by opening surge.ico in Windows Explorer.")
