"""Shared image safety policy for validation and generation."""
from PIL import Image

# Preserve Pillow's existing warning threshold; its hard limit is twice this.
IMAGE_WARNING_PIXELS = 89_478_485
Image.MAX_IMAGE_PIXELS = IMAGE_WARNING_PIXELS


def pixel_limit_error():
    return f'Image exceeds configured pixel limit ({2 * Image.MAX_IMAGE_PIXELS:,} pixels); review scan dimensions.'
