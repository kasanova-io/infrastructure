"""Reviewed source identity; usable by the host binder without database packages."""

SIMPLY_REVISION = "7b01229d8960f8f53f909ee0e69fb628053ac8ae"
SIMPLY_IMAGE_REF = "supertypo/simply-kaspa-indexer@sha256:f28478e046af83b8d490984ca76bd75c663a71e242feba51a176e5ab6ce0a62f"

# Frozen source manifest14870974169e; never a moving tag. Capability activation
# additionally requires host-verified patch labels and the opt-in environment.
SIMPLY_JOURNAL_IMAGE_REF = "sha256:045bb7ada09559048cb867bc5312fba9ac8523f39f025ad507cd924d9097f3d7"
SIMPLY_JOURNAL_PATCH_SHA256 = "760989b4c58c41868c25579260f04a170d81794604adac1058d409026d228e35"


def source_capabilities(image_ref):
    if image_ref == SIMPLY_IMAGE_REF:
        return {}
    if SIMPLY_JOURNAL_IMAGE_REF is not None and image_ref == SIMPLY_JOURNAL_IMAGE_REF:
        return {"block_removals": True, "live_overrides_archive_seed": True}
    raise ValueError("source image is not a reviewed variant")
