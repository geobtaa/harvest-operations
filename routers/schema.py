from fastapi import APIRouter, HTTPException
from utils.file_io import load_local_schema

router = APIRouter()

@router.get("/schema")
async def get_schema():
    """Return the current local metadata schema."""
    schema = load_local_schema()
    return schema

@router.get("/distribution-types")
async def get_distribution_types():
    """Return the current distribution types schema."""
    from utils.file_io import load_yaml_file  # assumes this exists
    return load_yaml_file("schemas/distribution_types.yaml")