"""Tools for instruction libraries: the fractal's own guidance for AI assistants."""

from .. import http
from ..app import as_json, tool


@tool
async def list_instruction_libraries() -> str:
    """
    List this fractal's instruction libraries.

    Libraries hold the operator's own guidance: playbooks, triage rules, escalation
    procedures. Worth reading at the start of an investigation, since it is where
    environment-specific context lives that no query will tell you.

    Returns:
        Libraries with names, descriptions, page counts, and sync status.
    """
    return as_json(await http.get("/instruction-libraries"))


@tool
async def get_instruction_library(library_id: str) -> str:
    """
    Get a library and the index of its pages.

    Page content is not included; load pages selectively with read_instruction_page.

    Args:
        library_id: The library UUID.

    Returns:
        The library and its page list with names and descriptions.
    """
    return as_json(await http.get(f"/instruction-libraries/{library_id}"))


@tool
async def read_instruction_page(library_id: str, page_id: str) -> str:
    """
    Read one instruction page in full.

    Args:
        library_id: The library UUID holding the page.
        page_id: The page UUID.

    Returns:
        The page's name, description, and instruction text.
    """
    return as_json(await http.get(f"/instruction-libraries/{library_id}/pages/{page_id}"))


@tool
async def create_instruction_library(
    name: str, description: str = "", is_default: bool = False
) -> str:
    """
    Create an instruction library.

    Args:
        name: Library name (e.g. 'SOC Playbooks').
        description: What this library covers.
        is_default: Make it the fractal's default library. Only one can be default.

    Returns:
        The created library.
    """
    body = {
        "name": name,
        "description": description,
        "is_default": is_default,
        "source": "manual",
    }
    return as_json(await http.post("/instruction-libraries", body))


@tool
async def create_instruction_page(
    library_id: str,
    name: str,
    content: str,
    description: str = "",
    always_include: bool = False,
    sort_order: int = 0,
) -> str:
    """
    Add a page to an instruction library.

    Pages marked always_include go into every AI conversation's system prompt; the
    rest appear in an index and are loaded on demand. Reserve always_include for
    guidance that applies to every investigation, since it costs context on all of them.

    Args:
        library_id: The library UUID.
        name: Page name (e.g. 'Incident Response').
        content: The instruction text (plain text or markdown).
        description: Summary shown in the page index.
        always_include: Always inject this page into AI context.
        sort_order: Display order; lower sorts first.

    Returns:
        The created page.
    """
    body = {
        "name": name,
        "description": description,
        "content": content,
        "always_include": always_include,
        "sort_order": sort_order,
    }
    return as_json(await http.post(f"/instruction-libraries/{library_id}/pages", body))


@tool
async def update_instruction_page(
    library_id: str,
    page_id: str,
    name: str,
    content: str,
    description: str = "",
    always_include: bool = False,
    sort_order: int = 0,
) -> str:
    """
    Replace an instruction page's contents.

    Every field is written, so read the page first unless you intend to overwrite it.

    Args:
        library_id: The library UUID holding the page.
        page_id: The page UUID.
        name: Page name.
        content: Instruction text.
        description: Summary for the page index.
        always_include: Always inject this page into AI context.
        sort_order: Display order; lower sorts first.

    Returns:
        The updated page.
    """
    body = {
        "name": name,
        "description": description,
        "content": content,
        "always_include": always_include,
        "sort_order": sort_order,
    }
    return as_json(await http.put(f"/instruction-libraries/{library_id}/pages/{page_id}", body))
