"""Worker functions for the data_store example.

These run inside dispatchio worker subprocesses. DISPATCHIO_JOB_NAME,
DISPATCHIO_RUN_ID, DISPATCHIO_DATA_DIR, and DISPATCHIO_DATA_NAMESPACE are
injected automatically — no manual wiring needed.
"""

from dispatchio.datastore import dispatchio_read_results, dispatchio_write_results


# Producer: return discovered entities; decorator stores them in DataStore.
# Default key is "return_value".
@dispatchio_write_results
def discover() -> list[str]:
    """Simulate discovering which entities changed today."""

    # In a real job, query a database or API instead.
    entities = ["customer_001", "customer_042", "customer_137"]

    print(f"Discovered {len(entities)} entities.")
    return entities


# Consumer: inject entities produced by discover() into the function argument.
# By default this reads key="return_value" for job="discover".
@dispatchio_read_results(
    param="entities",
    job="discover",
    strict=False,
)
def process(entities: list[str] | None = None) -> None:
    """Process entities injected from DataStore."""
    resolved_entities = entities or []
    print(f"Processing {len(resolved_entities)} entities: {resolved_entities}")

    for entity in resolved_entities:
        print(f"  processed {entity}")
