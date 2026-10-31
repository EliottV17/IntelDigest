from importlib.metadata import version


def test_installed_package_metadata_and_runtime_dependencies() -> None:
    import aiohttp
    import asyncpg
    import fastapi
    import lxml
    import pydantic
    import readability
    import redis
    import uvicorn

    assert version("inteldigest") == "0.1.0"
    assert aiohttp.__version__
    assert asyncpg.__version__
    assert fastapi.__version__
    assert lxml.__version__
    assert pydantic.__version__
    assert readability.__name__ == "readability"
    assert redis.__version__
    assert uvicorn.__version__
