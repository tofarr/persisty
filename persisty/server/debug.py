import os

from persisty.access_control.authorization import ROOT, Authorization
from persisty.util import get_logger

DEBUG = int(os.environ.get("SERVER_DEBUG", "1")) == 1
HOST = os.environ.get("SERVER_HOST", "0.0.0.0")
PORT = int(os.environ.get("SERVER_PORT", "8000"))
LOGGER = get_logger(__name__)


async def really_stupid_get_authorization() -> Authorization:
    print("This is very wrong. Replace it with FastAPI OAuth2 JWT!")
    return ROOT


def create_fastapi_app():
    try:
        from fastapi import FastAPI
        from persisty.integration.fastapi.fastapi_route_factory import (
            admin_create_all_routes,
        )

        api = FastAPI(
            title=os.environ.get("FAST_API_TITLE") or "Persisty",
            version=os.environ.get("FAST_API_VERSION") or "0.1.0",
        )
        admin_create_all_routes(api, really_stupid_get_authorization)
        get_logger(__name__).info("FastAPI routes mounted...")
    except ModuleNotFoundError:
        get_logger(__name__).warning(
            "FastAPI not found: Run `pip install strawberry-graphql`"
        )
        return
    add_strawberry_graphql(api)
    return api


def create_starlette_app():
    from starlette.applications import Starlette

    api = Starlette(debug=DEBUG)
    add_strawberry_graphql(api)
    return api


def add_strawberry_graphql(app_):
    logger = get_logger(__name__)
    try:
        from persisty.integration.strawberry import default_schema
        from strawberry.asgi import GraphQL

        graphql_app = GraphQL(default_schema.schema, debug=DEBUG)
        app_.add_route("/graphql", graphql_app)
        app_.add_websocket_route("/graphql", graphql_app)
        logger.info("Graphql mounted at /graphql")
    except ModuleNotFoundError:
        get_logger(__name__).warning(
            "Strawberry GraphQl Not Found. Run `pip install strawberry-graphql`"
        )


app = create_fastapi_app()
if app is None:
    create_starlette_app()


if __name__ == "__main__":
    try:
        import uvicorn
    except ImportError:
        raise ValueError("Additional Packages Required. Run `pip install uvicorn`")

    app = "persisty.server.debug:app"
    print(f"Running persisty on http://{HOST}:{PORT}/")
    uvicorn.run(
        app,
        host=HOST,
        port=PORT,
        log_level="debug",
        reload=True,
        reload_dirs=["."],
    )
