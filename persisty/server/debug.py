from strawberry.asgi import GraphQL
from persisty.strawberry import default_schema
from starlette.applications import Starlette

graphql_app = GraphQL(default_schema.schema, debug=True)
starlette_app = Starlette(debug=True)
starlette_app.add_route("/graphql", graphql_app)
starlette_app.add_websocket_route("/graphql", graphql_app)

HOST = "0.0.0.0"
PORT = 8000

if __name__ == "__main__":
    try:
        import uvicorn
    except ImportError:
        raise ValueError(
            "The debug server requires additional packages, install them by running:\n"
            "pip install uvicorn"
        )

    app = "persisty.server.debug:starlette_app"
    print(f"Running persisty on http://{HOST}:{PORT}/graphql")
    uvicorn.run(
        app,
        host=HOST,
        port=PORT,
        log_level="debug",
        reload=True,
        reload_dirs=["."],
    )
