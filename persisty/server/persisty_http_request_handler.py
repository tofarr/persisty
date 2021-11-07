from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Type

from marshy.marshaller_context import MarshallerContext

from persisty import PersistyContext
from persisty.obj_graph.entity_abc import EntityABC


class PersistyHttpRequestHandler(BaseHTTPRequestHandler):

    persisty_context: PersistyContext
    marshaller_context: MarshallerContext

    def process_request_headers(self):
        pass

    def send_response_headers(self):
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        pass

    def do_GET(self):
        self.process_request_headers()
        path = self.path
        # Check path...
        path = path.split('/')
        if len(path) < 2:
            self.send_response(404)
            return
        if path[1] == 'entities':
            if len(path) == 2:
                self.do_get_entity_types() # list entity types
                return
            entity_type = path[2]
            if len(path) == 3:
                # List entities (Search parameters from URL)
                return
            if len(path) == 4:
                # Read a single entity
                return
            else:
                # Read multiple entities by id
                return
        self.send_response(404)
        return
        """
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))
        self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
        self.wfile.write(bytes("<body>", "utf-8"))
        self.wfile.write(bytes("<p>This is an example web server.</p>", "utf-8"))
        self.wfile.write(bytes("</body></html>", "utf-8"))
        """

    def do_POST(self):
        path = self.path
        print(path)
        path = self.path.split('/')
        if len(path) < 2:
            self.send_response(404)
            return
        if path[1] == 'entities':
            if len(path) == 2:
                # create a new entity type
                return
            entity_type = path[2]
            if len(path) == 3:
                # create a new entity
                return
            if len(path) == 4:
                # update an entity
                return
        self.send_response(404)
        return

    #def do_OPTIONS(self):
    #    pass

    def do_PUT(self):
        path = self.path
        print(path)
        path = self.path.split('/')
        if len(path) < 2:
            self.send_response(404)
            return
        if path[1] == 'entities':
            if len(path) == 2:
                # update an entity type
                return
            entity_type = path[2]
            if len(path) == 3:
                # update an existing entity
                return
            if len(path) == 4:
                # update an entity - get key from url
                return
        self.send_response(404)
        return

    def do_get_entity_types(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        for entity_type in self.entity_context():
            self.marshaller_context.dump(entity_type.capabilities)
            self.marshaller_context.dump(entity_type.schemas)

        self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))

    def dump_entity_type(self, entity_type: Type[EntityABC]) -> Dict[str, ExternalType]:
        return dict(
            name=entity_type.get_store_name(),
            capabilities=self.marshaller_context.dump(entity_type.capabilities),
            schemas=self.marshaller_context.dump(entity_type.schemas) THIS IS WRONG. IT WILL NOT BE FOR THE ENITTY
        )


if __name__ == "__main__":

    hostName = "localhost"
    serverPort = 8080

    webServer = HTTPServer((hostName, serverPort), EntityHttpRequestHandler)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")