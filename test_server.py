from yax.worker.server import Server

server = Server("localhost", 5101, buffer_size=1024, loop=None)
server.run()
