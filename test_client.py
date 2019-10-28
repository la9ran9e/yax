from yax.worker.client import Client

c = Client("localhost", 5101)
print(c.apply({"id": 1}))
print(c.apply({"id": 1}))
print(c.apply({"id": 1}))
print(c.apply({"id": 2}))
print(c.apply({"id": 3}))
print(c.apply({"id": 3}))