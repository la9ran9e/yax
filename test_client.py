from yax.worker.client import Client

INDEX = "test_index"

c = Client("localhost", 9613, index=INDEX)
print(c.apply(1, {"id": 1}))
print(c.apply(1, {"id": 1}))
print(c.apply(1, {"id": 1}))
print(c.apply(2, {"id": 2}))
print(c.apply(3, {"id": 3}))
print(c.apply(3, {"id": 3}))