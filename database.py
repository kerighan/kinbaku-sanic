import asyncio
import queue
import random
import threading
import time
from dataclasses import dataclass
from enum import Enum

import kinbaku as kn


class Action(Enum):
    CLOSE = 0
    COUNT = 1
    ADD_EDGE = 2
    ADD_NODE = 3
    REMOVE_EDGE = 4
    REMOVE_NODE = 5
    GET_EDGE = 6
    GET_NODE = 7
    GET_BATCH_EDGES = 8
    GET_BATCH_NODES = 9
    GET_NEIGHBORS = 10
    SET_NEIGHBORS = 11
    GET_PREDECESSORS = 12
    SET_PREDECESSORS = 13


class ThreadedGraph(threading.Thread):
    def __init__(self, G, daemon=True, maxsize=1000000):
        super().__init__()
        self.G = G
        self.q = queue.PriorityQueue()
        self.setDaemon(daemon)
        self.start()

    def run(self):
        while True:
            _, (task, action, arg) = self.q.get()

            if action == Action.ADD_EDGE:
                self.handle_add_edge(task, arg)
            elif action == Action.ADD_NODE:
                self.handle_add_node(task, arg)
            elif action == Action.GET_EDGE:
                self.handle_get_edge(task, arg)
            elif action == Action.GET_NODE:
                self.handle_get_node(task, arg)
            elif action == Action.GET_BATCH_EDGES:
                self.handle_get_batch_edges(task, arg)
            elif action == Action.GET_BATCH_NODES:
                self.handle_get_batch_nodes(task, arg)
            elif action == Action.GET_NEIGHBORS:
                self.handle_get_neighbors(task, arg)
            elif action == Action.SET_NEIGHBORS:
                self.handle_set_neighbors(task, arg)
            elif action == Action.GET_PREDECESSORS:
                self.handle_get_predecessors(task, arg)
            elif action == Action.REMOVE_EDGE:
                self.handle_remove_edge(task, arg)
            elif action == Action.REMOVE_NODE:
                self.handle_remove_node(task, arg)
            elif action == Action.COUNT:
                self.handle_count(task)
            elif action == Action.CLOSE:
                break

            self.q.task_done()
        print("closing...")

    def put(self, priority, action, arg=None):
        task = Task()
        self.q.put((priority, (task, action, arg)))
        return task

    def close(self):
        return self.put(2, Action.CLOSE)

    def add_edge(self, u, v):
        return self.put(1, Action.ADD_EDGE, (u, v))

    def add_node(self, u):
        return self.put(1, Action.ADD_NODE, u)

    def remove_edge(self, u, v):
        return self.put(1, Action.REMOVE_EDGE, (u, v))

    def remove_node(self, u):
        return self.put(1, Action.REMOVE_NODE, u)

    def edge(self, u, v):
        return self.put(0, Action.GET_EDGE, (u, v))

    def node(self, u):
        return self.put(0, Action.GET_NODE, u)

    def batch_get_edges(self, batch_size=100, cursor=0):
        return self.put(0, Action.GET_BATCH_EDGES, (batch_size, cursor))

    def batch_get_nodes(self, batch_size=100, cursor=0):
        return self.put(0, Action.GET_BATCH_NODES, (batch_size, cursor))

    def count(self):
        return self.put(0, Action.COUNT)

    def neighbors(self, u):
        return self.put(0, Action.GET_NEIGHBORS, u)

    def set_neighbors(self, u, nodes):
        return self.put(1, Action.SET_NEIGHBORS, (u, nodes))

    def predecessors(self, u):
        return self.put(0, Action.GET_PREDECESSORS, u)

    def set_predecessors(self, v, nodes):
        return self.put(1, Action.SET_PREDECESSORS, (v, nodes))

    def join(self):
        self.close()
        super().join()

    def handle_add_edge(self, task, arg):
        self.G.add_edge(arg[0], arg[1])
        couple = {"source": arg[0], "target": arg[1]}
        data = {}
        data["edge"] = couple
        data["created"] = True
        task.done(data, 200)

    def handle_add_node(self, task, arg):
        data = self.G.add_node(arg).data()
        data["node"] = arg
        del data["key"]
        data["created"] = True
        task.done(data, 200)

    def handle_remove_edge(self, task, arg):
        try:
            self.G.remove_edge(arg[0], arg[1])
            couple = {"source": arg[0], "target": arg[1]}
            data = {}
            data["edge"] = couple
            data["removed"] = True
            task.done(data, 200)
        except (kn.exception.NodeNotFound, kn.exception.EdgeNotFound):
            data = {"edge": couple, "removed": False}
            task.done(data, 404)

    def handle_get_edge(self, task, arg):
        couple = {"source": arg[0], "target": arg[1]}
        try:
            data = self.G.edge(arg[0], arg[1]).data()
            data["edge"] = couple
            data["found"] = True
            task.done(data, 200)
        except (kn.exception.NodeNotFound, kn.exception.EdgeNotFound):
            data = {}
            data["edge"] = couple
            data["found"] = False
            task.done(data, 404)

    def handle_get_node(self, task, u):
        try:
            data = self.G.node(u).data()
            data["node"] = u
            del data["key"]
            data["found"] = True
            task.done(data, 200)
        except kn.exception.NodeNotFound:
            data = {}
            data["node"] = u
            data["found"] = False
            task.done(data, 404)

    def handle_remove_node(self, task, u):
        try:
            self.G.remove_node(u)
            data = {}
            data["node"] = u
            data["removed"] = True
            task.done(data, 200)
        except kn.exception.NodeNotFound:
            data = {"node": u, "removed": False}
            task.done(data, 404)

    def handle_get_batch_edges(self, task, arg):
        batch_size, cursor = arg
        edges, cursor = self.G.batch_get_edges(
            batch_size=batch_size, cursor=cursor)
        task.done({
            "edges": edges,
            "cursor": cursor}, 200)

    def handle_get_batch_nodes(self, task, arg):
        batch_size, cursor = arg
        nodes, cursor = self.G.batch_get_edges(
            batch_size=batch_size, cursor=cursor)
        task.done({
            "nodes": nodes,
            "cursor": cursor}, 200)

    def handle_count(self, task):
        n_nodes = self.G.n_nodes
        n_edges = self.G.n_edges
        avg_deg = round(n_edges / n_nodes, 0) if n_nodes != 0 else 0
        task.done({"nodes_count": self.G.n_nodes,
                   "edges_count": self.G.n_edges,
                   "avg_degree": avg_deg}, 200)

    def handle_get_neighbors(self, task, arg):
        try:
            data = {
                "node": arg,
                "neighbors": list(self.G.neighbors(arg))
            }
            task.done(data, 200)
        except kn.exception.NodeNotFound as e:
            data = {}
            data["node"] = arg
            data["found"] = False
            task.done(data, 404)

    def handle_set_neighbors(self, task, arg):
        self.G.set_neighbors(arg[0], arg[1])
        data = {"success": True}
        task.done(data, 200)

    def handle_get_predecessors(self, task, arg):
        try:
            data = {
                "node": arg,
                "predecessors": list(self.G.predecessors(arg))
            }
            task.done(data, 200)
        except kn.exception.NodeNotFound as e:
            data = {}
            data["node"] = arg
            data["found"] = False
            task.done(data, 404)

    def handle_set_predecessors(self, task, arg):
        self.G.set_predecessors(arg[0], arg[1])
        data = {"success": True}
        task.done(data, 200)


@dataclass
class Task:
    __slots__ = "pending", "timestamp", "data", "status"

    def __init__(self):
        self.pending = True
        self.timestamp = time.time() + random.random()

    def done(self, data=None, status=None):
        self.pending = False
        if data is not None:
            self.data = data
        if status is not None:
            self.status = status

    async def wait(self):
        while self.pending:
            await asyncio.sleep(1e-5)
