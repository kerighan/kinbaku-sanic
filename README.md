Kinbaku-sanic
=============

If you haven't already checked, Kinbaku is an open-source graph database.
- **Source:** https://github.com/kerighan/kinbaku
- **Documentation:** https://kinbaku.readthedocs.io

The current repo allows you to setup a Kinbaku server using the Sanic framework. It is not production-ready yet, but can accomplish the following tasks:
- getting, inserting and removing edges and nodes
- querying for neighbors and predecessors
- iterating through the edges and nodes
- basic analytics (edges and nodes count)

Kinbaku is very robust and can handle hundreds of millions of edges.

Setup
-----
Clone this repo and install the required packages:

```python
pip install -r requirements.txt
```

Run a Gunicorn server:
```
gunicorn -b 0.0.0.0:3200 app:app -k sanic.worker.GunicornWorker
```

A word of caution
-----
By default, all CORS are enabled. Besides, no authentication system has been integrated yet.
