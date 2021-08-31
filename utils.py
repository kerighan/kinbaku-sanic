import asyncio

from sanic import response

# =============================================================================
# WRAPPER
# =============================================================================


def json_task(func):
    async def wrapper(*args, **kwargs):
        task = await func(*args, **kwargs)
        while task.get_pending():
            await asyncio.sleep(1e-5)
        return response.json(task.get_data(), task.get_status())
    return wrapper
