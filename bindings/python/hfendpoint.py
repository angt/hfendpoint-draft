import asyncio
import inspect
import sys
import os
import msgpack
import socket

class WorkerError(Exception):
    pass

class HfEndpoint:
    """
    An asyncio-based worker for handling requests from the hfendpoint Rust server.
    """
    def __init__(self):
        self._handlers = {}

    def error(self, message):
        print(message, file=sys.stderr)

    def handler(self, name):
        def decorator(func):
            self._handlers[name] = func
            return func
        return decorator

    async def _writer_loop(self, writer, queue):
        """
        A dedicated task that gets messages from the queue and writes them
        to the output stream. It stops when it receives a `None` sentinel.
        """
        try:
            while True:
                message = await queue.get()
                if message is None:
                    break
                writer.write(message)
                while not queue.empty():
                    try:
                        item = queue.get_nowait()
                        if item is None:
                            await queue.put(None)
                            break
                        writer.write(item)
                    except asyncio.QueueEmpty:
                        break
                await writer.drain()

        except asyncio.CancelledError:
            pass
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def _handle_request(self, queue, request):
        """
        Processes a single request and puts the response(s) onto the queue.
        """
        request_id = request.get("id")
        if request_id is None:
            self.error(f"Received request without id: {request}")
            return

        async def send_chunk(chunk_data):
            reply = {"id": request_id, "data": chunk_data}
            packed_reply = msgpack.packb(reply, use_bin_type=True)
            await queue.put(packed_reply)

        try:
            request_name = request.get("name")
            request_data = request.get("data")
            handler_func = self._handlers.get(request_name)

            if not handler_func:
                raise WorkerError(f"No handler implemented for '{request_name}'")

            result = handler_func(request_data)

            if inspect.isasyncgen(result):
                async for chunk in result:
                    await send_chunk(chunk)
            elif inspect.iscoroutine(result):
                response = await result
                await send_chunk(response)
            else:
                await send_chunk(result)

        except Exception as e:
            reply = {"id": request_id, "error": str(e)}
            packed_reply = msgpack.packb(reply, use_bin_type=True)
            await queue.put(packed_reply)

    async def run(self):
        """
        The main entry point for the worker.
        """
        loop = asyncio.get_running_loop()
        try:
            sock = socket.socket(fileno=int(os.environ["HFENDPOINT_FD"]))
            reader, writer = await asyncio.open_connection(sock=sock)
        except Exception as e:
            self.error(f"Worker failed to open connection: {e}")
            return

        response_queue = asyncio.Queue()
        writer_task = loop.create_task(self._writer_loop(writer, response_queue))
        unpacker = msgpack.Unpacker()
        active_tasks = set()

        try:
            while not reader.at_eof():
                data = await reader.read(1024 * 1024)
                if not data:
                    break
                unpacker.feed(data)
                for message in unpacker:
                    task = loop.create_task(self._handle_request(response_queue, message))
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)
        except asyncio.CancelledError:
            pass
        except Exception:
            self.error("Main run loop encountered a fatal error")
        finally:
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)
            await response_queue.put(None)
            await writer_task

_hfendpoint = HfEndpoint()
handler = _hfendpoint.handler
run = _hfendpoint.run
