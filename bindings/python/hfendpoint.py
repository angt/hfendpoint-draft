import os
import select
import msgpack

def run(handler):
    request_fd = int(os.environ["HFENDPOINT_FD_REQUEST"])
    reply_fd = int(os.environ["HFENDPOINT_FD_REPLY"])
    unpacker = msgpack.Unpacker()
    reply_buffer = bytearray()

    def send_chunk(request_id, chunk):
        nonlocal reply_buffer
        reply_message = {"id": request_id, "data": chunk}
        reply_packed = msgpack.packb(reply_message, use_bin_type=True)
        reply_buffer.extend(reply_packed)

    try:
        while True:
            read_fds = [request_fd]
            write_fds = [reply_fd] if reply_buffer else []
            readable, writable, _ = select.select(read_fds, write_fds, [])

            if request_fd in readable:
                data = os.read(request_fd, 4096)
                if not data:
                    break
                unpacker.feed(data)
                for message in unpacker:
                    try:
                        print(message)
                        request_id = message["id"]
                        request_name = message["name"]
                        request_data = message["data"]
                        if request_name in handler:
                            handler[request_name](
                                request_data,
                                lambda chunk, rid=request_id: send_chunk(rid, chunk)
                            )
                        else:
                            print(f"No handler for {request_name}")
                    except Exception as e:
                        print(f"Error processing request {request_id}: {e}")

            if reply_fd in writable and reply_buffer:
                written = os.write(reply_fd, reply_buffer)
                if written > 0:
                    del reply_buffer[:written]
                else:
                    raise IOError("Failed to write to reply_fd")

    except Exception as e:
        print(f"Worker error: {e}")
    finally:
        os.close(request_fd)
        os.close(reply_fd)
