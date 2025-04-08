import os
import select
import msgpack

def run(handler):
    fd = int(os.environ["HFENDPOINT_FD"])
    unpacker = msgpack.Unpacker()
    reply_buffer = bytearray()

    def send_chunk(request_id, chunk):
        nonlocal reply_buffer
        reply_message = {"id": request_id, "data": chunk}
        reply_packed = msgpack.packb(reply_message, use_bin_type=True)
        reply_buffer.extend(reply_packed)

    read_fds = [fd]

    try:
        while True:
            write_fds = [fd] if reply_buffer else []

            if not read_fds and not write_fds:
                break

            readable, writable, _ = select.select(read_fds, write_fds, [])

            if fd in readable:
                data = os.read(fd, 4096)
                if not data:
                    read_fds = []
                    continue
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

            if fd in writable and reply_buffer:
                written = os.write(fd, reply_buffer)
                if written > 0:
                    del reply_buffer[:written]

    except Exception as e:
        print(f"Worker error: {e}")
    finally:
        os.close(fd)
