# hfendpoint draft

This project is a proof-of-concept for a gateway server designed to
offload AI inference tasks to a dedicated worker process. It serves
models by exposing a set of OpenAI-compatible API endpoints.

The architecture prioritizes adaptability by intentionally decoupling
the public API from the worker's implementation. This abstraction allows
the API to evolve independently of the core worker logic, providing
significant gains in maintainability for a minor performance trade-off.

Communication with the worker is efficiently handled over a Unix socket
using MessagePack for serialization. While the current overhead is
negligible for typical workloads, the layer can be drastically optimized
if performance becomes a concern.

## Install

### From pip

    uv pip install git+https://github.com/angt/hfendpoint-draft@v0.2.0#subdirectory=bindings/python

### From Precompiled Binaries

Binaries are provided for Linux and macOS (`x86_64` and `aarch64`).

    OS=linux     # or macos
    ARCH=x86_64  # or aarch64
    curl -sSf https://github.com/angt/hfendpoint-draft/releases/download/v0.3.0/hfendpoint-$ARCH-$OS.gz | gunzip > hfendpoint
    chmod +x hfendpoint

For convenience, you may want to move the `hfendpoint` binary to a directory included in your `PATH`,
such as `/usr/local/bin`, so you can run it from anywhere.

### From Source (Requires Rust toolchain)

    cargo install --git https://github.com/angt/hfendpoint-draft

## Usage

    $ hfendpoint --help
    Usage: hfendpoint [OPTIONS] <WORKER_PATH> [WORKER_ARGS]...

    Arguments:
      <WORKER_PATH>     Path to the worker executable
      [WORKER_ARGS]...  Arguments to pass to the worker executable

    Options:
          --host <HOST>
              Host address to bind to [default: 0.0.0.0]
          --port <PORT>
              Port to listen on [default: 3000]
          --max-image-capacity <MAX_IMAGE_CAPACITY>
              Maximum memory capacity for images in bytes [default: 1G]
          --max-body-size <MAX_BODY_SIZE>
              Maximum request body size [default: 10M]
      -h, --help
              Print help
      -V, --version
              Print version

## Endpoints

The API aims to be OpenAI-compatible but some features may be missing or incomplete.

| Endpoint                    | Method  | Description                                       |
|-----------------------------|---------|---------------------------------------------------|
| `/v1/chat/completions`      | POST    | Generate chat completions from a prompt.          |
| `/v1/images/generations`    | POST    | Generate images from a prompt.                    |
| `/v1/images/edits`          | POST    | Edit an image based on a prompt and input image.  |
| `/v1/images/{id}`           | GET     | Retrieve an image by its ID.                      |
| `/v1/images/{id}`           | DELETE  | Delete an image by its ID.                        |
| `/v1/embeddings`            | POST    | Compute vector embeddings for input texts.        |


## Examples of workers

Here are some example workers that demonstrate different capabilities:

 - [hfendpoint-draft-sentence](https://github.com/angt/hfendpoint-draft-sentence):
 Python worker using [sentence-transformers][sentence-transformers].

 - [hfendpoint-draft-llamacpp-embeddings](https://github.com/angt/hfendpoint-draft-llamacpp-embeddings):
 C worker using [llama.cpp][llamacpp].

 - [hfendpoint-draft-sdxl](https://github.com/angt/hfendpoint-draft-sdxl)
 Python worker using StableDiffusionXL from [diffusers][diffusers].


---
 [sentence-transformers]: https://huggingface.co/sentence-transformers
 [llamacpp]: https://github.com/ggml-org/llama.cpp
 [diffusers]: https://github.com/huggingface/diffusers
