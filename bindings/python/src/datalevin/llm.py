"""Local llama.cpp embedder and generator wrappers."""

from __future__ import annotations

from ._convert import to_python
from ._interop import _BINDINGS
from ._resource import ResourceWrapper


class LlamaEmbedder(ResourceWrapper):
    """Local llama.cpp-backed text embedder."""

    def __init__(self, handle) -> None:
        super().__init__(handle, _BINDINGS.close_llama_embedder, _BINDINGS.llama_embedder_closed, "llama embedder")

    def model_path(self) -> str:
        return str(_BINDINGS.llama_embedder_model_path(self.raw_handle()))

    def gpu_layers(self) -> int:
        return int(_BINDINGS.llama_embedder_gpu_layers(self.raw_handle()))

    def ctx_size(self) -> int:
        return int(_BINDINGS.llama_embedder_ctx_size(self.raw_handle()))

    def context_size(self) -> int:
        return int(_BINDINGS.llama_embedder_context_size(self.raw_handle()))

    def batch_size(self) -> int:
        return int(_BINDINGS.llama_embedder_batch_size(self.raw_handle()))

    def threads(self) -> int:
        return int(_BINDINGS.llama_embedder_threads(self.raw_handle()))

    def dimensions(self) -> int:
        return int(_BINDINGS.llama_embedder_dimensions(self.raw_handle()))

    def embed(self, text: str) -> list[float]:
        return [float(value) for value in to_python(_BINDINGS.llama_embedder_embed(self.raw_handle(), text))]

    def embed_all(self, texts) -> list[list[float]]:
        return [
            [float(value) for value in vector]
            for vector in to_python(_BINDINGS.llama_embedder_embed_all(self.raw_handle(), texts))
        ]

    def token_count(self, text: str) -> int:
        return int(_BINDINGS.llama_embedder_token_count(self.raw_handle(), text))

    def tokenize(self, text: str) -> list[int]:
        return [int(value) for value in to_python(_BINDINGS.llama_embedder_tokenize(self.raw_handle(), text))]

    def detokenize(self, tokens) -> str:
        return str(_BINDINGS.llama_embedder_detokenize(self.raw_handle(), tokens))

    def truncate_text(self, text: str, max_tokens: int) -> str:
        return str(_BINDINGS.llama_embedder_truncate_text(self.raw_handle(), text, max_tokens))


class LlamaGenerator(ResourceWrapper):
    """Local llama.cpp-backed text generator."""

    def __init__(self, handle) -> None:
        super().__init__(handle, _BINDINGS.close_llama_generator, _BINDINGS.llama_generator_closed, "llama generator")

    def model_path(self) -> str:
        return str(_BINDINGS.llama_generator_model_path(self.raw_handle()))

    def gpu_layers(self) -> int:
        return int(_BINDINGS.llama_generator_gpu_layers(self.raw_handle()))

    def ctx_size(self) -> int:
        return int(_BINDINGS.llama_generator_ctx_size(self.raw_handle()))

    def context_size(self) -> int:
        return int(_BINDINGS.llama_generator_context_size(self.raw_handle()))

    def threads(self) -> int:
        return int(_BINDINGS.llama_generator_threads(self.raw_handle()))

    def token_count(self, text: str) -> int:
        return int(_BINDINGS.llama_generator_token_count(self.raw_handle(), text))

    def generate(self, prompt: str, max_tokens: int) -> str:
        return str(_BINDINGS.llama_generator_generate(self.raw_handle(), prompt, max_tokens))

    def summarize(self, text: str, max_tokens: int) -> str:
        return str(_BINDINGS.llama_generator_summarize(self.raw_handle(), text, max_tokens))


def new_llama_embedder(model_path: str, *, gpu_layers=0, ctx_size=0, batch_size=0, threads=0) -> LlamaEmbedder:
    """Create a local llama.cpp text embedder."""

    return LlamaEmbedder(
        _BINDINGS.new_llama_embedder(
            model_path,
            gpu_layers=gpu_layers,
            ctx_size=ctx_size,
            batch_size=batch_size,
            threads=threads,
        )
    )


def new_llama_generator(model_path: str, *, gpu_layers=0, ctx_size=0, threads=0) -> LlamaGenerator:
    """Create a local llama.cpp text generator."""

    return LlamaGenerator(
        _BINDINGS.new_llama_generator(
            model_path,
            gpu_layers=gpu_layers,
            ctx_size=ctx_size,
            threads=threads,
        )
    )
