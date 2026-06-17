import { _BINDINGS } from "./interop.js";
import { ResourceWrapper } from "./resource.js";
import { toJsResult } from "./result.js";

function numberList(values) {
  return values.map((value) => Number(value));
}

export class LlamaEmbedder extends ResourceWrapper {
  constructor(handle) {
    super(
      handle,
      (rawHandle) => _BINDINGS.closeLlamaEmbedder(rawHandle),
      (rawHandle) => _BINDINGS.llamaEmbedderClosed(rawHandle),
      "llama embedder"
    );
  }

  async modelPath() {
    return toJsResult(await _BINDINGS.llamaEmbedderModelPath(this.rawHandle()));
  }

  async gpuLayers() {
    return Number(await toJsResult(await _BINDINGS.llamaEmbedderGpuLayers(this.rawHandle())));
  }

  async ctxSize() {
    return Number(await toJsResult(await _BINDINGS.llamaEmbedderCtxSize(this.rawHandle())));
  }

  async contextSize() {
    return Number(await toJsResult(await _BINDINGS.llamaEmbedderContextSize(this.rawHandle())));
  }

  async batchSize() {
    return Number(await toJsResult(await _BINDINGS.llamaEmbedderBatchSize(this.rawHandle())));
  }

  async threads() {
    return Number(await toJsResult(await _BINDINGS.llamaEmbedderThreads(this.rawHandle())));
  }

  async dimensions() {
    return Number(await toJsResult(await _BINDINGS.llamaEmbedderDimensions(this.rawHandle())));
  }

  async embed(text) {
    return numberList(await toJsResult(await _BINDINGS.llamaEmbedderEmbed(this.rawHandle(), text)));
  }

  async embedAll(texts) {
    const vectors = await toJsResult(await _BINDINGS.llamaEmbedderEmbedAll(this.rawHandle(), texts));
    return vectors.map(numberList);
  }

  async tokenCount(text) {
    return Number(await toJsResult(await _BINDINGS.llamaEmbedderTokenCount(this.rawHandle(), text)));
  }

  async tokenize(text) {
    return numberList(await toJsResult(await _BINDINGS.llamaEmbedderTokenize(this.rawHandle(), text)));
  }

  async detokenize(tokens) {
    return toJsResult(await _BINDINGS.llamaEmbedderDetokenize(this.rawHandle(), tokens));
  }

  async truncateText(text, maxTokens) {
    return toJsResult(await _BINDINGS.llamaEmbedderTruncateText(this.rawHandle(), text, maxTokens));
  }
}

export class LlamaGenerator extends ResourceWrapper {
  constructor(handle) {
    super(
      handle,
      (rawHandle) => _BINDINGS.closeLlamaGenerator(rawHandle),
      (rawHandle) => _BINDINGS.llamaGeneratorClosed(rawHandle),
      "llama generator"
    );
  }

  async modelPath() {
    return toJsResult(await _BINDINGS.llamaGeneratorModelPath(this.rawHandle()));
  }

  async gpuLayers() {
    return Number(await toJsResult(await _BINDINGS.llamaGeneratorGpuLayers(this.rawHandle())));
  }

  async ctxSize() {
    return Number(await toJsResult(await _BINDINGS.llamaGeneratorCtxSize(this.rawHandle())));
  }

  async contextSize() {
    return Number(await toJsResult(await _BINDINGS.llamaGeneratorContextSize(this.rawHandle())));
  }

  async threads() {
    return Number(await toJsResult(await _BINDINGS.llamaGeneratorThreads(this.rawHandle())));
  }

  async tokenCount(text) {
    return Number(await toJsResult(await _BINDINGS.llamaGeneratorTokenCount(this.rawHandle(), text)));
  }

  async generate(prompt, maxTokens) {
    return toJsResult(await _BINDINGS.llamaGeneratorGenerate(this.rawHandle(), prompt, maxTokens));
  }

  async summarize(text, maxTokens) {
    return toJsResult(await _BINDINGS.llamaGeneratorSummarize(this.rawHandle(), text, maxTokens));
  }
}
