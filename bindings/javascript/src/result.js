import { toJs } from "./convert.js";
import { _BINDINGS } from "./interop.js";

export async function toJsResult(value) {
  return toJs(await _BINDINGS.bridgeResult(value));
}
