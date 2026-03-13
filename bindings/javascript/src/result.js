import { isJavaObject, toJs } from "./convert.js";
import { _BINDINGS } from "./interop.js";

export async function toJsResult(value, { bridge = false } = {}) {
  if (bridge && isJavaObject(value)) {
    return toJs(await _BINDINGS.bridgeResult(value));
  }
  return toJs(value);
}
