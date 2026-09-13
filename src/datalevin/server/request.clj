;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.request
  "Internal completion handoff between routing, handlers, and transaction runners.")

(defn with-completion
  "Attach a server-owned completion callback after decoding a request."
  [message complete!]
  (vary-meta message assoc ::complete! complete!))

(defn complete!
  "Release the connection for its next request. The callback is idempotent;
  synchronous dispatch without a network completion callback is unaffected."
  [message]
  (when-let [f (::complete! (meta message))]
    (f)))
