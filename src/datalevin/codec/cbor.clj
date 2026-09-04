;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.codec.cbor
  "Non-durable Phase 0 facade for the purpose-built DL-CBOR codec."
  (:import
   [datalevin.codec DLCbor DLCbor$Limits DLCbor$Mode]
   [java.nio ByteBuffer]))

(def canonical DLCbor$Mode/CANONICAL)
(def fast DLCbor$Mode/FAST)

(defn encoded-size
  (^long [value]
   (DLCbor/encodedSize value canonical))
  (^long [value mode]
   (DLCbor/encodedSize value mode)))

(defn write-item!
  (^long [^ByteBuffer buffer value]
   (DLCbor/write buffer value canonical))
  (^long [^ByteBuffer buffer value mode]
   (DLCbor/write buffer value mode)))

(defn encode
  (^bytes [value]
   (DLCbor/encode value canonical))
  (^bytes [value mode]
   (DLCbor/encode value mode)))

(defn decode
  ([input]
   (if (bytes? input)
     (DLCbor/decode ^bytes input true)
     (DLCbor/decode ^ByteBuffer input true DLCbor$Limits/DEFAULT)))
  ([input canonical?]
   (if (bytes? input)
     (DLCbor/decode ^bytes input (boolean canonical?))
     (DLCbor/decode ^ByteBuffer input (boolean canonical?)
                    DLCbor$Limits/DEFAULT))))

(defn encode-storage
  (^bytes [value]
   (DLCbor/encodeStorage value canonical))
  (^bytes [value mode]
   (DLCbor/encodeStorage value mode)))

(defn decode-storage
  ([^bytes input]
   (DLCbor/decodeStorage input true))
  ([^bytes input canonical?]
   (DLCbor/decodeStorage input (boolean canonical?))))
