;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.hu
  "Fast encoder and decoder for Hu-Tucker codes. Used for key compression."
  (:require
   [datalevin.util :as u :refer [raise]])
  (:import
   [java.util LinkedList Arrays ArrayList]
   [java.nio ByteBuffer ByteOrder]
   [java.io DataOutputStream BufferedOutputStream FileOutputStream
    DataInputStream BufferedInputStream InputStream]
   [org.eclipse.collections.impl.map.mutable.primitive ObjectLongHashMap]
   [datalevin.utl LeftistHeap]))

(defprotocol INode
  (leaf? [_])
  (left-child [_])
  (right-child [_])
  (set-left-child [_ node])
  (set-right-child [_ node]))

;; Find optimal code lengths

(deftype SeqNode [^long sum          ;; sum of min freq and 2nd min freq
                  ^int i             ;; idx of min freq TreeNode
                  ^int j             ;; idx of 2nd min freq TreeNode
                  ^int l             ;; idx of the left terminal TreeNode
                  ^int r             ;; idx of the right terminal TreeNode
                  ^LeftistHeap heap] ;; heap of TreeNodes in this seq
  Object
  (hashCode [_] l)
  (equals [_ other] (= l (.-l ^SeqNode other))))

(defn- master-pq
  []
  (proxy [LeftistHeap] []
    (lessThan [^SeqNode a ^SeqNode b]
      (< (long (u/combine-cmp
                 (compare ^long (.-sum a) ^long (.-sum b))
                 (compare ^int (.-l a) ^int (.-l b)))) ;; tie breaker
         0))))

(defprotocol ITreeNode
  (left-seq [_])
  (right-seq [_])
  (set-left-seq [_ s])
  (set-right-seq [_ s]))

(deftype TreeNode [^int idx
                   ^long freq
                   ^:unsynchronized-mutable ^SeqNode left-seq
                   ^:unsynchronized-mutable ^SeqNode right-seq
                   left-child
                   right-child]
  INode
  (leaf? [_] (nil? left-child))

  ITreeNode
  (left-seq [_] left-seq)
  (right-seq [_] right-seq)
  (set-left-seq [_ s] (set! left-seq s))
  (set-right-seq [_ s] (set! right-seq s))

  Object
  (hashCode [_] idx)
  (equals [_ other] (= idx (.-idx ^TreeNode other))))

(defn- huffman-pq
  []
  (proxy [LeftistHeap] []
    (lessThan [^TreeNode a ^TreeNode b]
      (< (long (u/combine-cmp
                 (compare ^long (.-freq a) ^long (.-freq b))
                 (compare ^int (.-idx a) ^int (.-idx b))))
         0))))

(defn- init-queues
  [n ^longs freqs ^"[Ldatalevin.hu.TreeNode;" work
   ^"[Ldatalevin.hu.TreeNode;" terminals ^LeftistHeap mpq]
  (dotimes [k n]
    (let [node (TreeNode. k (aget freqs k) nil nil nil nil)]
      (aset terminals k node)
      (aset work k node)))
  (dotimes [k (dec ^long n)]
    (let [k+1  (inc k)
          t    ^TreeNode (aget work k)
          t+1  ^TreeNode (aget work k+1)
          wk   (.-freq t)
          wk+1 (.-freq t+1)
          i    (if (<= wk wk+1) k k+1)
          j    (if (= i k) k+1 k)
          hpq  ^LeftistHeap (huffman-pq)
          sn   (SeqNode. (+ wk wk+1) i j k k+1 hpq)]
      (set-right-seq t sn)
      (set-left-seq t+1 sn)
      (.insert hpq t)
      (.insert hpq t+1)
      (.insert mpq sn))))

(defn- build-level-tree
  [^long n ^longs freqs]
  (let [^"[Ldatalevin.hu.TreeNode;" work      (make-array TreeNode n)
        ^"[Ldatalevin.hu.TreeNode;" terminals (make-array TreeNode n)
        ^LeftistHeap mpq                      (master-pq)]
    (init-queues n freqs work terminals mpq)
    (dotimes [_ (dec n)]
      (let [m  ^SeqNode (.findMin mpq)
            i  (.-i m)
            j  (.-j m)
            l  (if (<= i j) i j)
            r  (if (= l i) j i)
            nl (aget work l)
            nr (aget work r)
            nn (TreeNode. l (.-sum m) nil nil nl nr)]
        (aset work l nn)
        (aset work r nil)
        (cond
          ;; combine 2 terminal nodes, need to merge 3 or 2 seqs
          (and (leaf? nl) (leaf? nr))
          (let [tl     (aget terminals l)
                tr     (aget terminals r)
                ll-seq ^SeqNode (left-seq tl)
                l      (if ll-seq (.-l ll-seq) -1)
                ll-hpq (when ll-seq
                         (doto ^LeftistHeap (.-heap ll-seq)
                           (.deleteElement tl)))
                lr-hpq (doto ^LeftistHeap (.-heap ^SeqNode (right-seq tl))
                         (.deleteElement tl))
                _      (doto ^LeftistHeap (.-heap ^SeqNode (left-seq tr))
                         (.deleteElement tr))
                rr-seq ^SeqNode (right-seq tr)
                r      (if rr-seq (.r rr-seq) n)
                rr-hpq (when rr-seq
                         (doto ^LeftistHeap (.-heap rr-seq)
                           (.deleteElement tr)))
                n-hpq  ^LeftistHeap (doto lr-hpq
                                      (.merge ll-hpq) (.merge rr-hpq)
                                      (.insert nn))
                minn   ^TreeNode (.findMin n-hpq)
                minn1  ^TreeNode (.findNextMin n-hpq)
                sn     (SeqNode. (+ (.-freq minn) (.-freq minn1))
                                 (.-idx minn) (.-idx minn1) l r n-hpq)]
            (when-not (= l -1) (set-right-seq (aget terminals l) sn))
            (when-not (= r n) (set-left-seq (aget terminals r) sn))
            (doto mpq
              (.deleteElement ll-seq) (.deleteElement rr-seq)
              (.deleteMin) (.insert sn)))
          ;; combine 2 internal nodes, no seq merge needed
          (and (not (leaf? nl)) (not (leaf? nr)))
          (let [hpq   ^LeftistHeap (.-heap m)
                n-hpq (doto hpq
                        (.deleteMin) (.deleteMin) (.insert nn))
                minn  ^TreeNode (.findMin n-hpq)
                minn1 ^TreeNode (.findNextMin n-hpq)
                l     (.-l m)
                r     (.-r m)
                sn    (when minn1
                        (SeqNode. (+ (.-freq minn) (.-freq minn1))
                                  (.-idx minn) (.-idx minn1) l r n-hpq))]
            (when-not (= l -1) (set-right-seq (aget terminals l) sn))
            (when-not (= r n) (set-left-seq (aget terminals r) sn))
            (doto mpq (.deleteMin) (.insert sn)))
          ;; combine a terminal and an internal node, need to merge two seqs
          :else
          (let [hpq   ^LeftistHeap (.-heap m)
                t     ^TreeNode (if (leaf? nl) nl nr)
                l-seq ^SeqNode (left-seq t)
                l     (if l-seq (.l l-seq) -1)
                l-hpq (when l-seq ^LeftistHeap (.-heap l-seq))
                r-seq ^SeqNode (right-seq t)
                r     (if r-seq (.r r-seq) n)
                r-hpq (when r-seq ^LeftistHeap (.-heap r-seq))
                o-hpq ^SeqNode (if (= hpq l-hpq) r-hpq l-hpq)
                o-seq (if (= o-hpq r-hpq) r-seq l-seq)
                _     (when o-hpq (.deleteElement o-hpq t))
                n-hpq (doto hpq
                        (.deleteMin) (.deleteMin)
                        (.merge o-hpq) (.insert nn))
                minn  ^TreeNode (.findMin n-hpq)
                minn1 ^TreeNode (.findNextMin n-hpq)
                sn    (SeqNode. (+ (.-freq minn) (.-freq minn1))
                                (.-idx minn) (.-idx minn1) l r n-hpq)]
            (when-not (= l -1) (set-right-seq (aget terminals l) sn))
            (when-not (= r n) (set-left-seq (aget terminals r) sn))
            (doto mpq
              (.deleteMin) (.deleteElement o-seq) (.insert sn))))))
    (aget work 0)))

(defn create-levels
  [^long n ^longs freqs]
  (let [tree   (build-level-tree n freqs)
        levels (byte-array n)]
    (letfn [(traverse [^TreeNode node ^long level]
              (if (leaf? node)
                (aset levels (.-idx node) (byte level))
                (let [l+1 (inc level)]
                  (traverse (.-left-child node) l+1)
                  (traverse (.-right-child node) l+1))))]
      (traverse tree 0)
      levels)))

;; Create codes

(deftype Node [level sym left-child right-child]
  INode
  (leaf? [_] (nil? left-child)))

(defn- build-code-tree
  [^long n ^bytes levels]
  (let [cur   (volatile! 0)
        stack (LinkedList.)]
    (while (not (and (= n @cur) (= 1 (.size stack))))
      (if (and (<= 2 (.size stack)) (= (.-level ^Node (.get stack 0))
                                       (.-level ^Node (.get stack 1))))
        (let [top   ^Node (.pop stack)
              top-1 ^Node (.pop stack)
              level (dec ^byte (.-level top))]
          (.push stack (Node. level nil top-1 top)))
        (when (< ^long @cur n)
          (let [sym   @cur
                level (aget levels sym)]
            (.push stack (Node. level sym nil nil))
            (vswap! cur u/long-inc)))))
    (.pop stack)))

(defn create-codes
  [^long n ^bytes lens ^ints codes ^longs freqs]
  (let [levels (create-levels n freqs)
        root   (build-code-tree n levels)]
    (letfn [(traverse [^Node node ^long code]
              (if (leaf? node)
                (let [sym (.-sym node)]
                  (aset lens sym (byte (.-level node)))
                  (when (> (long (.-level node)) 32)
                    (raise "Hu-Tucker code exceeds 32 bits" {:symbol sym}))
                  (aset codes sym (unchecked-int code)))
                (let [code1 (bit-shift-left code 1)]
                  (traverse (.-left-child node) code1)
                  (traverse (.-right-child node) (inc code1)))))]
      (traverse root 0))))

;; Byte-string alphabet. A terminal for an odd final byte precedes every pair
;; starting with that byte; the end-of-key terminal precedes every extension.
(def ^:const end-symbol 0)
(def ^:const symbol-count 65793)

(defn pair-symbol ^long [^long pair]
  (+ 2 pair (unsigned-bit-shift-right pair 8)))

(defn final-byte-symbol ^long [^long b]
  (inc (* 257 b)))

;; Decode each nibble into every completed byte, not just its last symbol.
;; Each step stores a byte count in bits 0..3 and a terminal flag in bit 4.
;; Bits 5+ hold the next table offset, or bits consumed when terminal.
(deftype DecodeNode [sym
                     ^:unsynchronized-mutable left-child
                     ^:unsynchronized-mutable right-child]
  INode
  (leaf? [_] (some? sym))
  (left-child [_] left-child)
  (right-child [_] right-child)
  (set-left-child [_ n] (set! left-child n) n)
  (set-right-child [_ n] (set! right-child n) n))

(defn- build-decode-tree
  [^bytes lens ^ints codes]
  (let [root (DecodeNode. nil nil nil)]
    (dotimes [sym (alength codes)]
      (let [len  (bit-and 0xFF (aget lens sym))
            code (bit-and 0xFFFFFFFF (aget codes sym))]
        (when (or (not (<= 1 len 32)) (>= code (bit-shift-left 1 len)))
          (raise "Invalid Hu-Tucker code" {:symbol sym :length len :code code}))
        (loop [bit (dec len) node root]
          (let [left? (zero? (bit-and 1 (unsigned-bit-shift-right code bit)))
                child (if left? (left-child node) (right-child node))]
            (if (zero? bit)
              (do
                (when child
                  (raise "Overlapping Hu-Tucker codes" {:symbol sym}))
                (let [leaf (DecodeNode. sym nil nil)]
                  (if left? (set-left-child node leaf) (set-right-child node leaf))))
              (let [child (or child
                              (let [n (DecodeNode. nil nil nil)]
                                (if left? (set-left-child node n)
                                    (set-right-child node n))))]
                (when (leaf? child)
                  (raise "Overlapping Hu-Tucker codes" {:symbol sym}))
                (recur (dec bit) child)))))))
    root))

(deftype DecodeTables [^longs outputs ^ints steps])

(defn- fill-decode-entry!
  [root ^ObjectLongHashMap offsets node ^longs outputs ^ints steps
   offset nibble]
  (let [idx (+ (long offset) (long nibble))]
    (loop [remaining (long 4) cur node packed (long 0) byte-count (long 0)]
      (if (zero? remaining)
        (do
          (aset-long outputs idx packed)
          (aset-int steps idx
                    (int (bit-or byte-count (bit-shift-left (.get offsets cur) 5)))))
        (let [next-node (if (bit-test (long nibble) (dec remaining))
                          (right-child cur) (left-child cur))]
          (if (leaf? next-node)
            (let [sym  (long (.-sym ^DecodeNode next-node))
                  end? (zero? sym)
                  odd? (and (pos? sym) (zero? (rem (dec sym) 257)))]
              (if (or end? odd?)
                (do
                  (aset-long outputs idx
                             (if odd?
                               (bit-or (bit-shift-left packed 8) (quot (dec sym) 257))
                               packed))
                  (aset-int steps idx
                            (int (bit-or (if odd? (inc byte-count) byte-count)
                                         0x10 (bit-shift-left (- 5 remaining) 5)))))
                (let [p (- sym 2)
                      word (bit-or (bit-shift-left (quot p 257) 8) (rem p 257))]
                  (recur (dec remaining) root
                         (bit-or (bit-shift-left packed 16) word) (+ byte-count 2)))))
            (recur (dec remaining) next-node packed byte-count)))))))

(defn create-decode-tables
  [^bytes lens ^ints codes]
  (when-not (= symbol-count (alength lens) (alength codes))
    (raise "Invalid Hu-Tucker dictionary size"
             {:lengths (alength lens) :codes (alength codes)}))
  (let [tree     (build-decode-tree lens codes)
        offsets  (ObjectLongHashMap.)
        nodes    (ArrayList.)
        expected (volatile! 0)]
    (letfn [(collect [node]
              (if (leaf? node)
                (do
                  (when-not (= @expected (.-sym ^DecodeNode node))
                    (raise "Hu-Tucker dictionary is not alphabetic" {}))
                  (vswap! expected u/long-inc))
                (do
                  (when-not (and (left-child node) (right-child node))
                    (raise "Incomplete Hu-Tucker dictionary" {}))
                  (.put offsets node (long (* 16 (.size nodes))))
                  (.add nodes node)
                  (collect (left-child node))
                  (collect (right-child node)))))]
      (collect tree))
    (let [size    (* 16 (.size nodes))
          outputs (long-array size)
          steps   (int-array size)]
      (dotimes [i (.size nodes)]
        (dotimes [nibble 16]
          (fill-decode-entry! tree offsets (.get nodes i) outputs steps
                              (* 16 i) nibble)))
      (DecodeTables. outputs steps))))

(defprotocol IEncodeBuf
  (set-br [this b r])
  (get-b [this])
  (get-r [this]))

(deftype EncodeBuf [^:unsynchronized-mutable ^byte b
                    ^:unsynchronized-mutable ^byte r]
  IEncodeBuf
  (set-br [_ bf remain]
    (set! b (unchecked-byte bf))
    (set! r (unchecked-byte remain)))
  (get-b [_] b)
  (get-r [_] r))

(defn- put-code!
  [^ByteBuffer dst bf ^long code ^long len]
  (loop [code (bit-and code 0xFFFFFFFF) len len]
    (let [o (- len (long (get-r bf)))
          b (long (get-b bf))]
      (cond
        (pos? o)
        (do
          (.put dst (unchecked-byte (bit-or b (unsigned-bit-shift-right code o))))
          (set-br bf 0 8)
          (recur (bit-and code (dec (bit-shift-left 1 o))) o))
        (neg? o)
        (set-br bf (bit-or b (bit-shift-left code (- o))) (- o))
        :else
        (do (.put dst (unchecked-byte (bit-or b code))) (set-br bf 0 8))))))

(defn- get-pair ^long [^ByteBuffer src]
  (bit-or (bit-shift-left (bit-and 0xFF (.get src)) 8)
          (bit-and 0xFF (.get src))))

(defn- encode-ordered!
  [^bytes lens ^ints codes ^ByteBuffer src ^ByteBuffer dst]
  (let [bf (EncodeBuf. (byte 0) (byte 8))]
    (while (< 1 (.remaining src))
      (let [sym (pair-symbol (get-pair src))]
        (put-code! dst bf (aget codes sym) (aget lens sym))))
    (let [sym (if (.hasRemaining src)
                (final-byte-symbol (bit-and 0xFF (.get src)))
                end-symbol)]
      (put-code! dst bf (aget codes sym) (aget lens sym)))
    (when (< (long (get-r bf)) 8) (.put dst (byte (get-b bf))))))

(defn- put-decoded!
  [^ByteBuffer dst ^long packed ^long n]
  (loop [shift (* 8 (dec n)) remaining n]
    (when (pos? remaining)
      (.put dst (unchecked-byte (unsigned-bit-shift-right packed shift)))
      (recur (- shift 8) (dec remaining)))))

(defn- decode-ordered!
  [^DecodeTables tables ^ByteBuffer src ^ByteBuffer dst]
  (let [^longs outputs (.-outputs tables)
        ^ints steps   (.-steps tables)]
    (loop [state (long 0) b (long 0) low? false]
      (when (and (not low?) (not (.hasRemaining src)))
        (raise "Missing Hu-Tucker key terminator" {}))
      (let [b     (if low? b (bit-and 0xFF (.get src)))
            idx   (+ state (if low? (bit-and b 0xF) (unsigned-bit-shift-right b 4)))
            step  (aget steps idx)
            count (bit-and step 0xF)]
        (put-decoded! dst (aget outputs idx) count)
        (if (bit-test step 4)
          (let [unused (- (if low? 4 8) (unsigned-bit-shift-right step 5))]
            (when (or (.hasRemaining src)
                      (not (zero? (bit-and b (dec (bit-shift-left 1 unused))))))
              (raise "Trailing data after Hu-Tucker key terminator" {})))
          (recur (unsigned-bit-shift-right step 5) b (not low?)))))))

(defprotocol IHuTucker
  (encode [this src-bf dst-bf])
  (decode [this src-bf dst-bf]))

(deftype HuTucker [^bytes lens ^ints codes ^DecodeTables tables]
  IHuTucker
  (encode [_ src dst] (encode-ordered! lens codes src dst))
  (decode [_ src dst] (decode-ordered! tables src dst)))

(defn codes->hu-tucker
  [^bytes lens ^ints codes]
  (HuTucker. lens codes (create-decode-tables lens codes)))

(defn new-hu-tucker
  "Build an ordered byte-string dictionary from byte-pair and terminal frequencies."
  [^longs freqs]
  (let [n (alength freqs)]
    (when-not (= n symbol-count)
      (raise "Invalid Hu-Tucker frequency array" {:symbols n}))
    (when (some #(not (pos? (long %))) freqs)
      (raise "Hu-Tucker frequencies must be positive" {}))
    (let [lens (byte-array n) codes (int-array n)]
      (create-codes n lens codes freqs)
      (codes->hu-tucker lens codes))))

(def ^:private magic-bytes (.getBytes "HUTU" "US-ASCII"))
(def ^:private ^:const format-version 1)
(def ^:private order ByteOrder/LITTLE_ENDIAN)

(defn dump-hu-tucker
  [^HuTucker hu ^String path]
  (with-open [^DataOutputStream out (DataOutputStream.
                                      (BufferedOutputStream.
                                        (FileOutputStream. path)))]
    (.write out ^bytes magic-bytes)
    (.writeByte out format-version)
    (.writeByte out 0)
    (.writeShort out 0)
    (let [lens ^bytes (.-lens hu) codes ^ints (.-codes hu)]
      (.writeInt out (alength lens))
      (.writeInt out (alength codes))
      (.write out lens)
      (let [bf (doto (ByteBuffer/allocate 4) (.order order))]
        (dotimes [i (alength codes)]
          (.putInt bf 0 (aget codes i))
          (.write out (.array bf)))))))

(defn load-hu-tucker
  "Load an ordered byte-string dictionary."
  [^InputStream is]
  (with-open [in (DataInputStream. (BufferedInputStream. is))]
    (let [magic (byte-array 4)]
      (.readFully in magic)
      (when-not (Arrays/equals magic ^bytes magic-bytes)
        (raise "Invalid magic header" {:magic magic})))
    (let [version (.readUnsignedByte in)
          flags   (.readUnsignedByte in)
          reserved (.readUnsignedShort in)
          llen    (.readInt in)
          lcodes  (.readInt in)]
      (when-not (= version format-version)
        (raise "Unsupported Hu-Tucker dictionary version" {:version version}))
      (when (or (not (zero? flags)) (not (zero? reserved)))
        (raise "Unsupported Hu-Tucker dictionary flags" {:flags flags :reserved reserved}))
      (when-not (= symbol-count llen lcodes)
        (raise "Invalid Hu-Tucker dictionary size"
                 {:version version :lengths llen :codes lcodes}))
      (let [lens (byte-array llen) codes (int-array lcodes)
            ibuf (byte-array 4) bb (doto (ByteBuffer/wrap ibuf) (.order order))]
        (.readFully in lens)
        (dotimes [i lcodes]
          (.readFully in ibuf)
          (aset-int codes i (.getInt bb 0)))
        (codes->hu-tucker lens codes)))))
