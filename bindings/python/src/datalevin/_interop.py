"""Internal interop bindings and public constructors."""

from __future__ import annotations

import json

import jpype

from ._convert import to_java, to_python
from ._java import call_java, classes
from ._jvm import jvm_started, start_jvm
from .errors import DatalevinError

_TIMEOUT_MISSING = object()


def _timeout_arg(timeout_ms):
    if timeout_ms is None:
        return None
    return jpype.JLong(timeout_ms)


class InteropBindings:
    """Thin wrapper around the Datalevin JVM bridge."""

    def api_info_raw(self):
        return call_java(classes().datalevin.apiInfo)

    def exec_json_raw(self, request_json: str) -> str:
        json_api_exec = getattr(classes().json_api, "exec_", None)
        if json_api_exec is None:
            json_api_exec = getattr(classes().json_api, "exec")
        return str(call_java(json_api_exec, request_json))

    def core_invoke(self, function: str, args=None):
        return call_java(classes().interop.coreInvoke, function, to_java(list(args or ())))

    def client_invoke(self, function: str, args=None):
        return call_java(classes().interop.clientInvoke, function, to_java(list(args or ())))

    def create_connection(self, dir=None, schema=None, opts=None, *, shared: bool = False):
        target = classes().interop.getConnection if shared else classes().interop.createConnection
        return call_java(target, dir, to_java(schema), to_java(opts))

    def init_db(self, datoms, dir=None, schema=None, opts=None):
        return call_java(classes().interop.initDb, to_java(datoms), dir, to_java(schema), to_java(opts))

    def fill_db(self, conn, datoms):
        handle = conn.raw_handle() if callable(getattr(conn, "raw_handle", None)) else conn
        return call_java(classes().interop.fillDb, handle, to_java(datoms))

    def close_connection(self, handle) -> None:
        call_java(classes().interop.closeConnection, handle)

    def connection_closed(self, handle) -> bool:
        return bool(call_java(classes().interop.connectionClosed, handle))

    def connection_db(self, handle):
        return call_java(classes().interop.connectionDb, handle)

    def connection_entity(self, handle, eid):
        return call_java(classes().interop.connectionEntity, handle, to_java(eid))

    def database_entid(self, db, eid):
        return call_java(classes().interop.databaseEntid, db, to_java(eid))

    def database_entity(self, db, eid):
        return call_java(classes().interop.databaseEntity, db, to_java(eid))

    def database_entity_map(self, db, eid):
        return call_java(classes().interop.databaseEntityMap, db, to_java(eid))

    def database_pull(self, db, selector, eid):
        return call_java(
            classes().interop.databasePull,
            db,
            selector,
            to_java(eid),
        )

    def database_pull_many(self, db, selector, eids):
        return call_java(
            classes().interop.databasePullMany,
            db,
            selector,
            to_java(eids),
        )

    def database_cardinality(self, db, attr):
        return call_java(classes().interop.databaseCardinality, db, to_java(attr))

    def database_analyze(self, db, attr=None):
        if attr is None:
            return call_java(classes().interop.databaseAnalyze, db)
        return call_java(classes().interop.databaseAnalyze, db, to_java(attr))

    def entity_is(self, value) -> bool:
        if value is None:
            return False
        return bool(call_java(classes().interop.entityIs, value))

    def entity_id(self, entity):
        return call_java(classes().interop.entityId, entity)

    def entity_get(self, entity, attr):
        return call_java(classes().interop.entityGet, entity, to_java(attr))

    def entity_contains(self, entity, attr) -> bool:
        return bool(call_java(classes().interop.entityContains, entity, to_java(attr)))

    def entity_touch(self, entity):
        return call_java(classes().interop.entityTouch, entity)

    def connection_datalog_kv(self, handle):
        return call_java(classes().interop.connectionDatalogKv, handle)

    def connection_transact(self, handle, tx_data, tx_meta=None):
        return call_java(
            classes().interop.connectionTransact,
            handle,
            to_java(tx_data),
            to_java(tx_meta),
        )

    def connection_transact_async(self, handle, tx_data, tx_meta=None):
        return call_java(
            classes().interop.connectionTransactAsync,
            handle,
            to_java(tx_data),
            to_java(tx_meta),
        )

    def connection_tx_data_to_simulated_report(self, handle, tx_data):
        return call_java(
            classes().interop.connectionTxDataToSimulatedReport,
            handle,
            to_java(tx_data),
        )

    def connection_listen(self, handle, key_or_listener, listener=None):
        if listener is None:
            return call_java(classes().interop.connectionListen, handle, key_or_listener)
        return call_java(classes().interop.connectionListen, handle, to_java(key_or_listener), listener)

    def connection_unlisten(self, handle, key):
        call_java(classes().interop.connectionUnlisten, handle, to_java(key))

    def connection_datoms(self, handle, index, c1=None, c2=None, c3=None, limit=None):
        return call_java(
            classes().interop.connectionDatoms,
            handle,
            to_java(index),
            to_java(c1),
            to_java(c2),
            to_java(c3),
            to_java(limit),
        )

    def connection_search_datoms(self, handle, e=None, attr=None, value=None):
        return call_java(
            classes().interop.connectionSearchDatoms,
            handle,
            to_java(e),
            to_java(attr),
            to_java(value),
        )

    def connection_count_datoms(self, handle, e=None, attr=None, value=None):
        return call_java(
            classes().interop.connectionCountDatoms,
            handle,
            to_java(e),
            to_java(attr),
            to_java(value),
        )

    def connection_seek_datoms(self, handle, index, c1=None, c2=None, c3=None, limit=None):
        return call_java(
            classes().interop.connectionSeekDatoms,
            handle,
            to_java(index),
            to_java(c1),
            to_java(c2),
            to_java(c3),
            to_java(limit),
        )

    def connection_rseek_datoms(self, handle, index, c1=None, c2=None, c3=None, limit=None):
        return call_java(
            classes().interop.connectionRseekDatoms,
            handle,
            to_java(index),
            to_java(c1),
            to_java(c2),
            to_java(c3),
            to_java(limit),
        )

    def connection_index_range(self, handle, attr, start, end):
        return call_java(
            classes().interop.connectionIndexRange,
            handle,
            to_java(attr),
            to_java(start),
            to_java(end),
        )

    def connection_fulltext_datoms(self, handle, query, opts=None):
        return call_java(classes().interop.connectionFulltextDatoms, handle, query, to_java(opts))

    def connection_copy(self, handle, dest, compact=None) -> None:
        call_java(classes().interop.connectionCopy, handle, dest, None if compact is None else bool(compact))

    def connection_tx_log_watermarks(self, handle):
        return call_java(classes().interop.connectionTxLogWatermarks, handle)

    def connection_open_tx_log(self, handle, from_lsn, upto_lsn=None):
        return call_java(classes().interop.connectionOpenTxLog, handle, from_lsn, upto_lsn)

    def connection_create_snapshot(self, handle):
        return call_java(classes().interop.connectionCreateSnapshot, handle)

    def connection_list_snapshots(self, handle):
        return call_java(classes().interop.connectionListSnapshots, handle)

    def connection_gc_tx_log_segments(self, handle, retain_floor_lsn=None):
        return call_java(classes().interop.connectionGcTxLogSegments, handle, retain_floor_lsn)

    def connection_with_transaction(self, handle, fn, timeout_ms=_TIMEOUT_MISSING):
        from .connection import Connection

        proxy = jpype.JProxy(
            classes().function_type,
            inst=_PythonFunction(lambda tx: fn(Connection(tx, owned=False))),
        )
        if timeout_ms is not _TIMEOUT_MISSING:
            return call_java(classes().interop.connectionWithTransaction, handle, _timeout_arg(timeout_ms), proxy)
        return call_java(classes().interop.connectionWithTransaction, handle, proxy)

    def connection_abort_transact(self, handle):
        return call_java(classes().interop.connectionAbortTransact, handle)

    def connection_re_index(self, handle, schema=None, opts=None):
        return call_java(classes().interop.connectionReIndex, handle, to_java(schema), to_java(opts))

    def open_key_value(self, dir, opts=None):
        return call_java(classes().interop.openKeyValue, dir, to_java(opts))

    def close_key_value(self, handle) -> None:
        call_java(classes().interop.closeKeyValue, handle)

    def key_value_closed(self, handle) -> bool:
        return bool(call_java(classes().interop.keyValueClosed, handle))

    def key_value_begin_transaction(self, handle):
        return call_java(classes().interop.keyValueBeginTransaction, handle)

    def key_value_commit_transaction(self, tx):
        return call_java(classes().interop.keyValueCommitTransaction, tx)

    def key_value_abort_transaction(self, tx):
        return call_java(classes().interop.keyValueAbortTransaction, tx)

    def key_value_with_transaction(self, handle, fn, timeout_ms=_TIMEOUT_MISSING):
        from .kv import KV

        proxy = jpype.JProxy(
            classes().function_type,
            inst=_PythonFunction(lambda tx: fn(KV(tx, owned=False))),
        )
        if timeout_ms is not _TIMEOUT_MISSING:
            return call_java(classes().interop.keyValueWithTransaction, handle, _timeout_arg(timeout_ms), proxy)
        return call_java(classes().interop.keyValueWithTransaction, handle, proxy)

    def key_value_re_index(self, handle, opts=None):
        return call_java(classes().interop.keyValueReIndex, handle, to_java(opts))

    def kv_get_by_rank(self, handle, dbi_name, rank, k_type, v_type, ignore_key):
        return call_java(
            classes().interop.kvGetByRank,
            handle,
            dbi_name,
            int(rank),
            to_java(k_type),
            to_java(v_type),
            bool(ignore_key),
        )

    def kv_get_entry_by_rank(self, handle, dbi_name, rank, k_type, v_type):
        return call_java(
            classes().interop.kvGetEntryByRank,
            handle,
            dbi_name,
            int(rank),
            to_java(k_type),
            to_java(v_type),
        )

    def kv_sample(self, handle, dbi_name, n, k_type, v_type, ignore_key):
        return call_java(
            classes().interop.kvSample,
            handle,
            dbi_name,
            int(n),
            to_java(k_type),
            to_java(v_type),
            bool(ignore_key),
        )

    def kv_visit(self, handle, dbi_name, visitor, k_range, k_type, v_type):
        return call_java(classes().interop.kvVisit, handle, dbi_name, visitor, k_range, k_type, v_type)

    def kv_visit_raw(self, handle, dbi_name, visitor, k_range, k_type, v_type):
        return call_java(classes().interop.kvVisitRaw, handle, dbi_name, visitor, k_range, k_type, v_type)

    def kv_visit_key_range(self, handle, dbi_name, visitor, k_range, k_type):
        return call_java(classes().interop.kvVisitKeyRange, handle, dbi_name, visitor, k_range, k_type)

    def kv_visit_key_range_raw(self, handle, dbi_name, visitor, k_range, k_type):
        return call_java(classes().interop.kvVisitKeyRangeRaw, handle, dbi_name, visitor, k_range, k_type)

    def kv_visit_list(self, handle, list_name, visitor, key, k_type, v_type):
        return call_java(classes().interop.kvVisitList, handle, list_name, visitor, to_java(key), k_type, v_type)

    def kv_visit_list_raw(self, handle, list_name, visitor, key, k_type):
        return call_java(classes().interop.kvVisitListRaw, handle, list_name, visitor, to_java(key), k_type)

    def kv_visit_list_range(self, handle, list_name, visitor, k_range, k_type, v_range, v_type):
        return call_java(classes().interop.kvVisitListRange, handle, list_name, visitor, k_range, k_type, v_range, v_type)

    def kv_visit_list_range_raw(self, handle, list_name, visitor, k_range, k_type, v_range, v_type):
        return call_java(
            classes().interop.kvVisitListRangeRaw,
            handle,
            list_name,
            visitor,
            k_range,
            k_type,
            v_range,
            v_type,
        )

    def kv_list_range_filter(self, handle, list_name, predicate, k_range, k_type, v_range, v_type):
        return call_java(
            classes().interop.kvListRangeFilter,
            handle,
            list_name,
            predicate,
            k_range,
            k_type,
            v_range,
            v_type,
        )

    def kv_list_range_filter_raw(self, handle, list_name, predicate, k_range, k_type, v_range, v_type):
        return call_java(
            classes().interop.kvListRangeFilterRaw,
            handle,
            list_name,
            predicate,
            k_range,
            k_type,
            v_range,
            v_type,
        )

    def kv_list_range_filter_count(self, handle, list_name, predicate, k_range, k_type, v_range, v_type):
        return call_java(
            classes().interop.kvListRangeFilterCount,
            handle,
            list_name,
            predicate,
            k_range,
            k_type,
            v_range,
            v_type,
        )

    def kv_list_range_filter_count_raw(self, handle, list_name, predicate, k_range, k_type, v_range, v_type):
        return call_java(
            classes().interop.kvListRangeFilterCountRaw,
            handle,
            list_name,
            predicate,
            k_range,
            k_type,
            v_range,
            v_type,
        )

    def kv_list_range_keep(self, handle, list_name, fn, k_range, k_type, v_range, v_type):
        return call_java(
            classes().interop.kvListRangeKeep,
            handle,
            list_name,
            fn,
            k_range,
            k_type,
            v_range,
            v_type,
        )

    def kv_list_range_keep_raw(self, handle, list_name, fn, k_range, k_type, v_range, v_type):
        return call_java(
            classes().interop.kvListRangeKeepRaw,
            handle,
            list_name,
            fn,
            k_range,
            k_type,
            v_range,
            v_type,
        )

    def kv_list_range_some(self, handle, list_name, fn, k_range, k_type, v_range, v_type):
        return call_java(
            classes().interop.kvListRangeSome,
            handle,
            list_name,
            fn,
            k_range,
            k_type,
            v_range,
            v_type,
        )

    def kv_list_range_some_raw(self, handle, list_name, fn, k_range, k_type, v_range, v_type):
        return call_java(
            classes().interop.kvListRangeSomeRaw,
            handle,
            list_name,
            fn,
            k_range,
            k_type,
            v_range,
            v_type,
        )

    def new_search_engine(self, kv, opts=None):
        handle = kv.raw_handle() if callable(getattr(kv, "raw_handle", None)) else kv
        return call_java(classes().interop.newSearchEngine, handle, to_java(opts))

    def search_add_doc(self, search, doc_ref, doc_text, check_exist=None):
        handle = search.raw_handle() if callable(getattr(search, "raw_handle", None)) else search
        return call_java(classes().interop.searchAddDoc, handle, to_java(doc_ref), doc_text, check_exist)

    def search_remove_doc(self, search, doc_ref):
        handle = search.raw_handle() if callable(getattr(search, "raw_handle", None)) else search
        return call_java(classes().interop.searchRemoveDoc, handle, to_java(doc_ref))

    def search_clear_docs(self, search):
        handle = search.raw_handle() if callable(getattr(search, "raw_handle", None)) else search
        return call_java(classes().interop.searchClearDocs, handle)

    def search_doc_indexed(self, search, doc_ref) -> bool:
        handle = search.raw_handle() if callable(getattr(search, "raw_handle", None)) else search
        return bool(call_java(classes().interop.searchDocIndexed, handle, to_java(doc_ref)))

    def search_doc_count(self, search) -> int:
        handle = search.raw_handle() if callable(getattr(search, "raw_handle", None)) else search
        return int(call_java(classes().interop.searchDocCount, handle))

    def search(self, search, query, opts=None):
        handle = search.raw_handle() if callable(getattr(search, "raw_handle", None)) else search
        return call_java(classes().interop.search, handle, query, to_java(opts))

    def search_re_index(self, search, opts=None):
        handle = search.raw_handle() if callable(getattr(search, "raw_handle", None)) else search
        return call_java(classes().interop.searchReIndex, handle, to_java(opts))

    def search_index_writer(self, kv, opts=None):
        handle = kv.raw_handle() if callable(getattr(kv, "raw_handle", None)) else kv
        return call_java(classes().interop.searchIndexWriter, handle, to_java(opts))

    def search_utils_create_analyzer(self, opts=None):
        return call_java(classes().search_utils.createAnalyzer, to_java(opts or {}))

    def search_utils_lower_case_token_filter(self):
        return call_java(classes().search_utils.lowerCaseTokenFilter)

    def search_utils_unaccent_token_filter(self):
        return call_java(classes().search_utils.unaccentTokenFilter)

    def search_utils_create_stop_words_token_filter(self, stop_words_or_predicate):
        return call_java(
            classes().search_utils.createStopWordsTokenFilter,
            to_java(stop_words_or_predicate),
        )

    def search_utils_en_stop_words_token_filter(self):
        return call_java(classes().search_utils.enStopWordsTokenFilter)

    def search_utils_prefix_token_filter(self):
        return call_java(classes().search_utils.prefixTokenFilter)

    def search_utils_create_ngram_token_filter(self, min_gram_size, max_gram_size=None):
        if max_gram_size is None:
            return call_java(classes().search_utils.createNgramTokenFilter, min_gram_size)
        return call_java(
            classes().search_utils.createNgramTokenFilter,
            min_gram_size,
            max_gram_size,
        )

    def search_utils_create_min_length_token_filter(self, min_length):
        return call_java(classes().search_utils.createMinLengthTokenFilter, min_length)

    def search_utils_create_max_length_token_filter(self, max_length):
        return call_java(classes().search_utils.createMaxLengthTokenFilter, max_length)

    def search_utils_create_stemming_token_filter(self, language: str):
        return call_java(classes().search_utils.createStemmingTokenFilter, language)

    def search_utils_create_regexp_tokenizer(self, pattern: str):
        return call_java(classes().search_utils.createRegexpTokenizer, pattern)

    def search_write(self, writer, doc_ref, doc_text):
        handle = writer.raw_handle() if callable(getattr(writer, "raw_handle", None)) else writer
        return call_java(classes().interop.searchWrite, handle, to_java(doc_ref), doc_text)

    def search_commit(self, writer):
        handle = writer.raw_handle() if callable(getattr(writer, "raw_handle", None)) else writer
        return call_java(classes().interop.searchCommit, handle)

    def new_vector_index(self, kv, opts=None):
        handle = kv.raw_handle() if callable(getattr(kv, "raw_handle", None)) else kv
        return call_java(classes().interop.newVectorIndex, handle, to_java(opts))

    def close_vector_index(self, index) -> None:
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        call_java(classes().interop.closeVectorIndex, handle)

    def vector_index_closed(self, index) -> bool:
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        return bool(call_java(classes().interop.vectorIndexClosed, handle))

    def vector_add_vec(self, index, vec_ref, vec_data):
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        return call_java(classes().interop.vectorAddVec, handle, to_java(vec_ref), to_java(vec_data))

    def vector_remove_vec(self, index, vec_ref):
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        return call_java(classes().interop.vectorRemoveVec, handle, to_java(vec_ref))

    def vector_indexed(self, index, vec_ref) -> bool:
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        return bool(call_java(classes().interop.vectorIndexed, handle, to_java(vec_ref)))

    def vector_search(self, index, query_vec, opts=None):
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        return call_java(classes().interop.vectorSearch, handle, to_java(query_vec), to_java(opts))

    def vector_re_index(self, index, opts=None):
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        return call_java(classes().interop.vectorReIndex, handle, to_java(opts))

    def vector_clear(self, index):
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        return call_java(classes().interop.vectorClear, handle)

    def vector_force_checkpoint(self, index):
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        return call_java(classes().interop.vectorForceCheckpoint, handle)

    def vector_info(self, index):
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        return call_java(classes().interop.vectorInfo, handle)

    def vector_checkpoint_state(self, index):
        handle = index.raw_handle() if callable(getattr(index, "raw_handle", None)) else index
        return call_java(classes().interop.vectorCheckpointState, handle)

    def new_llama_embedder(self, model_path, gpu_layers=0, ctx_size=0, batch_size=0, threads=0):
        return call_java(
            classes().interop.newLlamaEmbedder,
            model_path,
            int(gpu_layers),
            int(ctx_size),
            int(batch_size),
            int(threads),
        )

    def close_llama_embedder(self, embedder):
        handle = embedder.raw_handle() if callable(getattr(embedder, "raw_handle", None)) else embedder
        call_java(classes().interop.closeLlamaEmbedder, handle)

    def llama_embedder_closed(self, embedder):
        handle = embedder.raw_handle() if callable(getattr(embedder, "raw_handle", None)) else embedder
        return call_java(classes().interop.llamaEmbedderClosed, handle)

    def llama_embedder_model_path(self, embedder):
        return call_java(classes().interop.llamaEmbedderModelPath, embedder)

    def llama_embedder_gpu_layers(self, embedder):
        return call_java(classes().interop.llamaEmbedderGpuLayers, embedder)

    def llama_embedder_ctx_size(self, embedder):
        return call_java(classes().interop.llamaEmbedderCtxSize, embedder)

    def llama_embedder_context_size(self, embedder):
        return call_java(classes().interop.llamaEmbedderContextSize, embedder)

    def llama_embedder_batch_size(self, embedder):
        return call_java(classes().interop.llamaEmbedderBatchSize, embedder)

    def llama_embedder_threads(self, embedder):
        return call_java(classes().interop.llamaEmbedderThreads, embedder)

    def llama_embedder_dimensions(self, embedder):
        return call_java(classes().interop.llamaEmbedderDimensions, embedder)

    def llama_embedder_embed(self, embedder, text):
        return call_java(classes().interop.llamaEmbedderEmbed, embedder, text)

    def llama_embedder_embed_all(self, embedder, texts):
        return call_java(classes().interop.llamaEmbedderEmbedAll, embedder, to_java(texts))

    def llama_embedder_token_count(self, embedder, text):
        return call_java(classes().interop.llamaEmbedderTokenCount, embedder, text)

    def llama_embedder_tokenize(self, embedder, text):
        return call_java(classes().interop.llamaEmbedderTokenize, embedder, text)

    def llama_embedder_detokenize(self, embedder, tokens):
        return call_java(classes().interop.llamaEmbedderDetokenize, embedder, to_java(tokens))

    def llama_embedder_truncate_text(self, embedder, text, max_tokens):
        return call_java(classes().interop.llamaEmbedderTruncateText, embedder, text, int(max_tokens))

    def new_llama_generator(self, model_path, gpu_layers=0, ctx_size=0, threads=0):
        return call_java(
            classes().interop.newLlamaGenerator,
            model_path,
            int(gpu_layers),
            int(ctx_size),
            int(threads),
        )

    def close_llama_generator(self, generator):
        handle = generator.raw_handle() if callable(getattr(generator, "raw_handle", None)) else generator
        call_java(classes().interop.closeLlamaGenerator, handle)

    def llama_generator_closed(self, generator):
        handle = generator.raw_handle() if callable(getattr(generator, "raw_handle", None)) else generator
        return call_java(classes().interop.llamaGeneratorClosed, handle)

    def llama_generator_model_path(self, generator):
        return call_java(classes().interop.llamaGeneratorModelPath, generator)

    def llama_generator_gpu_layers(self, generator):
        return call_java(classes().interop.llamaGeneratorGpuLayers, generator)

    def llama_generator_ctx_size(self, generator):
        return call_java(classes().interop.llamaGeneratorCtxSize, generator)

    def llama_generator_context_size(self, generator):
        return call_java(classes().interop.llamaGeneratorContextSize, generator)

    def llama_generator_threads(self, generator):
        return call_java(classes().interop.llamaGeneratorThreads, generator)

    def llama_generator_token_count(self, generator, text):
        return call_java(classes().interop.llamaGeneratorTokenCount, generator, text)

    def llama_generator_generate(self, generator, prompt, max_tokens):
        return call_java(classes().interop.llamaGeneratorGenerate, generator, prompt, int(max_tokens))

    def llama_generator_summarize(self, generator, text, max_tokens):
        return call_java(classes().interop.llamaGeneratorSummarize, generator, text, int(max_tokens))

    def new_client(self, uri, opts=None):
        return call_java(classes().interop.newClient, uri, to_java(opts))

    def close_client(self, handle) -> None:
        call_java(classes().interop.closeClient, handle)

    def client_disconnected(self, handle) -> bool:
        return bool(call_java(classes().interop.clientDisconnected, handle))

    def read_edn(self, edn: str):
        return call_java(classes().interop.readEdn, edn)

    def write_edn(self, value) -> str:
        return str(call_java(classes().interop.writeEdn, to_java(value)))

    def keyword(self, value: str):
        return call_java(classes().interop.keyword, value)

    def symbol(self, value: str):
        return call_java(classes().interop.symbol, value)

    def schema(self, schema):
        if schema is None:
            return None
        return call_java(classes().interop.schema, to_java(schema))

    def options(self, opts):
        if opts is None:
            return None
        return call_java(classes().interop.options, to_java(opts))

    def udf_descriptor(self, descriptor):
        if descriptor is None:
            return None
        return call_java(classes().interop.udfDescriptor, to_java(descriptor))

    def create_udf_registry(self):
        return call_java(classes().interop.createUdfRegistry)

    def register_udf(self, registry, descriptor, fn):
        return call_java(classes().interop.registerUdf, registry, to_java(descriptor), fn)

    def unregister_udf(self, registry, descriptor):
        return call_java(classes().interop.unregisterUdf, registry, to_java(descriptor))

    def registered_udf(self, registry, descriptor) -> bool:
        return bool(call_java(classes().interop.registeredUdf, registry, to_java(descriptor)))

    def rename_map(self, rename_map):
        if rename_map is None:
            return None
        return call_java(classes().interop.renameMap, to_java(rename_map))

    def delete_attrs(self, attrs):
        if attrs is None:
            return None
        return call_java(classes().interop.deleteAttrs, to_java(list(attrs or ())))

    def lookup_ref(self, value):
        if value is None:
            return None
        return call_java(classes().interop.lookupRef, to_java(value))

    def datom(self, e, attr, value, tx=None, added=None):
        if added is not None and tx is None:
            raise TypeError("tx is required when added is provided")
        if added is not None:
            return call_java(classes().interop.datom, to_java(e), to_java(attr), to_java(value), to_java(tx), added)
        if tx is not None:
            return call_java(classes().interop.datom, to_java(e), to_java(attr), to_java(value), to_java(tx))
        return call_java(classes().interop.datom, to_java(e), to_java(attr), to_java(value))

    def datom_is(self, value) -> bool:
        return bool(call_java(classes().interop.datomIs, to_java(value)))

    def datom_e(self, datom):
        return call_java(classes().interop.datomE, to_java(datom))

    def datom_a(self, datom):
        return call_java(classes().interop.datomA, to_java(datom))

    def datom_v(self, datom):
        return call_java(classes().interop.datomV, to_java(datom))

    def datom_tx(self, datom):
        return call_java(classes().interop.datomTx, to_java(datom))

    def datom_added(self, datom):
        return call_java(classes().interop.datomAdded, to_java(datom))

    def tx_data(self, tx_data):
        if tx_data is None:
            return None
        return call_java(classes().interop.txData, to_java(tx_data))

    def kv_txs(self, txs):
        if txs is None:
            return None
        return call_java(classes().interop.kvTxs, to_java(txs))

    def kv_type(self, value):
        if value is None:
            return None
        return call_java(classes().interop.kvType, to_java(value))

    def database_type(self, value: str):
        return call_java(classes().interop.databaseType, value)

    def role(self, role: str):
        return call_java(classes().interop.role, role)

    def permission_keyword(self, value: str):
        return call_java(classes().interop.permissionKeyword, value)

    def permission_target(self, object_type: str, target):
        return call_java(classes().interop.permissionTarget, object_type, to_java(target))


_BINDINGS = InteropBindings()
_MISSING = object()


class _PythonFunction:
    def __init__(self, fn) -> None:
        self._fn = fn

    def apply(self, value):
        return to_java(self._fn(value))


def api_info():
    """Return Datalevin JSON/API metadata as a Python dictionary."""

    return to_python(_BINDINGS.api_info_raw())


def exec_json(op: str, args=None):
    """Execute a raw JSON API operation."""

    request = json.dumps({"op": op, "args": args or {}})
    envelope = json.loads(_BINDINGS.exec_json_raw(request))
    if envelope.get("ok"):
        return envelope.get("result")
    raise DatalevinError(
        envelope.get("error") or "Datalevin JSON API request failed.",
        type_name=envelope.get("type"),
        data=envelope.get("data"),
    )


def connect(dir=None, schema=None, opts=None, *, shared: bool = False) -> Connection:
    """Create or open a Datalevin Datalog connection."""

    from .connection import Connection

    return Connection(_BINDINGS.create_connection(dir, schema, opts, shared=shared))


def init_db(datoms, dir=None, schema=None, opts=None) -> Connection:
    """Create a Datalevin Datalog connection by bulk-loading datoms."""

    from .connection import Connection

    return Connection(_BINDINGS.init_db(datoms, dir, schema, opts))


def fill_db(conn: Connection, datoms) -> Connection:
    """Bulk-load datoms into an existing Datalevin Datalog connection."""

    _BINDINGS.fill_db(conn, datoms)
    return conn


def keyword(value: str):
    """Create a Clojure/EDN keyword value for places where a string is literal."""

    return _BINDINGS.keyword(value)


def symbol(value: str):
    """Create a Clojure/EDN symbol value."""

    return _BINDINGS.symbol(value)


def read_edn(edn: str):
    """Read EDN text into JVM-backed Datalevin values."""

    return _BINDINGS.read_edn(edn)


def write_edn(value) -> str:
    """Write an EDN-like Python value as EDN text."""

    from ._convert import to_edn_form

    return _BINDINGS.write_edn(to_edn_form(value))


def schema_attr(
    *,
    value_type=None,
    cardinality=None,
    unique=None,
    index=None,
    fulltext=None,
    is_component=None,
    no_history=None,
    doc=None,
    tuple_type=None,
    tuple_types=None,
    tuple_attrs=None,
    extra=None,
):
    """Build one schema attribute map using the public Python data convention."""

    spec = {}
    if value_type is not None:
        spec[":db/valueType"] = value_type
    if cardinality is not None:
        spec[":db/cardinality"] = cardinality
    if unique is not None:
        spec[":db/unique"] = unique
    if index is not None:
        spec[":db/index"] = bool(index)
    if fulltext is not None:
        spec[":db/fulltext"] = bool(fulltext)
    if is_component is not None:
        spec[":db/isComponent"] = bool(is_component)
    if no_history is not None:
        spec[":db/noHistory"] = bool(no_history)
    if doc is not None:
        spec[":db/doc"] = doc
    if tuple_type is not None:
        spec[":db/tupleType"] = tuple_type
    if tuple_types is not None:
        spec[":db/tupleTypes"] = list(tuple_types)
    if tuple_attrs is not None:
        spec[":db/tupleAttrs"] = list(tuple_attrs)
    if extra:
        spec.update(extra)
    return spec


def fulltext_attr(
    *,
    value_type=":db.type/string",
    domains=None,
    auto_domain=None,
    extra=None,
    **schema_kwargs,
):
    """Build a full-text indexed schema attribute."""

    merged = {":db/fulltext": True}
    if domains is not None:
        merged[":db.fulltext/domains"] = list(domains)
    if auto_domain is not None:
        merged[":db.fulltext/autoDomain"] = bool(auto_domain)
    if extra:
        merged.update(extra)
    return schema_attr(value_type=value_type, extra=merged, **schema_kwargs)


def embedding_attr(
    *,
    value_type=":db.type/string",
    domains=None,
    auto_domain=None,
    extra=None,
    **schema_kwargs,
):
    """Build an embedding-indexed text schema attribute."""

    merged = {":db/embedding": True}
    if domains is not None:
        merged[":db.embedding/domains"] = list(domains)
    if auto_domain is not None:
        merged[":db.embedding/autoDomain"] = bool(auto_domain)
    if extra:
        merged.update(extra)
    return schema_attr(value_type=value_type, extra=merged, **schema_kwargs)


def vector_attr(*, domains=None, extra=None, **schema_kwargs):
    """Build a vector schema attribute."""

    merged = {}
    if domains is not None:
        merged[":db.vec/domains"] = list(domains)
    if extra:
        merged.update(extra)
    return schema_attr(value_type=":db.type/vec", extra=merged, **schema_kwargs)


def idoc_attr(
    *,
    format=None,
    domain=None,
    indexed_paths=None,
    excluded_paths=None,
    extra=None,
    **schema_kwargs,
):
    """Build an indexed-document schema attribute."""

    merged = {}
    if format is not None:
        merged[":db/idocFormat"] = _keyword_value(format)
    if domain is not None:
        merged[":db/domain"] = domain
    if indexed_paths is not None:
        merged[":db.idoc/indexedPaths"] = list(indexed_paths)
    if excluded_paths is not None:
        merged[":db.idoc/excludedPaths"] = list(excluded_paths)
    if extra:
        merged.update(extra)
    return schema_attr(value_type=":db.type/idoc", extra=merged, **schema_kwargs)


def search_options(
    *,
    top=None,
    limit=None,
    offset=None,
    paging_cache_pages=None,
    display=None,
    domains=None,
    proximity_expansion=None,
    proximity_max_dist=None,
    indexing_mode=None,
    extra=None,
):
    """Build full-text query/default options."""

    opts = {}
    if top is not None:
        opts[":top"] = top
    if limit is not None:
        opts[":limit"] = limit
    if offset is not None:
        opts[":offset"] = offset
    if paging_cache_pages is not None:
        opts[":paging-cache-pages"] = paging_cache_pages
    if display is not None:
        opts[":display"] = _keyword_value(display)
    if domains is not None:
        opts[":domains"] = list(domains)
    if proximity_expansion is not None:
        opts[":proximity-expansion"] = proximity_expansion
    if proximity_max_dist is not None:
        opts[":proximity-max-dist"] = proximity_max_dist
    if indexing_mode is not None:
        opts[":indexing-mode"] = _keyword_value(indexing_mode)
    if extra:
        opts.update(extra)
    return opts


def search_domain(*, domain=None, index_position=None, include_text=None, indexing_mode=None, extra=None):
    """Build a full-text domain option map."""

    opts = {}
    if domain is not None:
        opts[":domain"] = domain
    if index_position is not None:
        opts[":index-position?"] = bool(index_position)
    if include_text is not None:
        opts[":include-text?"] = bool(include_text)
    if indexing_mode is not None:
        opts[":indexing-mode"] = _keyword_value(indexing_mode)
    if extra:
        opts.update(extra)
    return opts


def vector_options(
    *,
    dimensions=None,
    metric_type=None,
    quantization=None,
    connectivity=None,
    expansion_add=None,
    expansion_search=None,
    domain=None,
    indexing_mode=None,
    extra=None,
):
    """Build vector index/domain/default options."""

    opts = {}
    if dimensions is not None:
        opts[":dimensions"] = dimensions
    if metric_type is not None:
        opts[":metric-type"] = _keyword_value(metric_type)
    if quantization is not None:
        opts[":quantization"] = _keyword_value(quantization)
    if connectivity is not None:
        opts[":connectivity"] = connectivity
    if expansion_add is not None:
        opts[":expansion-add"] = expansion_add
    if expansion_search is not None:
        opts[":expansion-search"] = expansion_search
    if domain is not None:
        opts[":domain"] = domain
    if indexing_mode is not None:
        opts[":indexing-mode"] = _keyword_value(indexing_mode)
    if extra:
        opts.update(extra)
    return opts


def embedding_options(
    *,
    provider=None,
    model=None,
    base_url=None,
    api_key_env=None,
    request_dimensions=None,
    metric_type=None,
    indexing_mode=None,
    extra=None,
):
    """Build embedding provider/domain/default options."""

    opts = {}
    if provider is not None:
        opts[":provider"] = _keyword_value(provider)
    if model is not None:
        opts[":model"] = model
    if base_url is not None:
        opts[":base-url"] = base_url
    if api_key_env is not None:
        opts[":api-key-env"] = api_key_env
    if request_dimensions is not None:
        opts[":request-dimensions"] = request_dimensions
    if metric_type is not None:
        opts[":metric-type"] = _keyword_value(metric_type)
    if indexing_mode is not None:
        opts[":indexing-mode"] = _keyword_value(indexing_mode)
    if extra:
        opts.update(extra)
    return opts


def idoc_domain(*, indexed_paths=None, excluded_paths=None, extra=None):
    """Build an idoc domain/default option map."""

    opts = {}
    if indexed_paths is not None:
        opts[":indexed-paths"] = list(indexed_paths)
    if excluded_paths is not None:
        opts[":excluded-paths"] = list(excluded_paths)
    if extra:
        opts.update(extra)
    return opts


def idoc_options(*, domains=None, extra=None):
    """Build idoc-match query options."""

    opts = {}
    if domains is not None:
        opts[":domains"] = list(domains)
    if extra:
        opts.update(extra)
    return opts


def tx_entity(db_id=None, attrs=None, **values):
    """Build an entity-map transaction item."""

    entity = {}
    if db_id is not None:
        entity[":db/id"] = db_id
    if attrs:
        entity.update(attrs)
    for attr, value in values.items():
        entity[_attr_key(attr)] = value
    return entity


def tx_add(entity_id, attr, value):
    """Build a :db/add transaction form."""

    return [":db/add", entity_id, _attr_key(attr), value]


def tx_retract(entity_id, attr, value):
    """Build a :db/retract transaction form."""

    return [":db/retract", entity_id, _attr_key(attr), value]


def tx_retract_entity(entity_id):
    """Build a :db/retractEntity transaction form."""

    return [":db/retractEntity", entity_id]


def transact(conn: Connection, tx_data, tx_meta=None):
    """Run a Datalevin transaction and block until it commits."""

    return conn.transact(tx_data, tx_meta)


def transact_async(conn: Connection, tx_data, tx_meta=None):
    """Start an async transaction and return a concurrent.futures.Future."""

    return conn.transact_async(tx_data, tx_meta)


def abort_transact(conn: Connection):
    """Abort the current explicit Datalog transaction for conn."""

    return conn.abort_transact()


def tx_data_to_simulated_report(conn: Connection, tx_data):
    """Return a simulated transaction report without committing tx_data."""

    return conn.tx_data_to_simulated_report(tx_data)


def datalog_kv(conn: Connection) -> KV:
    """Return the borrowed KV handle backing a Datalog connection."""

    from .kv import KV

    handle = conn.raw_handle() if callable(getattr(conn, "raw_handle", None)) else conn
    return KV(_BINDINGS.connection_datalog_kv(handle), owned=False)


def max_eid(conn: Connection):
    """Return the highest allocated entity id for a connection."""

    return conn.max_eid()


def cardinality(conn: Connection, attr):
    """Return the number of distinct values for attr in a connection."""

    return conn.cardinality(attr)


def analyze(conn: Connection, attr=None):
    """Collect query-planner statistics for a connection."""

    return conn.analyze(attr)


def explicit_transaction_timeout(timeout_ms=_MISSING):
    """Get or set the default explicit transaction timeout in milliseconds."""

    args = [] if timeout_ms is _MISSING else [timeout_ms]
    return to_python(_BINDINGS.core_invoke("explicit-transaction-timeout", args))


def set_explicit_transaction_timeout(timeout_ms):
    """Set or clear the default explicit transaction timeout in milliseconds."""

    return to_python(_BINDINGS.core_invoke("set-explicit-transaction-timeout!", [timeout_ms]))


def re_index(target, opts=None, *, schema=None):
    """Rebuild indexes for a Connection, KV, or SearchEngine wrapper."""

    return target.re_index(opts, schema=schema) if schema is not None else target.re_index(opts)


def with_transaction(target, fn, timeout_ms=_MISSING):
    """Run a callback inside a KV or Datalog write transaction."""

    if timeout_ms is _MISSING:
        return target.with_transaction(fn)
    return target.with_transaction(fn, timeout_ms=timeout_ms)



def datom(e, attr, value, tx=_MISSING, added=_MISSING):
    """Create Datom-shaped data for `init_db()` or `fill_db()`."""

    if added is not _MISSING and tx is _MISSING:
        raise TypeError("tx is required when added is provided")
    if added is not _MISSING:
        return (e, attr, value, tx, added)
    if tx is not _MISSING:
        return (e, attr, value, tx)
    return (e, attr, value)


def datom_is(value) -> bool:
    """Return true if value is a Datom or datom-shaped data."""

    return _BINDINGS.datom_is(value)


def datom_e(value):
    """Return the entity id of a Datom or datom-shaped value."""

    return to_python(_BINDINGS.datom_e(value))


def datom_a(value):
    """Return the attribute of a Datom or datom-shaped value."""

    return to_python(_BINDINGS.datom_a(value))


def datom_v(value):
    """Return the value of a Datom or datom-shaped value."""

    return to_python(_BINDINGS.datom_v(value))


def datom_tx(value):
    """Return the transaction id of a Datom or datom-shaped value, or None."""

    return to_python(_BINDINGS.datom_tx(value))


def datom_added(value):
    """Return the assertion flag of a Datom or datom-shaped value, or None."""

    return to_python(_BINDINGS.datom_added(value))


def _attr_key(attr):
    if isinstance(attr, str) and attr.startswith(":"):
        return attr
    return f":{attr}"


def _keyword_value(value):
    text = str(value)
    return text if text.startswith(":") else f":{text}"


def open_kv(dir, opts=None) -> KV:
    """Open a Datalevin KV store."""

    from .kv import KV

    return KV(_BINDINGS.open_key_value(dir, opts))


def new_search_engine(kv: KV, opts=None):
    """Create a full-text search engine for a KV store."""

    from .search import SearchEngine

    return SearchEngine(_BINDINGS.new_search_engine(kv, opts))


def new_vector_index(kv: KV, opts):
    """Create a standalone vector index for a KV store."""

    from .vector import VectorIndex

    return VectorIndex(_BINDINGS.new_vector_index(kv, opts))


def search_index_writer(kv: KV, opts=None):
    """Create a batched full-text search index writer for a KV store."""

    from .search import SearchIndexWriter

    return SearchIndexWriter(_BINDINGS.search_index_writer(kv, opts))


def new_client(uri, opts=None) -> Client:
    """Open a remote Datalevin admin client."""

    from .client import Client

    return Client(_BINDINGS.new_client(uri, opts))


__all__ = [
    "_BINDINGS",
    "api_info",
    "connect",
    "datom",
    "datom_a",
    "datom_added",
    "datom_e",
    "datom_is",
    "datom_tx",
    "datom_v",
    "datalog_kv",
    "embedding_attr",
    "embedding_options",
    "exec_json",
    "fill_db",
    "fulltext_attr",
    "idoc_attr",
    "idoc_domain",
    "idoc_options",
    "init_db",
    "jvm_started",
    "keyword",
    "max_eid",
    "new_client",
    "new_vector_index",
    "open_kv",
    "read_edn",
    "re_index",
    "schema_attr",
    "new_search_engine",
    "search_index_writer",
    "search_domain",
    "search_options",
    "start_jvm",
    "symbol",
    "transact",
    "transact_async",
    "abort_transact",
    "tx_add",
    "tx_data_to_simulated_report",
    "tx_entity",
    "tx_retract",
    "tx_retract_entity",
    "vector_attr",
    "vector_options",
    "with_transaction",
    "write_edn",
]
