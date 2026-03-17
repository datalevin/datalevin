package datalevin.ha;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.Describer;
import datalevin.cpp.BufVal;
import datalevin.cpp.Cursor;
import datalevin.cpp.Dbi;
import datalevin.cpp.Env;
import datalevin.cpp.Txn;
import datalevin.cpp.Util;
import datalevin.dtlvnative.DTLV;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LMDBLogStorage implements LogStorage, Describer {

    private static final Logger LOG = LoggerFactory.getLogger(LMDBLogStorage.class);

    private static final String LOG_DBI_NAME = "datalevin.jraft/log";
    private static final String CONF_DBI_NAME = "datalevin.jraft/conf";
    private static final String META_DBI_NAME = "datalevin.jraft/meta";
    private static final String DATA_FILE_NAME = "data.mdb";
    private static final byte[] FIRST_LOG_INDEX_KEY =
        "first-log-index".getBytes(StandardCharsets.UTF_8);
    private static final int MAX_DBS = 4;
    private static final int MAX_READERS = 256;
    private static final long MAP_SIZE_BYTES = 64L * 1024L * 1024L;
    private static final int MAX_MAP_FULL_RETRIES = 6;
    private static final ThreadLocal<BufVal> LONG_KEY_BUF_VAL =
        ThreadLocal.withInitial(() -> new BufVal(Long.BYTES));
    private static final ThreadLocal<BufVal> CURSOR_KEY_BUF_VAL =
        ThreadLocal.withInitial(() -> new BufVal(Long.BYTES));
    private static final ThreadLocal<BufVal> BYTE_KEY_BUF_VAL =
        ThreadLocal.withInitial(() -> new BufVal(1));
    private static final ThreadLocal<BufVal> EMPTY_VAL_BUF_VAL =
        ThreadLocal.withInitial(() -> new BufVal(0));

    private final String path;
    private final boolean sync;
    private final ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();
    private final Lock readLock = this.dbLock.readLock();
    private final Lock writeLock = this.dbLock.writeLock();

    private Env env;
    private Dbi logDbi;
    private Dbi confDbi;
    private Dbi metaDbi;
    private String groupId;
    private LogEntryEncoder logEntryEncoder;
    private LogEntryDecoder logEntryDecoder;
    private volatile long firstLogIndex = 1L;
    private long mapSizeBytes = MAP_SIZE_BYTES;
    private volatile boolean hasLoadedFirstLogIndex;

    public LMDBLogStorage(final String path, final RaftOptions raftOptions) {
        this.path = path;
        this.sync = raftOptions.isSync();
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        this.writeLock.lock();
        try {
            if (this.env != null && !this.env.isClosed()) {
                LOG.warn("LMDBLogStorage init() in {} already.", this.path);
                return true;
            }
            if (opts.getConfigurationManager() == null) {
                throw new IllegalArgumentException("Null conf manager");
            }
            if (opts.getLogEntryCodecFactory() == null) {
                throw new IllegalArgumentException("Null log entry codec factory");
            }
            this.groupId = opts.getGroupId();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            if (this.logEntryEncoder == null || this.logEntryDecoder == null) {
                throw new IllegalArgumentException("Null log entry codec");
            }
            openEnv();
            if (!loadConfigurationEntries(opts.getConfigurationManager())) {
                closeEnv();
                return false;
            }
            return true;
        } catch (final Exception e) {
            LOG.error("Fail to init LMDBLogStorage, path={}.", this.path, e);
            closeEnv();
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            closeEnv();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            if (this.hasLoadedFirstLogIndex) {
                return this.firstLogIndex;
            }
            final Long persisted = readMetaLong(FIRST_LOG_INDEX_KEY);
            if (persisted != null) {
                setFirstLogIndex(persisted);
                return persisted;
            }
            final Long first = getBoundaryIndex(this.logDbi, DTLV.MDB_FIRST);
            if (first != null) {
                setFirstLogIndex(first);
                return first;
            }
            return 1L;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        try {
            final Long last = getBoundaryIndex(this.logDbi, DTLV.MDB_LAST);
            return last == null ? 0L : last;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();
        try {
            if (this.hasLoadedFirstLogIndex && index < this.firstLogIndex) {
                return null;
            }
            return readEntry(index);
        } catch (final RuntimeException e) {
            LOG.error("Fail to get log entry at index {} in data path: {}.",
                index, this.path, e);
            throw e;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getTerm(final long index) {
        final LogEntry entry = getEntry(index);
        return entry == null ? 0L : entry.getId().getTerm();
    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
        return appendEntries(Collections.singletonList(entry)) == 1;
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        this.writeLock.lock();
        try {
            ensureOpen();
            Util.MapFullException lastMapFull = null;
            for (int attempt = 0; attempt <= MAX_MAP_FULL_RETRIES; attempt++) {
                final Txn txn = Txn.create(this.env);
                try {
                    for (final LogEntry entry : entries) {
                        writeEntry(txn, entry);
                    }
                    txn.commit();
                    syncEnv();
                    return entries.size();
                } catch (final Util.MapFullException e) {
                    lastMapFull = e;
                } finally {
                    txn.close();
                }
                if (attempt == MAX_MAP_FULL_RETRIES) {
                    break;
                }
                growMapSizeForAppend(entries, attempt + 1);
            }
            if (lastMapFull != null) {
                LOG.error(
                    "Fail to append {} log entries in data path: {} after {} map resize attempts.",
                    entries.size(), this.path, MAX_MAP_FULL_RETRIES + 1, lastMapFull);
            }
            return 0;
        } catch (final Exception e) {
            LOG.error("Fail to append {} log entries in data path: {}.",
                entries.size(), this.path, e);
            return 0;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        this.writeLock.lock();
        try {
            ensureOpen();
            final long currentFirst = getFirstLogIndex();
            final long currentLast = getLastLogIndex();
            final Txn txn = Txn.create(this.env);
            try {
                if (firstIndexKept < currentFirst) {
                    LOG.warn("Try to truncate logs before {}, but the firstLogIndex is {}.",
                        firstIndexKept, currentFirst);
                    txn.abort();
                    return false;
                }
                final long deleteThrough = Math.min(firstIndexKept - 1, currentLast);
                deleteRange(txn, this.logDbi, currentFirst, deleteThrough);
                deleteRange(txn, this.confDbi, currentFirst, deleteThrough);
                writeMetaLong(txn, FIRST_LOG_INDEX_KEY, firstIndexKept);
                txn.commit();
                syncEnv();
                setFirstLogIndex(firstIndexKept);
                return true;
            } finally {
                txn.close();
            }
        } catch (final Exception e) {
            LOG.error("Fail to truncatePrefix in data path: {}, firstIndexKept={}.",
                this.path, firstIndexKept, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        this.writeLock.lock();
        try {
            ensureOpen();
            final long currentFirst = getFirstLogIndex();
            final long currentLast = getLastLogIndex();
            final Txn txn = Txn.create(this.env);
            try {
                final long deleteFrom = Math.max(currentFirst, lastIndexKept + 1);
                deleteRange(txn, this.logDbi, deleteFrom, currentLast);
                deleteRange(txn, this.confDbi, deleteFrom, currentLast);
                txn.commit();
                syncEnv();
                return true;
            } finally {
                txn.close();
            }
        } catch (final Exception e) {
            LOG.error("Fail to truncateSuffix {} in data path: {}.",
                lastIndexKept, this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.writeLock.lock();
        try {
            ensureOpen();
            LogEntry entry = readEntry(nextLogIndex);
            if (entry == null) {
                entry = new LogEntry();
                entry.setType(EntryType.ENTRY_TYPE_NO_OP);
                entry.setId(new LogId(nextLogIndex, 0));
                LOG.warn("Entry not found for nextLogIndex {} when reset in data path: {}.",
                    nextLogIndex, this.path);
            }
            final Txn txn = Txn.create(this.env);
            try {
                Util.checkRc(DTLV.mdb_drop(txn.get(), this.logDbi.get(), 0));
                Util.checkRc(DTLV.mdb_drop(txn.get(), this.confDbi.get(), 0));
                Util.checkRc(DTLV.mdb_drop(txn.get(), this.metaDbi.get(), 0));
                writeMetaLong(txn, FIRST_LOG_INDEX_KEY, nextLogIndex);
                writeEntry(txn, entry);
                txn.commit();
                syncEnv();
                setFirstLogIndex(nextLogIndex);
                return true;
            } finally {
                txn.close();
            }
        } catch (final Exception e) {
            LOG.error("Fail to reset next log index in data path: {}.", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        final long currentFirst = getFirstLogIndex();
        final long currentLast = getLastLogIndex();
        out.print("  lmdbStorage: [")
            .print(currentFirst)
            .print(", ")
            .print(currentLast)
            .println(']');
        out.print("  path: ").println(this.path);
        out.print("  groupId: ").println(this.groupId);
    }

    private void openEnv() {
        final File dir = new File(this.path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("Failed to create log dir " + this.path);
        }
        this.mapSizeBytes = resolveInitialMapSize(dir);
        this.env = Env.create(this.path, this.mapSizeBytes, MAX_READERS, MAX_DBS, 0);
        this.logDbi = Dbi.create(this.env, LOG_DBI_NAME, DTLV.MDB_CREATE);
        this.confDbi = Dbi.create(this.env, CONF_DBI_NAME, DTLV.MDB_CREATE);
        this.metaDbi = Dbi.create(this.env, META_DBI_NAME, DTLV.MDB_CREATE);
        this.firstLogIndex = 1L;
        this.hasLoadedFirstLogIndex = false;
    }

    private void closeEnv() {
        closeQuietly(this.logDbi);
        closeQuietly(this.confDbi);
        closeQuietly(this.metaDbi);
        closeQuietly(this.env);
        this.logDbi = null;
        this.confDbi = null;
        this.metaDbi = null;
        this.env = null;
        this.firstLogIndex = 1L;
        this.mapSizeBytes = MAP_SIZE_BYTES;
        this.hasLoadedFirstLogIndex = false;
    }

    private void growMapSizeForAppend(final List<LogEntry> entries, final int attempt) {
        final long previousSize = this.mapSizeBytes;
        final long nextSize = nextMapSize(previousSize);
        if (nextSize <= previousSize) {
            throw new IllegalStateException("LMDB log storage map size overflow at "
                + previousSize + " bytes.");
        }
        this.env.setMapSize(nextSize);
        this.mapSizeBytes = nextSize;
        final long firstIndex = entries.get(0).getId().getIndex();
        final long lastIndex = entries.get(entries.size() - 1).getId().getIndex();
        LOG.warn(
            "LMDB log storage map full, resizing map for append retry. path={}, oldMapSizeBytes={}, newMapSizeBytes={}, attempt={}, firstIndex={}, lastIndex={}, entryCount={}",
            this.path, previousSize, nextSize, attempt, firstIndex, lastIndex, entries.size());
    }

    private long resolveInitialMapSize(final File dir) {
        final File dataFile = new File(dir, DATA_FILE_NAME);
        if (!dataFile.exists()) {
            return MAP_SIZE_BYTES;
        }
        return roundUpMapSize(Math.max(MAP_SIZE_BYTES, dataFile.length()));
    }

    private static long nextMapSize(final long currentSize) {
        if (currentSize > Long.MAX_VALUE / 2L) {
            return Long.MAX_VALUE;
        }
        return currentSize * 2L;
    }

    private static long roundUpMapSize(final long size) {
        final long remainder = size % MAP_SIZE_BYTES;
        if (remainder == 0L) {
            return size;
        }
        return size + (MAP_SIZE_BYTES - remainder);
    }

    private void ensureOpen() {
        if (this.env == null || this.env.isClosed()) {
            throw new IllegalStateException("LMDBLogStorage is not initialized.");
        }
    }

    private void setFirstLogIndex(final long index) {
        this.firstLogIndex = index;
        this.hasLoadedFirstLogIndex = true;
    }

    private boolean loadConfigurationEntries(final ConfigurationManager confManager) {
        final Txn txn = Txn.createReadOnly(this.env);
        final BufVal key = cursorKeyBufVal();
        final BufVal val = emptyValBufVal();
        Cursor cursor = null;
        try {
            cursor = Cursor.create(txn, this.confDbi, key, val);
            if (!cursor.seek(DTLV.MDB_FIRST)) {
                return true;
            }
            do {
                final LogEntry entry = decodeEntry(copyBytes(cursor.val()));
                if (!confManager.add(toConfigurationEntry(entry))) {
                    LOG.error("Fail to load configuration entry at path={} index={}.",
                        this.path, entry.getId().getIndex());
                    return false;
                }
            } while (cursor.seek(DTLV.MDB_NEXT));
            return true;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            txn.close();
        }
    }

    private LogEntry readEntry(final long index) {
        ensureOpen();
        final Txn txn = Txn.createReadOnly(this.env);
        try {
            final byte[] bytes = getBytes(txn, this.logDbi, longKeyBufVal(index));
            return bytes == null ? null : decodeEntry(bytes);
        } finally {
            txn.close();
        }
    }

    private void writeEntry(final Txn txn, final LogEntry entry) {
        final long index = entry.getId().getIndex();
        final BufVal key = longKeyBufVal(index);
        final byte[] entryBytes = this.logEntryEncoder.encode(entry);
        putBytes(txn, this.logDbi, key, entryBytes);
        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
            putBytes(txn, this.confDbi, key, entryBytes);
        }
    }

    private void deleteRange(final Txn txn, final Dbi dbi,
                             final long fromIndexInclusive, final long toIndexInclusive) {
        if (fromIndexInclusive > toIndexInclusive) {
            return;
        }
        final BufVal key = cursorKeyBufVal();
        final BufVal val = emptyValBufVal();
        Cursor cursor = null;
        try {
            cursor = Cursor.create(txn, dbi, key, val);
            final BufVal start = longKeyBufVal(fromIndexInclusive);
            if (!cursor.get(start, DTLV.MDB_SET_RANGE)) {
                return;
            }
            while (readCursorKey(cursor) <= toIndexInclusive) {
                cursor.delete(0);
                if (!cursor.seek(DTLV.MDB_NEXT)) {
                    break;
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    private void writeMetaLong(final Txn txn, final byte[] keyBytes, final long value) {
        putBytes(txn, this.metaDbi, keyBytes, longToBytes(value));
    }

    private Long readMetaLong(final byte[] keyBytes) {
        ensureOpen();
        final Txn txn = Txn.createReadOnly(this.env);
        try {
            final byte[] bytes = getBytes(txn, this.metaDbi, keyBytes);
            if (bytes == null) {
                return null;
            }
            return ByteBuffer.wrap(bytes).getLong();
        } finally {
            txn.close();
        }
    }

    private Long getBoundaryIndex(final Dbi dbi, final int op) {
        ensureOpen();
        final Txn txn = Txn.createReadOnly(this.env);
        final BufVal key = cursorKeyBufVal();
        final BufVal val = emptyValBufVal();
        Cursor cursor = null;
        try {
            cursor = Cursor.create(txn, dbi, key, val);
            if (!cursor.seek(op)) {
                return null;
            }
            return cursor.key().outBuf().getLong();
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            txn.close();
        }
    }

    private byte[] getBytes(final Txn txn, final Dbi dbi, final byte[] keyBytes) {
        final BufVal key = byteKeyBufVal(keyBytes);
        return getBytes(txn, dbi, key);
    }

    private byte[] getBytes(final Txn txn, final Dbi dbi, final BufVal key) {
        final BufVal val = emptyValBufVal();
        final int rc = DTLV.mdb_get(txn.get(), dbi.get(), key.ptr(), val.ptr());
        if (rc == DTLV.MDB_NOTFOUND) {
            return null;
        }
        Util.checkRc(rc);
        return copyBytes(val);
    }

    private void putBytes(final Txn txn, final Dbi dbi,
                          final byte[] keyBytes, final byte[] valBytes) {
        final BufVal key = byteKeyBufVal(keyBytes);
        putBytes(txn, dbi, key, valBytes);
    }

    private void putBytes(final Txn txn, final Dbi dbi,
                          final BufVal key, final byte[] valBytes) {
        final BufVal val = newBufVal(valBytes);
        dbi.put(txn, key, val, 0);
    }

    private LogEntry decodeEntry(final byte[] bytes) {
        final LogEntry entry = this.logEntryDecoder.decode(bytes);
        if (entry == null) {
            throw new IllegalStateException(
                "Bad log entry format in data path: " + this.path);
        }
        return entry;
    }

    private ConfigurationEntry toConfigurationEntry(final LogEntry entry) {
        final ConfigurationEntry confEntry = new ConfigurationEntry();
        confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
        confEntry.setConf(new Configuration(entry.getPeers(), entry.getLearners()));
        if (entry.getOldPeers() != null) {
            confEntry.setOldConf(new Configuration(entry.getOldPeers(),
                entry.getOldLearners()));
        }
        return confEntry;
    }

    private void syncEnv() {
        if (this.sync && this.env != null && !this.env.isClosed()) {
            this.env.sync(1);
        }
    }

    private static byte[] copyBytes(final BufVal bufVal) {
        final ByteBuffer buffer = bufVal.outBuf().duplicate();
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static long readCursorKey(final Cursor cursor) {
        return cursor.key().outBuf().duplicate().getLong();
    }

    private static BufVal longKeyBufVal(final long value) {
        final BufVal bufVal = LONG_KEY_BUF_VAL.get();
        final ByteBuffer buffer = bufVal.inBuf();
        buffer.clear();
        buffer.putLong(value);
        buffer.flip();
        bufVal.ptr().mv_size(Long.BYTES);
        return bufVal;
    }

    private static BufVal cursorKeyBufVal() {
        final BufVal bufVal = CURSOR_KEY_BUF_VAL.get();
        bufVal.reset();
        return bufVal;
    }

    private static BufVal emptyValBufVal() {
        final BufVal bufVal = EMPTY_VAL_BUF_VAL.get();
        bufVal.reset();
        return bufVal;
    }

    private static byte[] longToBytes(final long value) {
        final byte[] bytes = new byte[Long.BYTES];
        ByteBuffer.wrap(bytes).putLong(value);
        return bytes;
    }

    private static BufVal newBufVal(final byte[] bytes) {
        final BufVal bufVal = new BufVal(Math.max(1, bytes.length));
        final ByteBuffer buffer = bufVal.inBuf();
        buffer.clear();
        buffer.put(bytes);
        buffer.flip();
        bufVal.ptr().mv_size(bytes.length);
        return bufVal;
    }

    private static BufVal byteKeyBufVal(final byte[] bytes) {
        final int size = Math.max(1, bytes.length);
        BufVal bufVal = BYTE_KEY_BUF_VAL.get();
        if (bufVal.inBuf().capacity() < size) {
            bufVal = new BufVal(size);
            BYTE_KEY_BUF_VAL.set(bufVal);
        }
        final ByteBuffer buffer = bufVal.inBuf();
        buffer.clear();
        buffer.put(bytes);
        buffer.flip();
        bufVal.ptr().mv_size(bytes.length);
        return bufVal;
    }

    private static void closeQuietly(final Dbi dbi) {
        if (dbi != null) {
            try {
                dbi.close();
            } catch (final Exception e) {
                LOG.warn("Failed to close LMDB dbi.", e);
            }
        }
    }

    private static void closeQuietly(final Env env) {
        if (env != null) {
            try {
                env.close();
            } catch (final Exception e) {
                LOG.warn("Failed to close LMDB env.", e);
            }
        }
    }
}
