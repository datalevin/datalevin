package datalevin.ha;

import com.alipay.sofa.jraft.JRaftServiceFactory;
import com.alipay.sofa.jraft.entity.codec.LogEntryCodecFactory;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.impl.LocalRaftMetaStorage;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;

public final class LMDBJRaftServiceFactory implements JRaftServiceFactory {

    public static final LMDBJRaftServiceFactory INSTANCE = new LMDBJRaftServiceFactory();

    private LMDBJRaftServiceFactory() {
    }

    @Override
    public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
        return new LMDBLogStorage(uri, raftOptions);
    }

    @Override
    public SnapshotStorage createSnapshotStorage(final NodeOptions nodeOptions) {
        return new LocalSnapshotStorage(nodeOptions.getSnapshotUri(),
            nodeOptions.getSnapshotTempUri(), nodeOptions.getRaftOptions());
    }

    @Override
    public RaftMetaStorage createRaftMetaStorage(final String uri,
                                                 final RaftOptions raftOptions) {
        return new LocalRaftMetaStorage(uri, raftOptions);
    }

    @Override
    public LogEntryCodecFactory createLogEntryCodecFactory() {
        return LogEntryV2CodecFactory.getInstance();
    }
}
