package datalevin.jepsen;

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.error.ConnectionFailureException;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RaftRpcFactory;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.BoltRaftRpcFactory;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.SPI;

@SPI(name = "datalevin-jepsen-partitioned", priority = 1000)
public final class PartitionAwareRaftRpcFactory implements RaftRpcFactory {

    private final BoltRaftRpcFactory delegate = new BoltRaftRpcFactory();

    @Override
    public void registerProtobufSerializer(final String className, final Object... args) {
        this.delegate.registerProtobufSerializer(className, args);
    }

    @Override
    public RpcClient createRpcClient(final ConfigHelper<RpcClient> configHelper) {
        return new PartitionAwareRpcClient(PartitionFaults.currentLocalPeerId(),
                                           this.delegate.createRpcClient(configHelper));
    }

    @Override
    public RpcServer createRpcServer(final Endpoint endpoint,
                                     final ConfigHelper<RpcServer> configHelper) {
        return this.delegate.createRpcServer(endpoint, configHelper);
    }

    @Override
    public RpcResponseFactory getRpcResponseFactory() {
        return this.delegate.getRpcResponseFactory();
    }

    @Override
    public void ensurePipeline() {
        this.delegate.ensurePipeline();
    }

    private static final class PartitionAwareRpcClient implements RpcClient {
        private final String localPeerId;
        private final RpcClient delegate;

        private PartitionAwareRpcClient(final String localPeerId, final RpcClient delegate) {
            this.localPeerId = localPeerId;
            this.delegate = delegate;
        }

        @Override
        public boolean init(final RpcOptions opts) {
            return this.delegate.init(opts);
        }

        @Override
        public void shutdown() {
            this.delegate.shutdown();
        }

        @Override
        public boolean checkConnection(final Endpoint endpoint) {
            try {
                delayIfNeeded(endpoint);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                quietlyClose(endpoint);
                return false;
            }
            if (blocked(endpoint)) {
                quietlyClose(endpoint);
                return false;
            }
            return this.delegate.checkConnection(endpoint);
        }

        @Override
        public boolean checkConnection(final Endpoint endpoint, final boolean createIfAbsent) {
            try {
                delayIfNeeded(endpoint);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                quietlyClose(endpoint);
                return false;
            }
            if (blocked(endpoint)) {
                quietlyClose(endpoint);
                return false;
            }
            return this.delegate.checkConnection(endpoint, createIfAbsent);
        }

        @Override
        public void closeConnection(final Endpoint endpoint) {
            this.delegate.closeConnection(endpoint);
        }

        @Override
        public void registerConnectEventListener(final ReplicatorGroup replicatorGroup) {
            this.delegate.registerConnectEventListener(replicatorGroup);
        }

        @Override
        public Object invokeSync(final Endpoint endpoint,
                                 final Object request,
                                 final InvokeContext invokeContext,
                                 final long timeoutMs)
            throws InterruptedException, RemotingException {
            delayIfNeeded(endpoint);
            failIfBlocked(endpoint);
            return this.delegate.invokeSync(endpoint, request, invokeContext, timeoutMs);
        }

        @Override
        public void invokeAsync(final Endpoint endpoint,
                                final Object request,
                                final InvokeContext invokeContext,
                                final InvokeCallback callback,
                                final long timeoutMs)
            throws InterruptedException, RemotingException {
            delayIfNeeded(endpoint);
            failIfBlocked(endpoint);
            this.delegate.invokeAsync(endpoint, request, invokeContext, callback, timeoutMs);
        }

        private boolean blocked(final Endpoint endpoint) {
            return endpoint != null
                && PartitionFaults.shouldDropPeerMessage(this.localPeerId, endpoint.toString());
        }

        private void delayIfNeeded(final Endpoint endpoint) throws InterruptedException {
            if (endpoint == null) {
                return;
            }
            final long delayMs =
                PartitionFaults.peerMessageDelayMs(this.localPeerId, endpoint.toString());
            if (delayMs <= 0L) {
                return;
            }
            Thread.sleep(delayMs);
        }

        private void failIfBlocked(final Endpoint endpoint) throws ConnectionFailureException {
            if (blocked(endpoint)) {
                quietlyClose(endpoint);
                throw new ConnectionFailureException(
                    "Jepsen partition blocks peer traffic from "
                        + this.localPeerId + " to " + endpoint);
            }
        }

        private void quietlyClose(final Endpoint endpoint) {
            try {
                this.delegate.closeConnection(endpoint);
            } catch (final Throwable ignored) {
                // Best effort only.
            }
        }
    }
}
