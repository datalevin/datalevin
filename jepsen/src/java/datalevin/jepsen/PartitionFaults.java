package datalevin.jepsen;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public final class PartitionFaults {

    private static final ThreadLocal<String> CURRENT_LOCAL_PEER_ID = new ThreadLocal<>();
    private static final ConcurrentHashMap<String, NodeRef> PEER_NODES = new ConcurrentHashMap<>();
    private static final Set<LinkKey> DROPPED_LINKS = ConcurrentHashMap.newKeySet();
    private static final ConcurrentHashMap<LinkKey, LinkBehavior> LINK_BEHAVIORS =
        new ConcurrentHashMap<>();

    private PartitionFaults() {
    }

    public static void registerPeer(final String clusterId,
                                    final String logicalNode,
                                    final String peerId) {
        if (clusterId == null || logicalNode == null || peerId == null) {
            return;
        }
        PEER_NODES.put(peerId, new NodeRef(clusterId, logicalNode));
    }

    public static void unregisterCluster(final String clusterId) {
        if (clusterId == null) {
            return;
        }
        PEER_NODES.entrySet().removeIf(e -> clusterId.equals(e.getValue().clusterId));
        DROPPED_LINKS.removeIf(link -> clusterId.equals(link.clusterId));
    }

    public static void setCurrentLocalPeerId(final String peerId) {
        if (peerId == null) {
            CURRENT_LOCAL_PEER_ID.remove();
        } else {
            CURRENT_LOCAL_PEER_ID.set(peerId);
        }
    }

    public static void clearCurrentLocalPeerId() {
        CURRENT_LOCAL_PEER_ID.remove();
    }

    public static String currentLocalPeerId() {
        return CURRENT_LOCAL_PEER_ID.get();
    }

    public static void dropLink(final String clusterId,
                                final String srcLogicalNode,
                                final String destLogicalNode) {
        if (clusterId == null || srcLogicalNode == null || destLogicalNode == null) {
            return;
        }
        DROPPED_LINKS.add(new LinkKey(clusterId, srcLogicalNode, destLogicalNode));
    }

    public static void healCluster(final String clusterId) {
        if (clusterId == null) {
            return;
        }
        DROPPED_LINKS.removeIf(link -> clusterId.equals(link.clusterId));
        LINK_BEHAVIORS.entrySet().removeIf(e -> clusterId.equals(e.getKey().clusterId));
    }

    public static void setLinkBehavior(final String clusterId,
                                       final String srcLogicalNode,
                                       final String destLogicalNode,
                                       final long delayMs,
                                       final long jitterMs,
                                       final double dropProbability) {
        if (clusterId == null || srcLogicalNode == null || destLogicalNode == null) {
            return;
        }
        final long normalizedDelayMs = Math.max(0L, delayMs);
        final long normalizedJitterMs = Math.max(0L, jitterMs);
        final double normalizedDropProbability =
            Math.max(0.0d, Math.min(1.0d, dropProbability));
        final LinkKey key = new LinkKey(clusterId, srcLogicalNode, destLogicalNode);
        if (normalizedDelayMs == 0L
            && normalizedJitterMs == 0L
            && normalizedDropProbability == 0.0d) {
            LINK_BEHAVIORS.remove(key);
        } else {
            LINK_BEHAVIORS.put(key,
                               new LinkBehavior(normalizedDelayMs,
                                                normalizedJitterMs,
                                                normalizedDropProbability));
        }
    }

    public static boolean shouldDropPeerMessage(final String srcPeerId,
                                                final String destPeerId) {
        if (srcPeerId == null || destPeerId == null) {
            return false;
        }
        final NodeRef src = PEER_NODES.get(srcPeerId);
        final NodeRef dest = PEER_NODES.get(destPeerId);
        if (src == null || dest == null) {
            return false;
        }
        if (!src.clusterId.equals(dest.clusterId)) {
            return false;
        }
        final LinkKey key = new LinkKey(src.clusterId, src.logicalNode, dest.logicalNode);
        if (DROPPED_LINKS.contains(key)) {
            return true;
        }
        final LinkBehavior behavior = LINK_BEHAVIORS.get(key);
        if (behavior == null || behavior.dropProbability <= 0.0d) {
            return false;
        }
        if (behavior.dropProbability >= 1.0d) {
            return true;
        }
        return ThreadLocalRandom.current().nextDouble() < behavior.dropProbability;
    }

    public static long peerMessageDelayMs(final String srcPeerId,
                                          final String destPeerId) {
        if (srcPeerId == null || destPeerId == null) {
            return 0L;
        }
        final NodeRef src = PEER_NODES.get(srcPeerId);
        final NodeRef dest = PEER_NODES.get(destPeerId);
        if (src == null || dest == null) {
            return 0L;
        }
        if (!src.clusterId.equals(dest.clusterId)) {
            return 0L;
        }
        final LinkBehavior behavior =
            LINK_BEHAVIORS.get(new LinkKey(src.clusterId,
                                           src.logicalNode,
                                           dest.logicalNode));
        if (behavior == null) {
            return 0L;
        }
        final long extraDelayMs =
            behavior.jitterMs <= 0L
            ? 0L
            : ThreadLocalRandom.current().nextLong(behavior.jitterMs + 1L);
        return behavior.delayMs + extraDelayMs;
    }

    private static final class NodeRef {
        private final String clusterId;
        private final String logicalNode;

        private NodeRef(final String clusterId, final String logicalNode) {
            this.clusterId = clusterId;
            this.logicalNode = logicalNode;
        }
    }

    private static final class LinkKey {
        private final String clusterId;
        private final String srcLogicalNode;
        private final String destLogicalNode;

        private LinkKey(final String clusterId,
                        final String srcLogicalNode,
                        final String destLogicalNode) {
            this.clusterId = clusterId;
            this.srcLogicalNode = srcLogicalNode;
            this.destLogicalNode = destLogicalNode;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof LinkKey that)) {
                return false;
            }
            return Objects.equals(this.clusterId, that.clusterId)
                && Objects.equals(this.srcLogicalNode, that.srcLogicalNode)
                && Objects.equals(this.destLogicalNode, that.destLogicalNode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.clusterId, this.srcLogicalNode, this.destLogicalNode);
        }
    }

    private static final class LinkBehavior {
        private final long delayMs;
        private final long jitterMs;
        private final double dropProbability;

        private LinkBehavior(final long delayMs,
                             final long jitterMs,
                             final double dropProbability) {
            this.delayMs = delayMs;
            this.jitterMs = jitterMs;
            this.dropProbability = dropProbability;
        }
    }
}
