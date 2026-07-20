package datalevin.utl;

/** Compact key for a dynamic-programming join-order state. */
public final class DPKey {
    private final long members;
    private final DPKey previous;
    private final int node;
    private final int length;
    private final int hash;
    private final boolean ordered;

    private DPKey(long members, DPKey previous, int node, int length,
                  int hash, boolean ordered) {
        this.members = members;
        this.previous = previous;
        this.node = node;
        this.length = length;
        this.hash = hash;
        this.ordered = ordered;
    }

    public static DPKey ordered(int node) {
        checkNode(node);
        return new DPKey(1L << node, null, node, 1, 31 + node, true);
    }

    public static DPKey canonical(long members) {
        return new DPKey(members, null, -1, Long.bitCount(members),
                         Long.hashCode(members), false);
    }

    public DPKey append(int node) {
        checkNode(node);
        long nextMembers = members | (1L << node);
        if (ordered) {
            return new DPKey(nextMembers, this, node, length + 1,
                             31 * hash + node, true);
        }
        return canonical(nextMembers);
    }

    public boolean contains(int node) {
        checkNode(node);
        return (members & (1L << node)) != 0;
    }

    public long members() {
        return members;
    }

    public boolean isOrdered() {
        return ordered;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof DPKey that)) return false;
        if (ordered != that.ordered || members != that.members ||
            length != that.length || hash != that.hash) {
            return false;
        }
        if (!ordered) return true;

        DPKey left = this;
        DPKey right = that;
        while (left != null && right != null) {
            if (left.node != right.node) return false;
            left = left.previous;
            right = right.previous;
        }
        return left == right;
    }

    private static void checkNode(int node) {
        if (node < 0 || node >= Long.SIZE) {
            throw new IllegalArgumentException("DP node ID out of range: " + node);
        }
    }
}
