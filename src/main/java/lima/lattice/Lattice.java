package lima.lattice;

import java.util.HashMap;
import java.util.Map;

/**
 * A lattice whose nodes are subsets of active predicates (at most one per
 * predicate group). Each node is a conjunction of predicates; edges
 * add/remove one predicate from one group. The root (empty node) represents
 * no active predicates (selectivity = 1.0).
 */
public final class Lattice {
    private final int[] pgs;       // pgs[i] = number of predicates in group i
    private final int npgs;
    private final int[] pgscores;  // mixed-radix positional weight per group, used for Node/Edge hashing
    private final int maxLatticeSize;

    public Lattice(int[] pgs) {
        this.pgs = pgs;
        this.npgs = pgs.length;

        // pgscores[i] = product of (pgs[j]+1) for j<i, so each group occupies
        // pgs[i]+1 "digits". Plain int multiplication wraps at 32 bits, which
        // is exactly what the Python original's explicit "& 0xffffffff" mask
        // achieves - only the bit pattern matters here, never the numeric
        // magnitude (these scores are only ever summed into a hash, never
        // compared or divided).
        this.pgscores = new int[npgs + 1];
        pgscores[0] = 1;
        for (int i = 0; i < npgs; i++) {
            pgscores[i + 1] = pgscores[i] * (pgs[i] + 1);
        }
        this.maxLatticeSize = pgscores[npgs];
    }

    public int npgs() {
        return npgs;
    }

    public int pgs(int pg) {
        return pgs[pg];
    }

    public int maxLatticeSize() {
        return maxLatticeSize;
    }

    public Node getRoot() {
        return new Node(this);
    }

    /** A node encodes a partial predicate assignment: preds = {pg -> predicate_index}. */
    public static final class Node {
        private final Lattice lattice;
        private final Map<Integer, Integer> preds;
        private long hash; // sum over active (pg,p) of pgscores[pg]*(p+1); group absent means digit 0

        Node(Lattice lattice) {
            this.lattice = lattice;
            this.preds = new HashMap<>();
            this.hash = 0;
        }

        /** The edge going up: this -> this + (pg,p). */
        public Edge to(int pg, int p) {
            Node t = copy();
            t.add(pg, p);
            return new Edge(this, t, pg, p);
        }

        /** The edge going down: this - pg -> this. */
        public Edge fr(int pg) {
            Node n = copy();
            int p = n.remove(pg);
            return new Edge(n, this, pg, p);
        }

        private void add(int pg, int p) {
            if (preds.containsKey(pg)) {
                throw new IllegalStateException("predicate group " + pg + " is already active in this node");
            }
            preds.put(pg, p);
            hash += (long) lattice.pgscores[pg] * (p + 1);
        }

        private int remove(int pg) {
            Integer p = preds.remove(pg);
            if (p == null) {
                throw new IllegalStateException("predicate group " + pg + " is not active in this node");
            }
            hash -= (long) lattice.pgscores[pg] * (p + 1);
            return p;
        }

        public int get(int pg) {
            return preds.get(pg);
        }

        public boolean contains(int pg) {
            return preds.containsKey(pg);
        }

        /** The active (pg -> predicate index) assignment. Returned live, not a copy. */
        public Map<Integer, Integer> preds() {
            return preds;
        }

        public int size() {
            return preds.size();
        }

        public Node copy() {
            Node res = new Node(lattice);
            res.hash = this.hash;
            res.preds.putAll(this.preds);
            return res;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(hash);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Node other)) {
                return false;
            }
            if (hash != other.hash) {
                return false;
            }
            return preds.equals(other.preds);
        }

        @Override
        public String toString() {
            return preds.toString();
        }
    }

    public static final class Edge {
        private final Node f;
        private final Node t;
        public final int pg;
        public final int p;
        private final long hash;

        Edge(Node f, Node t, int pg, int p) {
            this.f = f;
            this.t = t;
            this.pg = pg;
            this.p = p;
            this.hash = t.hash * t.size() + pg;
        }

        public Node to() {
            return t;
        }

        public Node fr() {
            return f;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(hash);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Edge other)) {
                return false;
            }
            if (hash != other.hash) {
                return false;
            }
            // Matches the Python original exactly: only f and p are compared,
            // not pg or t. Ported as-is, not fixed.
            return f.equals(other.f) && p == other.p;
        }

        @Override
        public String toString() {
            return f.toString() + " + (" + pg + ": " + p + ")";
        }
    }
}
