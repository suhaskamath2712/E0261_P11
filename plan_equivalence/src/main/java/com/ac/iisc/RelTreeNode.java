package com.ac.iisc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Lightweight tree wrapper used to represent Calcite {@code RelNode} plans in a
 * language‑agnostic way for debugging and comparisons.
 *
 * Characteristics:
 * - Each node stores a concise textual {@code label} plus a list of children
 *   in input order.
 * - Provides standard equality (order‑sensitive) via {@link #equals(Object)}.
 * - Provides order‑insensitive structural comparison via
 *   {@link #equalsIgnoreChildOrder(RelTreeNode)} using a canonical digest.
 * - {@link #toString()} renders a readable indented tree for logs.
 */
public class RelTreeNode {
    private String label;
    private final List<RelTreeNode> children = new ArrayList<>();

    public RelTreeNode() {}

    public RelTreeNode(String label) {
        this.label = label;
    }

    // --- Children management ---
    /** Append a child to this node (no effect for null). */
    public void addChild(RelTreeNode child) {
        if (child != null) this.children.add(child);
    }

    // --- Getters/Setters ---
    public String getLabel() { return label; }
    public void setLabel(String label) { this.label = label; }
    public List<RelTreeNode> getChildren() { return children; }

    // --- Equality/Hash ---
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RelTreeNode other)) return false;
        return Objects.equals(label, other.label) && Objects.equals(children, other.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, children);
    }

    // --- String representation ---
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb, 0);
        return sb.toString();
    }

    private void toString(StringBuilder sb, int depth) {
        sb.append(" ".repeat(Math.max(0, depth)));
        sb.append("- ").append(label == null ? "(null)" : label).append('\n');
        for (RelTreeNode child : children) {
            child.toString(sb, depth + 2);
        }
    }

    /**
     * Build a canonical digest where child order is ignored by sorting child digests.
     * Form: {@code label[childDigest1|childDigest2|...]}
     * Two trees are order‑insensitively equivalent iff their canonical digests match.
     */
    public String canonicalDigest() {
        final String nodeLabel = label == null ? "" : label;
        if (children.isEmpty()) {
            return nodeLabel + "[]";
        }
        List<String> childDigests = new ArrayList<>(children.size());
        for (RelTreeNode c : children) {
            childDigests.add(c == null ? "(null)[]" : c.canonicalDigest());
        }
        Collections.sort(childDigests);
        return nodeLabel + "[" + String.join("|", childDigests) + "]";
    }

    /** Order‑insensitive structural equality (children treated as an unordered multiset). */
    public boolean equalsIgnoreChildOrder(RelTreeNode other) {
        if (this == other) return true;
        if (other == null) return false;
        return this.canonicalDigest().equals(other.canonicalDigest());
    }

    /** Static convenience to compare two trees for order‑insensitive equality. */
    public static boolean equalsIgnoreChildOrder(RelTreeNode a, RelTreeNode b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.canonicalDigest().equals(b.canonicalDigest());
    }
}
