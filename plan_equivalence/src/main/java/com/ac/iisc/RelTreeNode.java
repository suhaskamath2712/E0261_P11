package com.ac.iisc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Simple tree node to represent a RelNode tree in a language-agnostic way.
 * Each node has a textual label and a list of children in input order.
 */
/**
 * Tree wrapper used for order-sensitive and order-insensitive comparisons of Calcite RelNodes.
 * Labels are simple strings; children preserve input order unless comparisons ignore order.
 */
public class RelTreeNode {
    private String label;
    private final List<RelTreeNode> children = new ArrayList<>();

    public RelTreeNode() {}

    public RelTreeNode(String label) {
        this.label = label;
    }

    // --- Children management ---
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
     * Build a canonical digest of this tree where the order of children does not matter.
     * The digest is constructed as: label[ sorted(childDigest1, childDigest2, ...) ].
     * Two trees are structurally equivalent ignoring child order if and only if their
     * canonical digests are equal.
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

    /**
     * Compare this tree to another, treating children as an unordered multiset.
     * Returns true if both trees are equivalent up to permutation of children at
     * every node.
     */
    public boolean equalsIgnoreChildOrder(RelTreeNode other) {
        if (this == other) return true;
        if (other == null) return false;
        return this.canonicalDigest().equals(other.canonicalDigest());
    }

    /**
     * Static convenience to compare two trees for order-insensitive equality.
     */
    public static boolean equalsIgnoreChildOrder(RelTreeNode a, RelTreeNode b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.canonicalDigest().equals(b.canonicalDigest());
    }
}
