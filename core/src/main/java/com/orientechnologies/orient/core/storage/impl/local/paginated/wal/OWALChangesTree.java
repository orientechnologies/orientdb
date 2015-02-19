package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.sun.xml.internal.ws.util.NoCloseOutputStream;

import java.util.Arrays;

public class OWALChangesTree {
  private static final boolean BLACK   = false;
  private static final boolean RED     = true;

  private Node                 root    = null;
  private int                  version = 0;

  public void add(byte[] value, int start) {
    version++;

    add(value, start, version, true);
  }

  private void add(byte[] value, int start, int version, boolean updateSerializedSize) {
    final Node fnode = bsearch(start);

    final Node node = new Node(value, start, RED, version);

    if (start < fnode.start) {
      fnode.left = node;
      node.parent = fnode;

      updateMaxEndAfterAppend(node);
      insertCaseOne(node);
    } else if (start > fnode.start) {
      fnode.right = node;
      node.parent = fnode;

      updateMaxEndAfterAppend(node);
      insertCaseOne(node);
    } else {
      final int end = start + value.length;
      if (end == fnode.end) {
        if (fnode.version < version)
          fnode.value = value;
      } else if (end < fnode.end) {
        if (fnode.version < version) {
          final byte[] cvalue = Arrays.copyOfRange(fnode.value, end, fnode.end);
          final int cversion = fnode.version;
          final int cstart = end;

          fnode.end = end;
          fnode.value = value;

          updateMaxEndAccordingToChildren(fnode);

          add(cvalue, cstart, cversion, updateSerializedSize);
        }
      } else {
        if (fnode.version > version) {
          final byte[] cvalue = Arrays.copyOfRange(value, fnode.end, end);
          final int cversion = version;
          final int cstart = fnode.end;

          add(cvalue, cstart, cversion, updateSerializedSize);
        } else {
          fnode.end = end;
          fnode.value = value;

          updateMaxEndAccordingToChildren(fnode);
        }
      }
    }
  }

  public void applyChanges(byte[] values, int start, int end) {

  }

  private Node bsearch(int start) {
    if (root == null)
      return null;

    Node current = root;

    while (true) {
      if (start < current.start) {
        if (current.left != null)
          current = current.left;
        else
          return current;
      } else if (start > current.start) {
        if (current.right != null)
          current = current.right;
        else
          return current;
      } else
        return current;
    }
  }

  private void insertCaseOne(Node node) {
    if (node.parent == null) {
      node.color = BLACK;
    } else {
      insertCaseTwo(node);
    }
  }

  private void insertCaseTwo(Node node) {
    if (node.parent.color == BLACK) {
      return;
    }

    insertCaseThree(node);
  }

  private void insertCaseThree(Node node) {
    Node u = uncle(node);
    if (u != null && u.color == RED) {
      node.parent.color = BLACK;
      u.color = BLACK;

      Node g = grandparent(node);
      g.color = RED;

      insertCaseOne(g);
    } else {
      insertCaseFour(node);
    }
  }

  private void insertCaseFour(Node node) {
    Node g = grandparent(node);
    if (node == node.parent.right && node.parent == g.left) {
      rotateLeft(node.parent);

      node = node.left;
    } else if (node == node.parent.left && node.parent == g.right) {
      rotateRight(node.parent);

      node = node.right;
    }

    insertCaseFive(node);
  }

  private void rotateRight(Node node) {
    Node q = node;
    Node p = q.left;
    Node b = p.right;

    Node parent = node.parent;

    p.right = q;
    q.parent = p;

    p.parent = parent;

    if (parent != null) {
      if (parent.left == node)
        parent.left = p;
      else
        parent.right = p;
    }

    q.left = b;
    if (b != null)
      b.parent = q;

    updateMaxEndAccordingToChildren(q);
  }

  private void rotateLeft(Node node) {
    Node p = node;
    Node q = p.right;
    Node b = q.left;

    Node parent = node.parent;

    q.left = p;
    p.parent = q;

    q.parent = parent;
    if (parent != null) {
      if (parent.left == node)
        parent.left = q;
      else
        parent.right = q;
    }

    p.right = b;

    if (b != null)
      b.parent = p;

    updateMaxEndAccordingToChildren(p);
  }

  private void updateMaxEndAccordingToChildren(Node node) {
    if (node.left != null && node.right != null) {
      node.maxEnd = Math.max(node.end, Math.max(node.left.maxEnd, node.right.maxEnd));
    } else if (node.left != null) {
      node.maxEnd = Math.max(node.end, node.left.maxEnd);
    } else if (node.right != null) {
      node.maxEnd = Math.max(node.end, node.right.maxEnd);
    } else {
      node.maxEnd = node.end;
    }

    if (node.parent != null)
      updateMaxEndAccordingToChildren(node.parent);
  }

  private void insertCaseFive(Node node) {
    Node g = grandparent(node);

    node.parent.color = BLACK;
    g.color = RED;

    if (node == node.parent.left)
      rotateRight(g);
    else
      rotateLeft(g);
  }

  private Node grandparent(Node node) {
    if (node != null && node.parent != null) {
      return node.parent.parent;
    }

    return null;
  }

  private Node uncle(Node node) {
    Node g = grandparent(node);

    if (g == null)
      return null;

    if (node.parent == g.left)
      return g.right;

    return g.left;
  }

  private void updateMaxEndAfterAppend(Node node) {
    final Node parent = node.parent;
    if (parent != null && parent.maxEnd < node.maxEnd) {
      parent.maxEnd = node.maxEnd;
      updateMaxEndAfterAppend(parent);
    }
  }

  private static final class Node {
    private boolean color;
    private byte[]  value;
    private int     version;

    private int     start;
    private int     end;
    private int     maxEnd;

    private Node    parent;
    private Node    left;
    private Node    right;

    public Node(byte[] value, int start, boolean color, int version) {
      this.color = color;
      this.version = version;

      this.value = value;
      this.start = start;
      this.end = start + value.length;
      this.maxEnd = end;
    }
  }
}
