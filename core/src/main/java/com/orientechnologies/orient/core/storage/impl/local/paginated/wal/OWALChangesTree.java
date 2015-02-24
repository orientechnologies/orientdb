package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.types.OModifiableInteger;

import java.util.*;

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

    assertInvariants();
  }

  public void applyChanges(byte[] values, int start, int end) {
    final List<Node> result = new ArrayList<Node>();
    findIntervals(root, start, end, result);

    if (result.isEmpty())
      return;

    final Queue<Node> processedNodes = new ArrayDeque<Node>();

    for (Node activeNode : result) {
      int activeStart = activeNode.start;

      final Iterator<Node> pNodesIterator = processedNodes.iterator();
      while (pNodesIterator.hasNext()) {
        final Node pNode = pNodesIterator.next();
        if (pNode.end > activeStart && pNode.version > activeNode.version)
          activeStart = pNode.end;

        if (pNode.end <= activeNode.start)
          pNodesIterator.remove();
      }

      processedNodes.add(activeNode);

      if (activeStart >= activeNode.end)
        continue;

      final int deltaStart = activeStart - start;

      System.arraycopy(activeNode.value, deltaStart >= 0 ? 0 : -deltaStart, values, deltaStart >= 0 ? deltaStart : 0,
          deltaStart < 0 ? activeNode.value.length + deltaStart : activeNode.value.length);
    }
  }

  private void assertInvariants() {
    assert rootIsBlack();
    assert redHaveBlackChildNodes();
    assert allBlackPathsAreEqual();
    assert maxEndIsPresent();
    assert maxEndValueIsCorrect();
  }

  private boolean allBlackPathsAreEqual() {
    List<OModifiableInteger> paths = new ArrayList<OModifiableInteger>();
    OModifiableInteger currentPath = new OModifiableInteger();
    paths.add(currentPath);

    calculateBlackPathsFromNode(root, paths, currentPath);

    final int basePath = paths.get(0).getValue();
    for (OModifiableInteger path : paths) {
      if (path.getValue() != basePath)
        return false;
    }

    return true;
  }

  private void calculateBlackPathsFromNode(Node node, List<OModifiableInteger> paths, OModifiableInteger currentPath) {
    if (node.color == BLACK) {
      currentPath.increment();
    }

    if (node.left != null) {
      calculateBlackPathsFromNode(node.left, paths, currentPath);
    }

    if (node.right != null) {
      OModifiableInteger newPath = new OModifiableInteger(currentPath.getValue());
      paths.add(newPath);

      calculateBlackPathsFromNode(node.right, paths, newPath);
    }
  }

  private boolean redHaveBlackChildNodes() {
    return redHaveBlackChildNodes(root);
  }

  private boolean redHaveBlackChildNodes(Node node) {
    if (node.color == RED) {
      if (node.left != null && node.left.color == RED)
        return false;

      if (node.right != null && node.right.color == RED)
        return false;
    }

    if (node.left != null && !redHaveBlackChildNodes(node.left))
      return false;

    if (node.right != null && redHaveBlackChildNodes(node.right))
      return false;

    return true;
  }

  private boolean rootIsBlack() {
    if (root == null)
      return true;

    return root.color == BLACK;
  }

  private boolean maxEndIsPresent() {
    if (root == null)
      return true;

    return maxEndIsPresent(root);
  }

  private boolean maxEndIsPresent(Node node) {
    boolean result = endValueIsPresentAmongChildren(node.maxEnd, node);
    if (!result)
      return false;

    if (node.left != null)
      result = maxEndIsPresent(node.left);

    if (!result)
      return false;

    if (node.right != null)
      result = maxEndIsPresent(node.right);

    return result;
  }

  private boolean endValueIsPresentAmongChildren(int value, Node node) {
    if (node.end == value)
      return true;

    if (node.left != null && endValueIsPresentAmongChildren(value, node.left))
      return true;

    if (node.right != null && endValueIsPresentAmongChildren(value, node.right))
      return true;

    return false;
  }

  private boolean maxEndValueIsCorrect() {
    if (root == null)
      return true;

    return maxEndValueIsCorrect(root);
  }

  private boolean maxEndValueIsCorrect(Node node) {
    if (!valueIsMaxEndValueAmongChildren(node, node.maxEnd))
      return false;

    if (node.left != null && !maxEndIsPresent(node.left))
      return false;

    if (node.right != null && !maxEndIsPresent(node.right))
      return false;

    return true;
  }

  private boolean valueIsMaxEndValueAmongChildren(Node node, int value) {
    if (node.end > value)
      return false;

    if (node.left != null && !valueIsMaxEndValueAmongChildren(node.left, value))
      return false;

    if (node.right != null && !valueIsMaxEndValueAmongChildren(node.right, value))
      return false;

    return true;
  }

  private void findIntervals(Node node, int start, int end, List<Node> result) {
    if (node == null)
      return;

    if (start >= node.maxEnd)
      return;

    if (node.left != null)
      findIntervals(node.left, start, end, result);

    if (node.overlapsWith(start, end))
      result.add(node);

    if (end <= node.start)
      return;

    if (node.right != null)
      findIntervals(node.right, start, end, result);
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

    private boolean overlapsWith(Node other) {
      return start < other.end && end > other.start;
    }

    private boolean overlapsWith(int start, int end) {
      return this.start < end && this.end > start;
    }
  }
}
