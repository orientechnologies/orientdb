package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.common.types.OModifiableInteger;

import java.util.*;

public class OWALChangesTree {
  private static final boolean BLACK          = false;
  private static final boolean RED            = true;

  private Node                 root           = null;
  private int                  version        = 0;

  private boolean              debug;

  private int                  serializedSize = OIntegerSerializer.INT_SIZE;

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public byte getByteValue(ODirectMemoryPointer pointer, int offset) {
    final int end = offset + OByteSerializer.BYTE_SIZE;
    final List<Node> result = new ArrayList<Node>();
    findIntervals(root, offset, end, result);

    if (pointer != null && result.isEmpty())
      return pointer.getByte(offset);

    byte[] value = new byte[] { 0 };
    applyChanges(value, offset, end, result);

    return value[0];
  }

  public byte[] getBinaryValue(ODirectMemoryPointer pointer, int offset, int len) {
    final int end = offset + len;

    final List<Node> result = new ArrayList<Node>();
    findIntervals(root, offset, end, result);

    if (result.isEmpty() && pointer != null)
      return pointer.get(offset, len);

    byte[] value;

    if (pointer != null)
      value = pointer.get(offset, len);
    else
      value = new byte[len];

    applyChanges(value, offset, end, result);

    return value;
  }

  public short getShortValue(ODirectMemoryPointer pointer, int offset) {
    int end = offset + OShortSerializer.SHORT_SIZE;

    final List<Node> result = new ArrayList<Node>();
    findIntervals(root, offset, end, result);

    if (result.isEmpty() && pointer != null)
      return pointer.getShort(offset);

    byte[] value;
    if (pointer != null)
      value = pointer.get(offset, OShortSerializer.SHORT_SIZE);
    else
      value = new byte[OShortSerializer.SHORT_SIZE];

    applyChanges(value, offset, end, result);

    return OShortSerializer.INSTANCE.deserializeNative(value, 0);
  }

  public int getIntValue(ODirectMemoryPointer pointer, int offset) {
    int end = offset + OIntegerSerializer.INT_SIZE;

    final List<Node> result = new ArrayList<Node>();
    findIntervals(root, offset, end, result);

    if (result.isEmpty() && pointer != null)
      return pointer.getInt(offset);

    byte[] value;
    if (pointer != null)
      value = pointer.get(offset, OIntegerSerializer.INT_SIZE);
    else
      value = new byte[OIntegerSerializer.INT_SIZE];

    applyChanges(value, offset, end, result);

    return OIntegerSerializer.INSTANCE.deserializeNative(value, 0);
  }

  public long getLongValue(ODirectMemoryPointer pointer, int offset) {
    int end = offset + OLongSerializer.LONG_SIZE;

    final List<Node> result = new ArrayList<Node>();
    findIntervals(root, offset, end, result);

    if (result.isEmpty() && pointer != null)
      return pointer.getLong(offset);

    byte[] value;
    if (pointer != null)
      value = pointer.get(offset, OLongSerializer.LONG_SIZE);
    else
      value = new byte[OLongSerializer.LONG_SIZE];

    applyChanges(value, offset, end, result);

    return OLongSerializer.INSTANCE.deserializeNative(value, 0);
  }

  public void add(byte[] value, int start) {
    version++;

    add(value, start, version, true);
  }

  private void add(byte[] value, int start, int version, boolean updateSerializedSize) {
    if (value == null || value.length == 0)
      return;

    final Node fnode = bsearch(start);

    final Node node = new Node(value, start, RED, version);

    if (fnode == null) {
      root = node;
      root.color = BLACK;

      if (debug)
        assertInvariants();

      if (updateSerializedSize)
        serializedSize += serializedSize(value.length);

      return;
    }

    if (start < fnode.start) {
      fnode.left = node;
      node.parent = fnode;

      if (updateSerializedSize)
        serializedSize += serializedSize(value.length);

      updateMaxEndAfterAppend(node);
      insertCaseOne(node);
    } else if (start > fnode.start) {
      fnode.right = node;
      node.parent = fnode;

      if (updateSerializedSize)
        serializedSize += serializedSize(value.length);

      updateMaxEndAfterAppend(node);
      insertCaseOne(node);
    } else {
      final int end = start + value.length;
      if (end == fnode.end) {
        if (fnode.version < version) {
          if (updateSerializedSize)
            serializedSize -= fnode.value.length;

          fnode.value = value;
          fnode.version = version;

          if (updateSerializedSize)
            serializedSize += fnode.value.length;
        }
      } else if (end < fnode.end) {
        if (fnode.version < version) {
          final byte[] cvalue = Arrays.copyOfRange(fnode.value, end - start, fnode.end - start);
          final int cversion = fnode.version;
          final int cstart = end;

          if (updateSerializedSize)
            serializedSize -= fnode.value.length;

          fnode.end = end;
          fnode.value = value;
          fnode.version = version;

          if (updateSerializedSize)
            serializedSize += fnode.value.length;

          updateMaxEndAccordingToChildren(fnode);

          add(cvalue, cstart, cversion, updateSerializedSize);
        }
      } else {
        if (fnode.version > version) {
          final byte[] cvalue = Arrays.copyOfRange(value, fnode.end - start, end - start);
          final int cversion = version;
          final int cstart = fnode.end;

          add(cvalue, cstart, cversion, updateSerializedSize);
        } else {
          if (updateSerializedSize)
            serializedSize -= fnode.value.length;

          fnode.end = end;
          fnode.value = value;
          fnode.version = version;

          if (updateSerializedSize)
            serializedSize += fnode.value.length;

          updateMaxEndAccordingToChildren(fnode);
        }
      }
    }

    if (debug)
      assertInvariants();
  }

  public void applyChanges(byte[] values, int start) {
    final int end = start + values.length;
    final List<Node> result = new ArrayList<Node>();
    findIntervals(root, start, end, result);

    applyChanges(values, start, end, result);
  }

  public int serializedSize() {
    return serializedSize;
  }

  public int serializedSize(int content) {
    return content + 3 * OIntegerSerializer.INT_SIZE; // start, version, content size + content
  }

  private void applyChanges(byte[] values, int start, int end, List<Node> result) {
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

      final int vStart;
      if (deltaStart > 0)
        vStart = start + deltaStart;
      else
        vStart = start;

      int vLength;
      if (deltaStart > 0)
        vLength = (activeNode.end - activeStart);
      else
        vLength = (activeNode.end - activeStart) + deltaStart;

      final int vEnd = vLength + vStart;
      if (vEnd > end)
        vLength = vLength - (vEnd - end);

      if (vLength <= 0)
        continue;

      System.arraycopy(activeNode.value, deltaStart >= 0 ? activeStart - activeNode.start : activeStart - activeNode.start
          - deltaStart, values, deltaStart >= 0 ? deltaStart : 0, vLength);
    }
  }

  public void applyChanges(ODirectMemoryPointer pointer) {
    if (root == null)
      return;

    final Queue<Node> processedNodes = new ArrayDeque<Node>();
    applyChanges(pointer, root, processedNodes);
  }

  public PointerWrapper wrap(final ODirectMemoryPointer pointer) {
    return new PointerWrapper(pointer);
  }

  public int getSerializedSize() {
    return serializedSize;
  }

  public int fromStream(int offset, byte[] stream) {
    serializedSize = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
    int readBytes = OIntegerSerializer.INT_SIZE;

    while (readBytes < serializedSize) {
      int start = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset + readBytes);
      readBytes += OIntegerSerializer.INT_SIZE;

      int version = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset + readBytes);
      readBytes += OIntegerSerializer.INT_SIZE;

      int length = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset + readBytes);
      readBytes += OIntegerSerializer.INT_SIZE;

      byte[] data = new byte[length];
      System.arraycopy(stream, offset + readBytes, data, 0, length);

      readBytes += length;

      add(data, start, version, false);
    }

    return offset + readBytes;
  }

  public int toStream(int offset, byte[] stream) {
    OIntegerSerializer.INSTANCE.serializeNative(serializedSize, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;
    if (root == null)
      return offset;

    offset = toStream(root, offset, stream);

    return offset;
  }

  private int toStream(Node node, int offset, byte[] stream) {
    if (node.left != null)
      offset = toStream(node.left, offset, stream);

    OIntegerSerializer.INSTANCE.serializeNative(node.start, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(node.version, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(node.value.length, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(node.value, 0, stream, offset, node.value.length);
    offset += node.value.length;

    if (node.right != null)
      offset = toStream(node.right, offset, stream);

    return offset;
  }

  private void applyChanges(ODirectMemoryPointer pointer, Node node, Queue<Node> processedNodes) {
    if (node.left != null)
      applyChanges(pointer, node.left, processedNodes);

    int activeStart = node.start;

    final Iterator<Node> pNodesIterator = processedNodes.iterator();
    while (pNodesIterator.hasNext()) {
      final Node pNode = pNodesIterator.next();
      if (pNode.end > activeStart && pNode.version > node.version)
        activeStart = pNode.end;

      if (pNode.end <= node.start)
        pNodesIterator.remove();
    }

    processedNodes.add(node);

    if (activeStart < node.end) {
      final int vLength = node.end - activeStart;
      pointer.set(activeStart, node.value, activeStart - node.start, vLength);
    }

    if (node.right != null)
      applyChanges(pointer, node.right, processedNodes);
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

    if (node.right != null) {
      OModifiableInteger newPath = new OModifiableInteger(currentPath.getValue());
      paths.add(newPath);

      calculateBlackPathsFromNode(node.right, paths, newPath);
    }

    if (node.left != null) {
      calculateBlackPathsFromNode(node.left, paths, currentPath);
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

    if (node.right != null && !redHaveBlackChildNodes(node.right))
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

    if (node == root)
      root = p;

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

    if (node == root)
      root = q;

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

  public final class PointerWrapper {
    private final ODirectMemoryPointer pointer;

    private PointerWrapper(ODirectMemoryPointer pointer) {
      this.pointer = pointer;
    }

    public byte getByte(long offset) {
      return OWALChangesTree.this.getByteValue(pointer, (int) offset);
    }

    public short getShort(long offset) {
      return OWALChangesTree.this.getShortValue(pointer, (int) offset);
    }

    public int getInt(long offset) {
      return OWALChangesTree.this.getIntValue(pointer, (int) offset);
    }

    public long getLong(long offset) {
      return OWALChangesTree.this.getLongValue(pointer, (int) offset);
    }

    public byte[] get(long offset, int len) {
      return OWALChangesTree.this.getBinaryValue(pointer, (int) offset, len);
    }

    public char getChar(long offset) {
      return (char) OWALChangesTree.this.getShortValue(pointer, (int) offset);
    }

  }
}
