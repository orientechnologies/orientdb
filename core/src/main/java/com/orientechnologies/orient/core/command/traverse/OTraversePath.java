package com.orientechnologies.orient.core.command.traverse;

import java.util.ArrayDeque;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OTraversePath {
  private static final OTraversePath EMPTY_PATH = new OTraversePath(new FirstPathItem());

  private final PathItem             lastPathItem;

  private OTraversePath(PathItem lastPathItem) {
    this.lastPathItem = lastPathItem;
  }

  @Override
  public String toString() {
    final ArrayDeque<PathItem> stack = new ArrayDeque<PathItem>();
    PathItem currentItem = lastPathItem;
    while (currentItem != null) {
      stack.push(currentItem);
      currentItem = currentItem.parentItem;
    }

    final StringBuilder buf = new StringBuilder();
    for (PathItem pathItem : stack) {
      buf.append(pathItem.toString());
    }

    return buf.toString();
  }

  public OTraversePath append(OIdentifiable record) {
    return new OTraversePath(new RecordPathItem(record, lastPathItem));
  }

  public OTraversePath appendField(String fieldName) {
    return new OTraversePath(new FieldPathItem(fieldName, lastPathItem));
  }

  public OTraversePath appendIndex(int index) {
    return new OTraversePath(new CollectionPathItem(index, lastPathItem));
  }

  public OTraversePath appendRecordSet() {
    return this;
  }

  public int getDepth() {
    return lastPathItem.depth;
  }

  public static OTraversePath empty() {
    return EMPTY_PATH;
  }

  private static abstract class PathItem {
    protected final PathItem parentItem;
    protected final int      depth;

    private PathItem(PathItem parentItem, int depth) {
      this.parentItem = parentItem;
      this.depth = depth;
    }
  }

  private static class RecordPathItem extends PathItem {
    private final OIdentifiable record;

    private RecordPathItem(OIdentifiable record, PathItem parentItem) {
      super(parentItem, parentItem.depth + 1);
      this.record = record;
    }

    @Override
    public String toString() {
      return "(" + record.getIdentity().toString() + ")";
    }
  }

  private static class FieldPathItem extends PathItem {
    private final String name;

    private FieldPathItem(String name, PathItem parentItem) {
      super(parentItem, parentItem.depth);
      this.name = name;
    }

    @Override
    public String toString() {
      return "." + name;
    }
  }

  private static class CollectionPathItem extends PathItem {
    private final int index;

    private CollectionPathItem(int index, PathItem parentItem) {
      super(parentItem, parentItem.depth);
      this.index = index;
    }

    @Override
    public String toString() {
      return "[" + index + "]";
    }
  }

  private static class FirstPathItem extends PathItem {
    private FirstPathItem() {
      super(null, -1);
    }

    @Override
    public String toString() {
      return "";
    }
  }
}
