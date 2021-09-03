/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.command.traverse;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.util.ArrayDeque;

/** @author Artem Orobets (enisher-at-gmail.com) */
public class OTraversePath {
  private static final OTraversePath EMPTY_PATH = new OTraversePath(new FirstPathItem());

  private final PathItem lastPathItem;

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

    final StringBuilder buf = new StringBuilder(1024);
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

  private abstract static class PathItem {
    protected final PathItem parentItem;
    protected final int depth;

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
