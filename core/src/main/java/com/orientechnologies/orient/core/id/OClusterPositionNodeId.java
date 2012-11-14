/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.id;

/**
 * 
 * 192 bit signed presentation of {@link OClusterPosition} instance. With values from -2<sup>192</sup>+1 till 2<sup>192</sup>-1. It
 * is based on {@link ONodeId} class so conversion from nodeid to cluster position for autosharded storage is pretty simple task.
 * 
 * @author Andrey Lomakin
 * @since 12.11.12
 */
public final class OClusterPositionNodeId extends OClusterPosition {
  private final ONodeId nodeId;

  public ONodeId getNodeId() {
    return nodeId;
  }

  public OClusterPositionNodeId(ONodeId nodeId) {
    this.nodeId = nodeId;
  }

  @Override
  public OClusterPosition inc() {
    return new OClusterPositionNodeId(nodeId.add(ONodeId.ONE));
  }

  @Override
  public OClusterPosition dec() {
    return new OClusterPositionNodeId(nodeId.subtract(ONodeId.ONE));
  }

  @Override
  public boolean isValid() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPersistent() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNew() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isTemporary() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] toStream() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compareTo(OClusterPosition o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int intValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long longValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float floatValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double doubleValue() {
    throw new UnsupportedOperationException();
  }
}
