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
  private static final ONodeId INVALID_NODE_ID = ONodeId.valueOf(-1);
  private final ONodeId        nodeId;

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
    return !nodeId.equals(INVALID_NODE_ID);
  }

  @Override
  public boolean isPersistent() {
    return nodeId.compareTo(INVALID_NODE_ID) > 0;
  }

  @Override
  public boolean isNew() {
    return nodeId.compareTo(ONodeId.ZERO) < 0;
  }

  @Override
  public boolean isTemporary() {
    return nodeId.compareTo(INVALID_NODE_ID) < 0;
  }

  @Override
  public byte[] toStream() {
    return nodeId.toStream();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OClusterPositionNodeId that = (OClusterPositionNodeId) o;

    return nodeId.equals(that.nodeId);
  }

  @Override
  public int hashCode() {
    return nodeId.hashCode();
  }

  @Override
  public String toString() {
    return nodeId.toString();
  }

  @Override
  public int compareTo(OClusterPosition o) {
    final OClusterPositionNodeId clusterPositionNodeId = (OClusterPositionNodeId) o;
    return nodeId.compareTo(clusterPositionNodeId.getNodeId());
  }

  @Override
  public int intValue() {
    return nodeId.intValue();
  }

  @Override
  public long longValue() {
    return nodeId.longValue();
  }

  @Override
  public float floatValue() {
    return nodeId.floatValue();
  }

  @Override
  public double doubleValue() {
    return nodeId.doubleValue();
  }
}
