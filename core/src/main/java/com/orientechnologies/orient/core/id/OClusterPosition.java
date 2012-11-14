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
 * @author Andrey Lomakin
 * @since 12.11.12
 */
public abstract class OClusterPosition extends Number implements Comparable<OClusterPosition> {
  public final static OClusterPosition INVALID_POSITION = OClusterPositionFactory.INSTANCE.valueOf(-1);

  public abstract OClusterPosition inc();

  public abstract OClusterPosition dec();

  public abstract boolean isValid();

  public abstract boolean isPersistent();

  public abstract boolean isNew();

  public abstract boolean isTemporary();

  public abstract byte[] toStream();
}
