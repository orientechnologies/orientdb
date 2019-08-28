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

package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1;

import com.orientechnologies.orient.core.exception.ODurableComponentException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/30/13
 */
public final class OCellBTreeSingleValueV1Exception extends ODurableComponentException {

  @SuppressWarnings("unused")
  public OCellBTreeSingleValueV1Exception(final OCellBTreeSingleValueV1Exception exception) {
    super(exception);
  }

  OCellBTreeSingleValueV1Exception(final String message, final CellBTreeSingleValueV1 component) {
    super(message, component);
  }
}
