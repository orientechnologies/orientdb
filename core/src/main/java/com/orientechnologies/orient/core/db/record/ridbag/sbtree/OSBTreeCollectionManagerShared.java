/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsaiLocal;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeCollectionManagerShared extends OSBTreeCollectionManagerAbstract {

  public OSBTreeCollectionManagerShared() {
    super();
  }

  public OSBTreeCollectionManagerShared(int evictionThreshold, int cacheMaxSize) {
    super(evictionThreshold, cacheMaxSize);
  }

  @Override
  protected OSBTreeBonsaiLocal<OIdentifiable, Integer> createTree(String extension) {
    return new OSBTreeBonsaiLocal<OIdentifiable, Integer>(extension, true);
  }
}
