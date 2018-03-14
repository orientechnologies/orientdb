/*
 * Copyright 2018 OrientDB.
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
package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import java.io.IOException;
import java.io.PrintStream;

/**
 *
 * @author mdjurovi
 */
public interface OSBTreeRidBag extends ORidBagDelegate{
  OBonsaiCollectionPointer getCollectionPointer();
  void setCollectionPointer(OBonsaiCollectionPointer collectionPointer);
  void debugPrint(PrintStream writer) throws IOException;
  void mergeChanges(OSBTreeRidBag treeRidBag);
  void clearChanges();
  void confirmDelete();
}
