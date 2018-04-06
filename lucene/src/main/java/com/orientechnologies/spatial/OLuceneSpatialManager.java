/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.spatial.shape.OShapeBuilder;

import static com.orientechnologies.spatial.shape.OShapeBuilder.BASE_CLASS;

/**
 * Created by Enrico Risa on 06/08/15.
 */
public class OLuceneSpatialManager {

  private final OShapeBuilder shapeBuilder;

  public OLuceneSpatialManager(OShapeBuilder shapeBuilder) {
    this.shapeBuilder = shapeBuilder;
  }

  public void init(ODatabaseInternal db) {

    //This is just for bypass the distributed listener that conflict with create class
    db.registerListener(new ODatabaseListener() {
      @Override
      public void onCreate(ODatabase iDatabase) {
        internalInit((ODatabaseInternal) iDatabase);
        iDatabase.unregisterListener(this);
      }

      @Override
      public void onDelete(ODatabase iDatabase) {

      }

      @Override
      public void onOpen(ODatabase iDatabase) {
        internalInit((ODatabaseInternal) iDatabase);
        iDatabase.unregisterListener(this);
      }

      @Override
      public void onBeforeTxBegin(ODatabase iDatabase) {

      }

      @Override
      public void onBeforeTxRollback(ODatabase iDatabase) {

      }

      @Override
      public void onAfterTxRollback(ODatabase iDatabase) {

      }

      @Override
      public void onBeforeTxCommit(ODatabase iDatabase) {

      }

      @Override
      public void onAfterTxCommit(ODatabase iDatabase) {

      }

      @Override
      public void onClose(ODatabase iDatabase) {

      }

      @Override
      public void onBeforeCommand(OCommandRequestText iCommand, OCommandExecutor executor) {

      }

      @Override
      public void onAfterCommand(OCommandRequestText iCommand, OCommandExecutor executor, Object result) {

      }
    });

  }

  private void internalInit(ODatabaseInternal db) {
    if (db.getMetadata().getSchema().getClass(BASE_CLASS) == null) {
      db.getMetadata().getSchema().createAbstractClass(BASE_CLASS);
      shapeBuilder.initClazz(db);
    }
  }
}
