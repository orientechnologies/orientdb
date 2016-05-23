/*
 * Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.incrementalbackup;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Before;

import java.io.File;

/**
 * It test the behaviour of a LocalBackupUploader.
 */
public abstract class AbstractUploaderTest extends AbstractBackupTest {

  protected OrientBaseGraph graph;
  //  protected final String dbPath =  "target/db_upload";
  protected final String dbURL = "plocal:target/" + this.getDatabaseName();
  protected final String backupPath =  "target/backup/";
  protected final String downloadedBackupPath =  "target/downloaded-backup/";

  @Before
  public void setUp() {

    incrementalVerticesIdForThread = new int[numberOfThreads];
    for(int i=0; i < this.numberOfThreads; i++) {
      this.incrementalVerticesIdForThread[i] = 0;
    }

    try {

      this.graph = new OrientGraphNoTx(this.dbURL);

      // initial schema
      OrientVertexType userType = this.graph.createVertexType("User");
      userType.createProperty("name", OType.STRING);
      userType.createProperty("updated", OType.BOOLEAN);
      userType.createIndex("User.index", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "name");

      OrientVertexType productType = this.graph.createVertexType("Product");
      productType.createProperty("name", OType.STRING);
      productType.createProperty("updated", OType.BOOLEAN);
      productType.createIndex("Product.index", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "name");

      OrientEdgeType edgeType = this.graph.createEdgeType("bought");
      edgeType.createProperty("purchaseDate", OType.DATE);
      edgeType.createProperty("updated", OType.BOOLEAN);

      // inserting 10000 vertices
      this.banner("1st op. - Inserting 5000 triples (10000 vertices, 5000 edges)");
      executeWrites(this.dbURL, 1000);

    } catch(Exception e) {
      e.printStackTrace();
      // cleaning all the directories
      this.cleanDirectories();
    } finally {
      this.graph.shutdown();
    }
  }

  protected void cleanDirectories() {
    OFileUtils.deleteRecursively(new File("target/" + this.getDatabaseName()));
    OFileUtils.deleteRecursively(new File(this.backupPath));
    OFileUtils.deleteRecursively(new File(this.downloadedBackupPath));
  }

}
