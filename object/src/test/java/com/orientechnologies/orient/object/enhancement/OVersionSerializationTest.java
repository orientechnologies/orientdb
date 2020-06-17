/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.orient.object.enhancement;

import com.orientechnologies.orient.core.annotation.OId;
import com.orientechnologies.orient.core.annotation.OVersion;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 * @since 12/14/12
 */
public class OVersionSerializationTest {
  private OObjectDatabaseTx database;

  @Before
  public void setUp() throws Exception {
    database = new OObjectDatabaseTx("memory:OVersionSerializationTest");
    database.create();

    database.getEntityManager().registerEntityClass(EntityLongVersion.class);
    database.getEntityManager().registerEntityClass(EntityStringVersion.class);
    database.getEntityManager().registerEntityClass(EntityObjectVersion.class);
    database.getEntityManager().registerEntityClass(EntityExactVersionType.class);
  }

  @After
  public void tearDown() {
    database.drop();
  }

  @Test
  public void testExactVersionTypeSerialization() throws Exception {
    final EntityExactVersionType object = database.save(new EntityExactVersionType());
    final EntityExactVersionType loadedObject = database.load(object.getRid());

    Assert.assertEquals(object.getVersion(), loadedObject.getVersion());

    final ODocument document = database.getUnderlying().load(object.getRid());
    Assert.assertEquals(loadedObject.getVersion(), document.getVersion());
  }

  @Test
  public void testIntegerSerialization() throws Exception {
    final EntityLongVersion object = database.save(new EntityLongVersion());
    final EntityLongVersion loadedObject = database.load(object.getRid());

    Assert.assertNotNull(object.getVersion());
    Assert.assertNotNull(loadedObject.getVersion());
    Assert.assertEquals(object.getVersion(), loadedObject.getVersion());

    final ODocument document = database.getUnderlying().load(object.getRid());
    Assert.assertEquals(loadedObject.getVersion().intValue(), document.getVersion());
  }

  public static class EntityStringVersion {
    @OId private ORID rid;

    @OVersion private String version;

    public EntityStringVersion() {}

    public ORID getRid() {
      return rid;
    }

    public String getVersion() {
      return version;
    }
  }

  public static class EntityObjectVersion {
    @OId private ORID rid;

    @OVersion private Object version;

    public EntityObjectVersion() {}

    public ORID getRid() {
      return rid;
    }

    public Object getVersion() {
      return version;
    }
  }

  public static class EntityExactVersionType {
    @OId private ORID rid;

    @OVersion private int version;

    public EntityExactVersionType() {}

    public ORID getRid() {
      return rid;
    }

    public int getVersion() {
      return version;
    }
  }

  public static class EntityLongVersion {
    @OId private ORID rid;

    @OVersion private Long version;

    public EntityLongVersion() {}

    public ORID getRid() {
      return rid;
    }

    public Long getVersion() {
      return version;
    }
  }
}
