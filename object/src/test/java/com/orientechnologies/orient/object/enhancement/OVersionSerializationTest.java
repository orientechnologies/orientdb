/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 * @since 12/14/12
 */
public class OVersionSerializationTest {
  private OObjectDatabaseTx database;

  @BeforeClass
  protected void setUp() throws Exception {
    database = new OObjectDatabaseTx("memory:OVersionSerializationTest");
    database.create();

    database.getEntityManager().registerEntityClass(EntityStringVersion.class);
    database.getEntityManager().registerEntityClass(EntityObjectVersion.class);
    database.getEntityManager().registerEntityClass(EntityExactVersionType.class);
    if (!OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.getValueAsBoolean())
      database.getEntityManager().registerEntityClass(EntityLongVersion.class);
  }

  @AfterClass
  protected void tearDown() {
    database.drop();
  }

  @Test
  public void testStringSerialization() throws Exception {
    final EntityStringVersion object = database.save(new EntityStringVersion());
    final EntityStringVersion loadedObject = database.load(object.getRid());

    Assert.assertNotSame(object.getVersion(), loadedObject.getVersion(), "The same object of entity is shared between entities");
    Assert.assertEquals(object.getVersion(), loadedObject.getVersion());

    final ORecordVersion version = OVersionFactory.instance().createVersion();
    version.getSerializer().fromString(loadedObject.getVersion(), version);
    final ODocument document = database.getUnderlying().load(object.getRid());
    Assert.assertEquals(version, document.getRecordVersion());
    Assert.assertEquals(loadedObject.getVersion(), document.getRecordVersion().toString());
  }

  @Test
  public void testObjectSerialization() throws Exception {
    final EntityObjectVersion object = database.save(new EntityObjectVersion());
    final EntityObjectVersion loadedObject = database.load(object.getRid());

    Assert.assertTrue(object.getVersion() instanceof ORecordVersion);
    Assert.assertTrue(loadedObject.getVersion() instanceof ORecordVersion);
    Assert.assertNotSame(object.getVersion(), loadedObject.getVersion(), "The same object of entity is shared between entities");
    Assert.assertEquals(object.getVersion(), loadedObject.getVersion());

    final ODocument document = database.getUnderlying().load(object.getRid());
    Assert.assertEquals(loadedObject.getVersion(), document.getRecordVersion());
  }

  @Test
  public void testExactVersionTypeSerialization() throws Exception {
    final EntityExactVersionType object = database.save(new EntityExactVersionType());
    final EntityExactVersionType loadedObject = database.load(object.getRid());

    Assert.assertNotNull(object.getVersion());
    Assert.assertNotNull(loadedObject.getVersion());
    Assert.assertNotSame(object.getVersion(), loadedObject.getVersion(), "The same object of entity is shared between entities");
    Assert.assertEquals(object.getVersion(), loadedObject.getVersion());

    final ODocument document = database.getUnderlying().load(object.getRid());
    Assert.assertEquals(loadedObject.getVersion(), document.getRecordVersion());
  }

  @Test
  public void testIntegerSerialization() throws Exception {
    if (OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.getValueAsBoolean())
      return;

    final EntityLongVersion object = database.save(new EntityLongVersion());
    final EntityLongVersion loadedObject = database.load(object.getRid());

    Assert.assertNotNull(object.getVersion());
    Assert.assertNotNull(loadedObject.getVersion());
    Assert.assertEquals(object.getVersion(), loadedObject.getVersion());

    final ODocument document = database.getUnderlying().load(object.getRid());
    Assert.assertEquals(loadedObject.getVersion().intValue(), document.getRecordVersion().getCounter());
  }

  public static class EntityStringVersion {
    @OId
    private ORID   rid;

    @OVersion
    private String version;

    public EntityStringVersion() {
    }

    public ORID getRid() {
      return rid;
    }

    public String getVersion() {
      return version;
    }
  }

  public static class EntityObjectVersion {
    @OId
    private ORID   rid;

    @OVersion
    private Object version;

    public EntityObjectVersion() {
    }

    public ORID getRid() {
      return rid;
    }

    public Object getVersion() {
      return version;
    }
  }

  public static class EntityExactVersionType {
    @OId
    private ORID           rid;

    @OVersion
    private ORecordVersion version;

    public EntityExactVersionType() {
    }

    public ORID getRid() {
      return rid;
    }

    public ORecordVersion getVersion() {
      return version;
    }
  }

  public static class EntityLongVersion {
    @OId
    private ORID rid;

    @OVersion
    private Long version;

    public EntityLongVersion() {
    }

    public ORID getRid() {
      return rid;
    }

    public Long getVersion() {
      return version;
    }
  }
}
