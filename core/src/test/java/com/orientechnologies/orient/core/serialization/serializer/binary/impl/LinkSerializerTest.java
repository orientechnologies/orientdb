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

package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 07.02.12
 */
public class LinkSerializerTest {
  private static final int              FIELD_SIZE = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
  private ORecordId                     OBJECT;
  private static final int              clusterId  = 5;
  private static final OClusterPosition position   = OClusterPositionFactory.INSTANCE.valueOf(100500L);
  private OLinkSerializer               linkSerializer;
  byte[]                                stream     = new byte[FIELD_SIZE];

  @BeforeClass
  public void beforeClass() {
    OBJECT = new ORecordId(clusterId, position);
    linkSerializer = new OLinkSerializer();
  }

  @Test
  public void testFieldSize() {
    Assert.assertEquals(linkSerializer.getObjectSize(null), FIELD_SIZE);
  }

  @Test
  public void testSerialize() {
    linkSerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(linkSerializer.deserialize(stream, 0), OBJECT);
  }
}
