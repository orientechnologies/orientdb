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
package com.orientechnologies.orient.test.database.auto;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;

/**
 * SQL test against metadata.
 */
@Test(groups = "sql-select")
public class SQLMetadataTest extends DocumentDBBaseTest {
	@Parameters(value = "url")
	public SQLMetadataTest(@Optional String url) {
		super(url);
	}
  @Test
  public void querySchemaClasses() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select expand(classes) from metadata:schema"))
        .execute();

    Assert.assertTrue(result.size() != 0);
  }

  @Test
  public void querySchemaProperties() {
    List<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>(
            "select expand(properties) from (select expand(classes) from metadata:schema) where name = 'OUser'")).execute();

    Assert.assertTrue(result.size() != 0);
  }

  @Test
  public void queryIndexes() {
    List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select expand(indexes) from metadata:indexmanager"))
        .execute();

    Assert.assertTrue(result.size() != 0);
  }

  @Test
  public void queryMetadataNotSupported() {
    try {
      database.command(new OSQLSynchQuery<ODocument>("select expand(indexes) from metadata:blaaa")).execute();
      Assert.fail();
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OQueryParsingException);
    } catch (OQueryParsingException e) {
    }

  }
}
