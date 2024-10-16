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
package com.orientechnologies.orient.test.database.auto;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** SQL test against metadata. */
@Test(groups = "sql-select")
public class SQLMetadataTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public SQLMetadataTest(@Optional String url) {
    super(url);
  }

  @Test
  public void querySchemaClasses() {
    List<OResult> result =
        database.command("select expand(classes) from metadata:schema").stream().toList();

    Assert.assertTrue(result.size() != 0);
  }

  @Test
  public void querySchemaProperties() {
    List<OResult> result =
        database
            .command(
                "select expand(properties) from (select expand(classes) from metadata:schema)"
                    + " where name = 'OUser'")
            .stream()
            .toList();

    Assert.assertTrue(result.size() != 0);
  }

  @Test
  public void queryIndexes() {
    List<OResult> result =
        database.command("select expand(indexes) from metadata:indexmanager").stream().toList();

    Assert.assertTrue(result.size() != 0);
  }

  @Test
  public void queryMetadataNotSupported() {
    try {
      database.command("select expand(indexes) from metadata:blaaa").next();
      Assert.fail();
    } catch (UnsupportedOperationException e) {
    } catch (OStorageException e) {
      assertTrue(e.getCause() instanceof UnsupportedOperationException);
    }
  }
}
