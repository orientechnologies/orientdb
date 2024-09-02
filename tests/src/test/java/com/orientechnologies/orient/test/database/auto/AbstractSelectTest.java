package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public abstract class AbstractSelectTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  protected AbstractSelectTest(@Optional String url) {
    super(url);
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseDocumentInternal db, Object... args) {
    final List<ODocument> synchResult = db.query(new OSQLSynchQuery<ODocument>(sql), args);
    return synchResult;
  }
}
