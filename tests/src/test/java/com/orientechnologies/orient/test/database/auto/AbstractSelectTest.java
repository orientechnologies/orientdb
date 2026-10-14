package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public abstract class AbstractSelectTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  protected AbstractSelectTest(@Optional String url) {
    super(url);
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseDocumentInternal db, Object... args) {
    final List<ODocument> synchResult;
    if (args.length == 1 && args[0] instanceof Map) {
      synchResult =
          db.query(sql, (Map) args[0]).stream().map(x -> (ODocument) x.toElement()).toList();
    } else {
      synchResult = db.query(sql, args).stream().map(x -> (ODocument) x.toElement()).toList();
    }
    return synchResult;
  }
}
