package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

/** Created by olena.kolesnyk on 03/08/2017. */
public class ConvertToResultInternalStepTest extends TestUtilsFixture {

  private static final String STRING_PROPERTY = "stringPropertyName";
  private static final String INTEGER_PROPERTY = "integerPropertyName";
  private List<ODocument> documents = new ArrayList<>();

  @Test
  public void shouldConvertUpdatableResult() {
    OCommandContext context = new OBasicCommandContext();
    ConvertToResultInternalStep step = new ConvertToResultInternalStep(context, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
            OInternalResultSet result = new OInternalResultSet();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                ODocument document = new ODocument();
                document.setProperty(STRING_PROPERTY, RandomStringUtils.randomAlphanumeric(10));
                document.setProperty(INTEGER_PROPERTY, new Random().nextInt());
                documents.add(document);
                OUpdatableResult item = new OUpdatableResult(document);
                result.add(item);
              }
              done = true;
            }
            return result;
          }
        };

    step.setPrevious(previous);
    OResultSet result = step.syncPull(context, 10);

    int counter = 0;
    while (result.hasNext()) {
      OResult currentItem = result.next();
      if (!(currentItem.getClass().equals(OResultInternal.class))) {
        Assert.fail("There is an item in result set that is not an instance of OResultInternal");
      }
      if (!currentItem
          .getElement()
          .get()
          .getProperty(STRING_PROPERTY)
          .equals(documents.get(counter).getProperty(STRING_PROPERTY))) {
        Assert.fail("String ODocument property inside OResult instance is not preserved");
      }
      if (!currentItem
          .getElement()
          .get()
          .getProperty(INTEGER_PROPERTY)
          .equals(documents.get(counter).getProperty(INTEGER_PROPERTY))) {
        Assert.fail("Integer ODocument property inside OResult instance is not preserved");
      }
      counter++;
    }
  }
}
