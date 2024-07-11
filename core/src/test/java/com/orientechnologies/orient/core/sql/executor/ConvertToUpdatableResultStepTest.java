package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

/** Created by olena.kolesnyk on 03/08/2017. */
public class ConvertToUpdatableResultStepTest extends TestUtilsFixture {

  private static final String STRING_PROPERTY = "stringPropertyName";
  private static final String INTEGER_PROPERTY = "integerPropertyName";
  private List<ODocument> documents = new ArrayList<>();

  @Test
  public void shouldConvertUpdatableResult() {
    OCommandContext context = new OBasicCommandContext(db);
    ConvertToUpdatableResultStep step = new ConvertToUpdatableResultStep(context, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
            List<OResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                ODocument document = new ODocument();
                document.setProperty(STRING_PROPERTY, RandomStringUtils.randomAlphanumeric(10));
                document.setProperty(INTEGER_PROPERTY, new Random().nextInt());
                documents.add(document);
                result.add(new OResultInternal(document));
              }
              done = true;
            }
            return OExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    OExecutionStream result = step.start(context);

    int counter = 0;
    while (result.hasNext(context)) {
      OResult currentItem = result.next(context);
      if (!(currentItem.getClass().equals(OUpdatableResult.class))) {
        Assert.fail("There is an item in result set that is not an instance of OUpdatableResult");
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
