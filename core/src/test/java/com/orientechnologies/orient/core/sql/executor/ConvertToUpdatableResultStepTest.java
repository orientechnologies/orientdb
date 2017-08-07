package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by olena.kolesnyk on 03/08/2017.
 */
public class ConvertToUpdatableResultStepTest extends TestUtilsFixture {

    @Test
    public void shouldConvertUpdatableResult() {
        OCommandContext context = new OBasicCommandContext();
        ConvertToUpdatableResultStep step = new ConvertToUpdatableResultStep(context, false);
        AbstractExecutionStep previous = new AbstractExecutionStep(context, false) {
            boolean done = false;

            @Override
            public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
                OInternalResultSet result = new OInternalResultSet();
                if (!done) {
                    for (int i = 0; i < 10; i++) {
                        OResultInternal item = new OResultInternal();
                        item.setElement(new ODocument(createClassInstance().getName()));
                        result.add(item);
                    }
                    done = true;
                }
                return result;
            }
        };

        step.setPrevious(previous);
        OResultSet result = step.syncPull(context, 10);
        while (result.hasNext()) {
            if (!(result.next().getClass().equals(OUpdatableResult.class))) {
                Assert.fail("There is an item in result set that is not an instance of OUpdatableResult");
            }
        }
    }

}
