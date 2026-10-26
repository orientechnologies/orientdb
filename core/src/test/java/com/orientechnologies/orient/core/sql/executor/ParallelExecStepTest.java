package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.id.ORecordId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/** Created by luigidellaquila on 26/07/16. */
public class ParallelExecStepTest {

  @Test
  public void test() {
    List<OInternalExecutionPlan> subPlans = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      FetchFromRidsStep step0 = new FetchFromRidsStep(Collections.singleton(new ORecordId(12, i)));
      FetchFromRidsStep step1 = new FetchFromRidsStep(Collections.singleton(new ORecordId(12, i)));
      OInternalExecutionPlan plan = new OSelectExecutionPlan();
      plan.getSteps().add(step0);
      plan.getSteps().add(step1);
      subPlans.add(plan);
    }

    ParallelExecStep step = new ParallelExecStep(subPlans);

    OSelectExecutionPlan plan = new OSelectExecutionPlan();
    plan.getSteps().add(new FetchFromRidsStep(Collections.singleton(new ORecordId(12, 100))));
    plan.getSteps().add(step);
    plan.getSteps().add(new FetchFromRidsStep(Collections.singleton(new ORecordId(12, 100))));
  }
}
