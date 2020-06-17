package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.orient.core.sql.OIndexSearchResult;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import org.junit.Assert;
import org.junit.Test;

public class OFilterOptimizerTest {

  private final OFilterOptimizer optimizer = new OFilterOptimizer();

  @Test
  public void testOptimizeFullOptimization() throws Exception {
    final OSQLFilter filter = OSQLEngine.getInstance().parseCondition("a = 3", null, "WHERE");

    final OSQLFilterCondition condition = filter.getRootCondition();

    final OIndexSearchResult searchResult =
        new OIndexSearchResult(
            condition.getOperator(),
            ((OSQLFilterItemField) condition.getLeft()).getFieldChain(),
            3);

    optimizer.optimize(filter, searchResult);

    Assert.assertNull(filter.getRootCondition());
  }

  @Test
  public void testOptimizeFullOptimizationComplex() throws Exception {
    final OSQLFilter filter =
        OSQLEngine.getInstance().parseCondition("a = 3 and b = 4", null, "WHERE");

    final OSQLFilterCondition condition = filter.getRootCondition();

    final OIndexSearchResult searchResult;
    {
      final OIndexSearchResult searchResult1;
      {
        final OSQLFilterCondition cnd = (OSQLFilterCondition) condition.getLeft();
        searchResult1 =
            new OIndexSearchResult(
                cnd.getOperator(), ((OSQLFilterItemField) cnd.getLeft()).getFieldChain(), 3);
      }
      final OIndexSearchResult searchResult2;
      {
        final OSQLFilterCondition cnd = (OSQLFilterCondition) condition.getRight();
        searchResult2 =
            new OIndexSearchResult(
                cnd.getOperator(), ((OSQLFilterItemField) cnd.getLeft()).getFieldChain(), 4);
      }
      searchResult = searchResult1.merge(searchResult2);
    }

    optimizer.optimize(filter, searchResult);

    Assert.assertNull(filter.getRootCondition());
  }

  @Test
  public void testOptimizePartialOptimization() throws Exception {
    final OSQLFilter filter =
        OSQLEngine.getInstance().parseCondition("a = 3 and b > 5", null, "WHERE");

    final OSQLFilterCondition condition = filter.getRootCondition();

    final OIndexSearchResult searchResult =
        new OIndexSearchResult(
            ((OSQLFilterCondition) condition.getLeft()).getOperator(),
            ((OSQLFilterItemField) ((OSQLFilterCondition) condition.getLeft()).getLeft())
                .getFieldChain(),
            3);

    optimizer.optimize(filter, searchResult);

    Assert.assertEquals(filter.getRootCondition().toString(), "(b > 5)");
  }

  @Test
  public void testOptimizePartialOptimizationMethod() throws Exception {
    final OSQLFilter filter =
        OSQLEngine.getInstance().parseCondition("a = 3 and b.asFloat() > 3.14", null, "WHERE");

    final OSQLFilterCondition condition = filter.getRootCondition();

    final OIndexSearchResult searchResult =
        new OIndexSearchResult(
            ((OSQLFilterCondition) condition.getLeft()).getOperator(),
            ((OSQLFilterItemField) ((OSQLFilterCondition) condition.getLeft()).getLeft())
                .getFieldChain(),
            3);

    optimizer.optimize(filter, searchResult);

    Assert.assertEquals(filter.getRootCondition().toString(), "(b.asfloat > 3.14)");
  }
}
