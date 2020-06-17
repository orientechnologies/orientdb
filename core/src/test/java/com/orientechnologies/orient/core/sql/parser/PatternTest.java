package com.orientechnologies.orient.core.sql.parser;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/** Created by luigidellaquila on 11/10/16. */
public class PatternTest extends OParserTestAbstract {

  @Test
  public void testSimplePattern() {
    String query = "MATCH {as:a, class:Person} return a";
    OrientSql parser = getParserFor(query);
    try {
      OMatchStatement stm = (OMatchStatement) parser.parse();
      stm.buildPatterns();
      Pattern pattern = stm.pattern;
      Assert.assertEquals(0, pattern.getNumOfEdges());
      Assert.assertEquals(1, pattern.getAliasToNode().size());
      Assert.assertNotNull(pattern.getAliasToNode().get("a"));
      Assert.assertEquals(1, pattern.getDisjointPatterns().size());
    } catch (ParseException e) {
      Assert.fail();
    }
  }

  @Test
  public void testCartesianProduct() {
    String query = "MATCH {as:a, class:Person}, {as:b, class:Person} return a, b";
    OrientSql parser = getParserFor(query);
    try {
      OMatchStatement stm = (OMatchStatement) parser.parse();
      stm.buildPatterns();
      Pattern pattern = stm.pattern;
      Assert.assertEquals(0, pattern.getNumOfEdges());
      Assert.assertEquals(2, pattern.getAliasToNode().size());
      Assert.assertNotNull(pattern.getAliasToNode().get("a"));
      List<Pattern> subPatterns = pattern.getDisjointPatterns();
      Assert.assertEquals(2, subPatterns.size());
      Assert.assertEquals(0, subPatterns.get(0).getNumOfEdges());
      Assert.assertEquals(1, subPatterns.get(0).getAliasToNode().size());
      Assert.assertEquals(0, subPatterns.get(1).getNumOfEdges());
      Assert.assertEquals(1, subPatterns.get(1).getAliasToNode().size());

      Set<String> aliases = new HashSet<>();
      aliases.add("a");
      aliases.add("b");
      aliases.remove(subPatterns.get(0).getAliasToNode().keySet().iterator().next());
      aliases.remove(subPatterns.get(1).getAliasToNode().keySet().iterator().next());
      Assert.assertEquals(0, aliases.size());

    } catch (ParseException e) {
      Assert.fail();
    }
  }

  @Test
  public void testComplexCartesianProduct() {
    String query =
        "MATCH {as:a, class:Person}-->{as:b}, {as:c, class:Person}-->{as:d}-->{as:e}, {as:d, class:Foo}-->{as:f} return a, b";
    OrientSql parser = getParserFor(query);
    try {
      OMatchStatement stm = (OMatchStatement) parser.parse();
      stm.buildPatterns();
      Pattern pattern = stm.pattern;
      Assert.assertEquals(4, pattern.getNumOfEdges());
      Assert.assertEquals(6, pattern.getAliasToNode().size());
      Assert.assertNotNull(pattern.getAliasToNode().get("a"));
      List<Pattern> subPatterns = pattern.getDisjointPatterns();
      Assert.assertEquals(2, subPatterns.size());

      Set<String> aliases = new HashSet<>();
      aliases.add("a");
      aliases.add("b");
      aliases.add("c");
      aliases.add("d");
      aliases.add("e");
      aliases.add("f");
      aliases.removeAll(subPatterns.get(0).getAliasToNode().keySet());
      aliases.removeAll(subPatterns.get(1).getAliasToNode().keySet());
      Assert.assertEquals(0, aliases.size());

    } catch (ParseException e) {
      Assert.fail();
    }
  }
}
