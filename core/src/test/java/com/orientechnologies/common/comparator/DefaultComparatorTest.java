package com.orientechnologies.common.comparator;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @author Daniel Ritter
 * @since 11.07.12
 */
public class DefaultComparatorTest {
  private ODefaultComparator comparator = ODefaultComparator.INSTANCE;

  @Test
  public void testCompareStrings() {
    final OCompositeKey keyOne = new OCompositeKey("name4", OType.STRING);
    final OCompositeKey keyTwo = new OCompositeKey("name5", OType.STRING);

    assertCompareTwoKeys(comparator, keyOne, keyTwo);
  }

  private void assertCompareTwoKeys(
      final Comparator<Object> comparator, final Object keyOne, final Object keyTwo) {
    Assert.assertTrue(comparator.compare(keyOne, keyTwo) < 0);
    Assert.assertTrue(comparator.compare(keyTwo, keyOne) > 0);
    Assert.assertTrue(comparator.compare(keyTwo, keyTwo) == 0);
  }

  @Test
  public void testCompareLists() {
    List<String> listOne = List.of("a");
    List<String> listTwo = List.of("b");

    assertCompareTwoKeys(comparator, listOne, listTwo);
  }

  @Test
  public void testCompareSets() {
    Set<String> setOne = Set.of("a");
    Set<String> setTwo = Set.of("b");

    assertCompareTwoKeys(comparator, setOne, setTwo);
  }

  @Test
  public void testCompareMaps() {
    Map<String, String> mapOne = Map.of("a", "a");
    Map<String, String> mapTwo = Map.of("b", "b");

    assertCompareTwoKeys(comparator, mapOne, mapTwo);
    Map<String, String> mapThree = Map.of("a", "a");
    Map<String, String> mepFor = Map.of("a", "b");
    assertCompareTwoKeys(comparator, mapThree, mepFor);
  }

  @Test
  public void testCompareDocuments() {
    ODocument docOne = new ODocument("a", "a");
    ODocument docTwo = new ODocument("b", "b");

    assertCompareTwoKeys(comparator, docOne, docTwo);
    ODocument docThree = new ODocument("a", "a");
    ODocument docFor = new ODocument("a", "b");
    assertCompareTwoKeys(comparator, docThree, docFor);
  }
}
