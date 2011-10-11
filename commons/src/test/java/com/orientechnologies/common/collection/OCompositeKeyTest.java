package com.orientechnologies.common.collection;

import com.orientechnologies.common.exception.OException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class OCompositeKeyTest {

    @Test
    public void testEqualSameKeys() {
        final OCompositeKey compositeKey = new OCompositeKey();

        compositeKey.addKey("a");
        compositeKey.addKey("b");

        final OCompositeKey anotherCompositeKey = new OCompositeKey();
        anotherCompositeKey.addKey("a");
        anotherCompositeKey.addKey("b");

        assertTrue(compositeKey.equals(anotherCompositeKey));
        assertTrue(compositeKey.hashCode() == anotherCompositeKey.hashCode());
    }

    @Test
    public void testEqualNotSameKeys() {
        final OCompositeKey compositeKey = new OCompositeKey();

        compositeKey.addKey("a");
        compositeKey.addKey("b");

        final OCompositeKey anotherCompositeKey = new OCompositeKey();
        anotherCompositeKey.addKey("a");
        anotherCompositeKey.addKey("b");
        anotherCompositeKey.addKey("c");

        assertFalse(compositeKey.equals(anotherCompositeKey));
    }

    @Test
    public void testEqualNull() {
        final OCompositeKey compositeKey = new OCompositeKey();
        assertFalse(compositeKey.equals(null));
    }

    @Test
    public void testEqualSame() {
        final OCompositeKey compositeKey = new OCompositeKey();
        assertTrue(compositeKey.equals(compositeKey));
    }

    @Test
    public void testEqualDiffClass() {
        final OCompositeKey compositeKey = new OCompositeKey();
        assertFalse(compositeKey.equals("1"));
    }

    @Test
    public void testAddKeyComparable() {
        final OCompositeKey compositeKey = new OCompositeKey();

        compositeKey.addKey("a");

        assertEquals(compositeKey.getKeys().size(), 1);
        assertTrue(compositeKey.getKeys().contains("a"));
    }

    @Test
    public void testAddKeyComposite() {
        final OCompositeKey compositeKey = new OCompositeKey();

        compositeKey.addKey("a");

        final OCompositeKey compositeKeyToAdd = new OCompositeKey();
        compositeKeyToAdd.addKey("a");
        compositeKeyToAdd.addKey("b");

        compositeKey.addKey(compositeKeyToAdd);

        assertEquals(compositeKey.getKeys().size(), 3);
        assertTrue(compositeKey.getKeys().contains("a"));
        assertTrue(compositeKey.getKeys().contains("b"));
    }

    @Test
    public void testCompareToSame() {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey("a");
        compositeKey.addKey("b");

        final OCompositeKey anotherCompositeKey = new OCompositeKey();
        anotherCompositeKey.addKey("a");
        anotherCompositeKey.addKey("b");

        assertEquals(compositeKey.compareTo(anotherCompositeKey), 0);
    }

    @Test
    public void testCompareToPartiallyOneCase() {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey("a");
        compositeKey.addKey("b");

        final OCompositeKey anotherCompositeKey = new OCompositeKey();
        anotherCompositeKey.addKey("a");
        anotherCompositeKey.addKey("b");
        anotherCompositeKey.addKey("c");

        assertEquals(compositeKey.compareTo(anotherCompositeKey), 0);
    }

    @Test
    public void testCompareToPartiallySecondCase() {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey("a");
        compositeKey.addKey("b");
        compositeKey.addKey("c");

        final OCompositeKey anotherCompositeKey = new OCompositeKey();
        anotherCompositeKey.addKey("a");
        anotherCompositeKey.addKey("b");

        assertEquals(compositeKey.compareTo(anotherCompositeKey), 0);
    }

    @Test
    public void testCompareToGT() {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey("b");

        final OCompositeKey anotherCompositeKey = new OCompositeKey();
        anotherCompositeKey.addKey("a");
        anotherCompositeKey.addKey("b");

        assertEquals(compositeKey.compareTo(anotherCompositeKey), 1);
    }

    @Test
    public void testCompareToLT() {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey("a");
        compositeKey.addKey("b");

        final OCompositeKey anotherCompositeKey = new OCompositeKey();

        anotherCompositeKey.addKey("b");

        assertEquals(compositeKey.compareTo(anotherCompositeKey), -1);
    }

    @Test
    public void testCompareToSymmetryOne() {
        final OCompositeKey compositeKeyOne = new OCompositeKey();
        compositeKeyOne.addKey(1);
        compositeKeyOne.addKey(2);


        final OCompositeKey compositeKeyTwo = new OCompositeKey();
        compositeKeyTwo.addKey(1);
        compositeKeyTwo.addKey(3);
        compositeKeyTwo.addKey(1);

        assertEquals(compositeKeyOne.compareTo(compositeKeyTwo), -1);
        assertEquals(compositeKeyTwo.compareTo(compositeKeyOne), 1);
    }

    @Test
    public void testCompareToSymmetryTwo() {
        final OCompositeKey compositeKeyOne = new OCompositeKey();
        compositeKeyOne.addKey(1);
        compositeKeyOne.addKey(2);


        final OCompositeKey compositeKeyTwo = new OCompositeKey();
        compositeKeyTwo.addKey(1);
        compositeKeyTwo.addKey(2);
        compositeKeyTwo.addKey(3);

        assertEquals(compositeKeyOne.compareTo(compositeKeyTwo), 0);
        assertEquals(compositeKeyTwo.compareTo(compositeKeyOne), 0);
    }
}
