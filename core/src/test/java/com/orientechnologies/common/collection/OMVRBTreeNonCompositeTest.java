package com.orientechnologies.common.collection;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.awt.*;
import java.awt.geom.QuadCurve2D;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test
public class OMVRBTreeNonCompositeTest {
    protected OMVRBTree<Double, Double> tree;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        tree = new OMVRBTreeMemory<Double, Double>(4, 0.5f);

        for(double i = 1; i < 10; i++) {
            tree.put(i, i);
        }
    }

    @Test
    public void testGetEntry() {
        assertEquals(tree.get(1.0), 1.0);
        assertEquals(tree.get(3.0), 3.0);
        assertEquals(tree.get(7.0), 7.0);
        assertEquals(tree.get(9.0), 9.0);
        assertNull(tree.get(10.0));
    }

    @Test
    public void testSubMapInclusive() {
        final ONavigableMap<Double, Double> navigableMap = tree.subMap(2.0, true, 7.0, true);

        assertEquals(navigableMap.size(), 6);

        for(double i = 2; i <= 7; i++) {
            assertTrue(navigableMap.containsKey(i));
        }
    }

    @Test
    public void testSubMapFromInclusive() {
        final ONavigableMap<Double, Double> navigableMap = tree.subMap(2.0, true, 7.0, false);

        assertEquals(navigableMap.size(), 5);

        for(double i = 2; i < 7; i++) {
            assertTrue(navigableMap.containsKey(i));
        }
    }

    @Test
    public void testSubMapToInclusive() {
        final ONavigableMap<Double, Double> navigableMap = tree.subMap(2.0, false, 7.0, true);

        assertEquals(navigableMap.size(), 5);

        for(double i = 3; i <= 7; i++) {
            assertTrue(navigableMap.containsKey(i));
        }
    }

    @Test
    public void testSubMapNonInclusive() {
        final ONavigableMap<Double, Double> navigableMap = tree.subMap(2.0, false, 7.0, false);

        assertEquals(navigableMap.size(), 4);

        for(double i = 3; i < 7; i++) {
            assertTrue(navigableMap.containsKey(i));
        }
    }

    @Test
    public void testTailMapInclusive() {
        final ONavigableMap<Double, Double> navigableMap = tree.tailMap(2.0, true);
        assertEquals(navigableMap.size(), 8);

        for(double i = 2; i <= 9; i++) {
            assertTrue(navigableMap.containsKey(i));
        }
    }

    @Test
    public void testTailMapNonInclusive() {
        final ONavigableMap<Double, Double> navigableMap = tree.tailMap(2.0, false);
        assertEquals(navigableMap.size(), 7);

        for(double i = 3; i <= 9; i++) {
            assertTrue(navigableMap.containsKey(i));
        }
    }

    @Test
    public void testHeadMapInclusive() {
        final ONavigableMap<Double, Double> navigableMap = tree.headMap(7.0, true);
        assertEquals(navigableMap.size(), 7);

        for(double i = 1; i <= 7; i++) {
            assertTrue(navigableMap.containsKey(i));
        }
    }

    @Test
    public void testHeadMapNonInclusive() {
        final ONavigableMap<Double, Double> navigableMap = tree.headMap(7.0, false);
        assertEquals(navigableMap.size(), 6);

        for(double i = 1; i < 7; i++) {
            assertTrue(navigableMap.containsKey(i));
        }
    }

    @Test
    public void testGetCeilingEntryKeyExist() {
        OMVRBTreeEntry<Double, Double> entry =
                tree.getCeilingEntry(4.0, OMVRBTree.PartialSearchMode.NONE);
        assertEquals(entry.getKey(), 4.0);

        entry = tree.getCeilingEntry(4.0, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
        assertEquals(entry.getKey(), 4.0);

        entry = tree.getCeilingEntry(4.0, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
        assertEquals(entry.getKey(), 4.0);
    }

    @Test
    public void testGetCeilingEntryKeyNotExist() {
        OMVRBTreeEntry<Double, Double> entry =
                tree.getCeilingEntry(4.3, OMVRBTree.PartialSearchMode.NONE);
        assertEquals(entry.getKey(), 5.0);

        entry = tree.getCeilingEntry(4.3, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
        assertEquals(entry.getKey(), 5.0);

        entry = tree.getCeilingEntry(4.3, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
        assertEquals(entry.getKey(), 5.0);

        entry = tree.getCeilingEntry(20.0, OMVRBTree.PartialSearchMode.NONE);
        assertNull(entry);

        entry = tree.getCeilingEntry(-1.0, OMVRBTree.PartialSearchMode.NONE);
        assertEquals(entry.getKey(), 1.0);
    }

    @Test
    public void testGetFloorEntryKeyExist() {
        OMVRBTreeEntry<Double, Double> entry =
                tree.getFloorEntry(4.0, OMVRBTree.PartialSearchMode.NONE);
        assertEquals(entry.getKey(), 4.0);

        entry = tree.getFloorEntry(4.0, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
        assertEquals(entry.getKey(), 4.0);
 }

    @Test
    public void testGetFloorEntryKeyNotExist() {
        OMVRBTreeEntry<Double, Double> entry =
                tree.getFloorEntry(4.3, OMVRBTree.PartialSearchMode.NONE);
        assertEquals(entry.getKey(), 4.0);

        entry = tree.getFloorEntry(4.3, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
        assertEquals(entry.getKey(), 4.0);

        entry = tree.getFloorEntry(4.3, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
        assertEquals(entry.getKey(), 4.0);

        entry = tree.getFloorEntry(20.0, OMVRBTree.PartialSearchMode.NONE);
        assertEquals(entry.getKey(), 9.0);

        entry = tree.getFloorEntry(-1.0, OMVRBTree.PartialSearchMode.NONE);
        assertNull(entry);
    }

    @Test
    public void testHigherEntryKeyExist() {
        OMVRBTreeEntry<Double, Double> entry = tree.getHigherEntry(4.0);
        assertEquals(entry.getKey(), 5.0);
    }

    @Test
    public void testHigherEntryKeyNotExist() {
        OMVRBTreeEntry<Double, Double> entry = tree.getHigherEntry(4.5);
        assertEquals(entry.getKey(), 5.0);
    }

     @Test
    public void testHigherEntryNullResult() {
        OMVRBTreeEntry<Double, Double> entry = tree.getHigherEntry(12.0);
        assertNull(entry);
    }

    @Test
    public void testLowerEntryNullResult() {
        OMVRBTreeEntry<Double, Double> entry = tree.getLowerEntry(0.0);
        assertNull(entry);
    }


    @Test
    public void testLowerEntryKeyExist() {
        OMVRBTreeEntry<Double, Double> entry = tree.getLowerEntry(4.0);
        assertEquals(entry.getKey(), 3.0);
    }

    @Test
    public void testLowerEntryKeyNotExist() {
        OMVRBTreeEntry<Double, Double> entry = tree.getLowerEntry(4.5);
        assertEquals(entry.getKey(), 4.0);
    }
}
