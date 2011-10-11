package com.orientechnologies.common.util;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

@Test
public class OMultiKeyTest {
    @Test
    public void testEqualsDifferentSize() {
        final OMultiKey multiKey = new OMultiKey(Collections.singletonList("a"));
        final OMultiKey anotherMultiKey = new OMultiKey(Arrays.asList(new String[]{"a", "b"}));

        assertFalse(multiKey.equals(anotherMultiKey));
    }

    @Test
    public void testEqualsDifferentItems() {
        final OMultiKey multiKey = new OMultiKey(Arrays.asList(new String[]{"b", "c"}));
        final OMultiKey anotherMultiKey = new OMultiKey(Arrays.asList(new String[]{"a", "b"}));

        assertFalse(multiKey.equals(anotherMultiKey));
    }


    @Test
    public void testEqualsTheSame() {
        final OMultiKey multiKey = new OMultiKey(Collections.singletonList("a"));
        assertTrue(multiKey.equals(multiKey));
    }

    @Test
    public void testEqualsNull() {
        final OMultiKey multiKey = new OMultiKey(Collections.singletonList("a"));
        assertFalse(multiKey.equals(null));
    }

    @Test
    public void testEqualsDifferentClass() {
        final OMultiKey multiKey = new OMultiKey(Collections.singletonList("a"));
        assertFalse(multiKey.equals("a"));
    }

    @Test
    public void testEmptyKeyEquals() {
        final Map<OMultiKey, Object> multiKeyMap = new HashMap<OMultiKey, Object>();

        final OMultiKey multiKey = new OMultiKey(Collections.emptyList());
        multiKeyMap.put(multiKey, new Object());

        final OMultiKey anotherMultiKey = new OMultiKey(Collections.emptyList());
        final Object mapResult = multiKeyMap.get(anotherMultiKey);

        assertNotNull(mapResult);
    }


    @Test
    public void testOneKeyMap() {
        final Map<OMultiKey, Object> multiKeyMap = new HashMap<OMultiKey, Object>();

        final OMultiKey multiKey = new OMultiKey(Collections.singletonList("a"));
        multiKeyMap.put(multiKey, new Object());

        final OMultiKey anotherMultiKey = new OMultiKey(Collections.singletonList("a"));
        final Object mapResult = multiKeyMap.get(anotherMultiKey);

        assertNotNull(mapResult);
    }

    @Test
    public void testOneKeyNotInMap() {
        final Map<OMultiKey, Object> multiKeyMap = new HashMap<OMultiKey, Object>();

        final OMultiKey multiKey = new OMultiKey(Collections.singletonList("a"));
        multiKeyMap.put(multiKey, new Object());

        final OMultiKey anotherMultiKey = new OMultiKey(Collections.singletonList("b"));
        final Object mapResult = multiKeyMap.get(anotherMultiKey);

        assertNull(mapResult);
    }


    @Test
    public void testTwoKeyMap() {
        final Map<OMultiKey, Object> multiKeyMap = new HashMap<OMultiKey, Object>();

        final OMultiKey multiKey = new OMultiKey(Arrays.asList(new String[]{"a", "b"}));
        multiKeyMap.put(multiKey, new Object());

        final OMultiKey anotherMultiKey = new OMultiKey(Arrays.asList(new String[]{"a", "b"}));
        final Object mapResult = multiKeyMap.get(anotherMultiKey);

        assertNotNull(mapResult);
    }

    @Test
    public void testTwoKeyMapReordered() {
        final Map<OMultiKey, Object> multiKeyMap = new HashMap<OMultiKey, Object>();

        final OMultiKey multiKey = new OMultiKey(Arrays.asList(new String[]{"a", "b"}));
        multiKeyMap.put(multiKey, new Object());

        final OMultiKey anotherMultiKey = new OMultiKey(Arrays.asList(new String[]{"b", "a"}));
        final Object mapResult = multiKeyMap.get(anotherMultiKey);

        assertNotNull(mapResult);
    }

}
