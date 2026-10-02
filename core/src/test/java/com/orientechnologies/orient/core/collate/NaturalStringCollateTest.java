package com.orientechnologies.orient.core.collate;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class NaturalStringCollateTest {

    @Test
    public void testNaturalSortOrder() {
        NaturalStringCollate collate = new NaturalStringCollate();

        // 测试基本的自然排序
        List<String> testData = new ArrayList<>();
        testData.add("M10");
        testData.add("M2");
        testData.add("M1");
        testData.add("M20");
        testData.add("M3");

        // 使用自然排序进行排序
        Collections.sort(testData, (a, b) -> collate.compareForOrderBy(a, b));

        // 验证排序结果
        assertEquals("M1", testData.get(0));
        assertEquals("M2", testData.get(1));
        assertEquals("M3", testData.get(2));
        assertEquals("M10", testData.get(3));
        assertEquals("M20", testData.get(4));
    }

    @Test
    public void testComplexNaturalSortOrder() {
        NaturalStringCollate collate = new NaturalStringCollate();

        List<String> testData = new ArrayList<>();
        testData.add("item100");
        testData.add("item2");
        testData.add("item1");
        testData.add("item20");
        testData.add("item3");
        testData.add("item10");

        Collections.sort(testData, (a, b) -> collate.compareForOrderBy(a, b));

        assertEquals("item1", testData.get(0));
        assertEquals("item2", testData.get(1));
        assertEquals("item3", testData.get(2));
        assertEquals("item10", testData.get(3));
        assertEquals("item20", testData.get(4));
        assertEquals("item100", testData.get(5));
    }

    @Test
    public void testMixedContentNaturalSort() {
        NaturalStringCollate collate = new NaturalStringCollate();

        List<String> testData = new ArrayList<>();
        testData.add("version2.10");
        testData.add("version2.2");
        testData.add("version2.1");
        testData.add("version10.1");
        testData.add("version1.10");

        Collections.sort(testData, (a, b) -> collate.compareForOrderBy(a, b));

        assertEquals("version1.10", testData.get(0));
        assertEquals("version2.1", testData.get(1));
        assertEquals("version2.2", testData.get(2));
        assertEquals("version2.10", testData.get(3));
        assertEquals("version10.1", testData.get(4));
    }

    @Test
    public void testLeadingZeros() {
        NaturalStringCollate collate = new NaturalStringCollate();

        List<String> testData = new ArrayList<>();
        testData.add("test0003");
        testData.add("test02");
        testData.add("test1");
        testData.add("test001");

        Collections.sort(testData, (a, b) -> collate.compareForOrderBy(a, b));

        assertEquals("test1", testData.get(0));
        assertEquals("test02", testData.get(1));
        assertEquals("test001", testData.get(2));
        assertEquals("test0003", testData.get(3));
    }

    @Test
    public void testNullValues() {
        NaturalStringCollate collate = new NaturalStringCollate();

        // 测试 null 值处理
        assertTrue(collate.compareForOrderBy(null, null) == 0);
        assertTrue(collate.compareForOrderBy(null, "test") < 0);
        assertTrue(collate.compareForOrderBy("test", null) > 0);
    }

    @Test
    public void testEqualValues() {
        NaturalStringCollate collate = new NaturalStringCollate();

        // 测试相等值
        assertEquals(0, collate.compareForOrderBy("test", "test"));
        assertEquals(0, collate.compareForOrderBy("item10", "item10"));
    }

    @Test
    public void testGetName() {
        NaturalStringCollate collate = new NaturalStringCollate();
        assertEquals("NATURAL", collate.getName());
    }

    @Test
    public void testTransform() {
        NaturalStringCollate collate = new NaturalStringCollate();

        // 测试 transform 方法（应该返回原始对象）
        String testString = "test";
        assertEquals(testString, collate.transform(testString));

        Integer testInt = 123;
        assertEquals(testInt, collate.transform(testInt));

        assertNull(collate.transform(null));
    }

    @Test
    public void testFactoryIntegration() {
        NaturalStringCollateFactory factory = new NaturalStringCollateFactory();

        // 测试工厂是否能正确创建 NaturalStringCollate 实例
        OCollate collate = factory.getCollate("NATURAL");
        assertNotNull("Factory should return a collate instance for NATURAL", collate);
        assertEquals("NATURAL", collate.getName());

        // 测试大小写不敏感
        OCollate collate2 = factory.getCollate("natural");
        assertNotNull("Factory should work case-insensitively", collate2);

        // 测试不存在的 collate
        OCollate collate3 = factory.getCollate("NONEXISTENT");
        assertNull("Factory should return null for non-existent collate", collate3);

        // 测试 getNames 方法
        assertTrue("Factory should support NATURAL collate", factory.getNames().contains("NATURAL"));
    }
}
