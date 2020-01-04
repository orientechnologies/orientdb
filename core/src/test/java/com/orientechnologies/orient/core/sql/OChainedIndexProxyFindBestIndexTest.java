package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexFullText;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OChainedIndexProxyFindBestIndexTest {

  @Test
  public void testTheOnlyChoice() throws Exception {

    final OIndexUnique expectedResult = mockUniqueIndex();

    final List<OIndex> indexes = Arrays.<OIndex> asList(expectedResult);

    final OIndex result = OChainedIndexProxy.findBestIndex(indexes);

    Assert.assertSame(result, expectedResult);
  }

  @Test
  public void testDoNotUseIndexesWithNoNullValueSupport() throws Exception {

    final OIndexUnique expectedResult = mockUniqueIndex();

    final List<OIndex> indexes = Arrays.<OIndex> asList(mockUniqueCompositeHashIndex(), mockUniqueCompositeIndex(),
        expectedResult);

    final OIndex result = OChainedIndexProxy.findBestIndex(indexes);

    Assert.assertSame(result, expectedResult);
  }

  @Test
  public void testDoNotUseCompositeHashIndex() throws Exception {

    final OIndexUnique expectedResult = mockUniqueIndex();

    final List<OIndex> indexes = Arrays.<OIndex> asList(mockUniqueCompositeHashIndexWithNullSupport(), expectedResult,
        mockUniqueCompositeHashIndexWithNullSupport());

    final OIndex result = OChainedIndexProxy.findBestIndex(indexes);

    Assert.assertSame(result, expectedResult);
  }

  @Test
  public void testPriorityHashOverNonHash() throws Exception {

    final OIndexUnique expectedResult = mockUniqueHashIndex();

    final List<OIndex> indexes = Arrays.<OIndex> asList(mockUniqueIndex(), mockUniqueCompositeIndex(), expectedResult,
        mockUniqueIndex(), mockUniqueCompositeIndex());

    final OIndex result = OChainedIndexProxy.findBestIndex(indexes);

    Assert.assertSame(result, expectedResult);
  }

  @Test
  public void testPriorityNonCompositeOverComposite() throws Exception {

    final OIndexUnique expectedResult = mockUniqueIndex();

    final List<OIndex> indexes = Arrays.<OIndex> asList(mockUniqueCompositeIndexWithNullSupport(),
        mockUniqueCompositeHashIndexWithNullSupport(), expectedResult, mockUniqueCompositeIndexWithNullSupport(),
        mockUniqueCompositeHashIndexWithNullSupport());

    final OIndex result = OChainedIndexProxy.findBestIndex(indexes);

    Assert.assertSame(result, expectedResult);
  }

  private OIndexUnique mockUniqueIndex() {
    final OIndexUnique uniqueIndex = mock(OIndexUnique.class);
    when(uniqueIndex.getInternal()).thenReturn(uniqueIndex);

    final OIndexDefinition definition = mock(OIndexDefinition.class);
    when(definition.getParamCount()).thenReturn(1);

    when(uniqueIndex.getDefinition()).thenReturn(definition);
    when(uniqueIndex.getType()).thenReturn(OClass.INDEX_TYPE.UNIQUE.toString());

    return uniqueIndex;
  }

  private OIndexUnique mockUniqueCompositeIndex() {
    final OIndexUnique uniqueIndex = mock(OIndexUnique.class);
    when(uniqueIndex.getInternal()).thenReturn(uniqueIndex);

    final OIndexDefinition definition = mock(OIndexDefinition.class);
    when(definition.getParamCount()).thenReturn(2);

    when(uniqueIndex.getDefinition()).thenReturn(definition);
    when(uniqueIndex.getType()).thenReturn(OClass.INDEX_TYPE.UNIQUE.toString());

    return uniqueIndex;
  }

  private OIndexUnique mockUniqueCompositeHashIndex() {
    final OIndexUnique uniqueIndex = mock(OIndexUnique.class);
    when(uniqueIndex.getInternal()).thenReturn(uniqueIndex);

    final OIndexDefinition definition = mock(OIndexDefinition.class);
    when(definition.getParamCount()).thenReturn(2);

    when(uniqueIndex.getDefinition()).thenReturn(definition);
    when(uniqueIndex.getType()).thenReturn(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString());

    return uniqueIndex;
  }

  private OIndexUnique mockUniqueCompositeIndexWithNullSupport() {
    final OIndexUnique uniqueIndex = mock(OIndexUnique.class);
    when(uniqueIndex.getInternal()).thenReturn(uniqueIndex);

    final OIndexDefinition definition = mock(OIndexDefinition.class);
    when(definition.getParamCount()).thenReturn(2);

    when(uniqueIndex.getDefinition()).thenReturn(definition);
    when(uniqueIndex.getType()).thenReturn(OClass.INDEX_TYPE.UNIQUE.toString());

    when(uniqueIndex.getMetadata()).thenReturn(new ODocument().field("ignoreNullValues", false));

    return uniqueIndex;
  }

  private OIndexUnique mockUniqueCompositeHashIndexWithNullSupport() {
    final OIndexUnique uniqueIndex = mock(OIndexUnique.class);
    when(uniqueIndex.getInternal()).thenReturn(uniqueIndex);

    final OIndexDefinition definition = mock(OIndexDefinition.class);
    when(definition.getParamCount()).thenReturn(2);

    when(uniqueIndex.getDefinition()).thenReturn(definition);
    when(uniqueIndex.getType()).thenReturn(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString());

    when(uniqueIndex.getMetadata()).thenReturn(new ODocument().field("ignoreNullValues", false));

    return uniqueIndex;
  }

  private OIndexUnique mockUniqueHashIndex() {
    final OIndexUnique uniqueIndex = mock(OIndexUnique.class);
    when(uniqueIndex.getInternal()).thenReturn(uniqueIndex);

    final OIndexDefinition definition = mock(OIndexDefinition.class);
    when(definition.getParamCount()).thenReturn(1);

    when(uniqueIndex.getDefinition()).thenReturn(definition);
    when(uniqueIndex.getType()).thenReturn(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString());

    return uniqueIndex;
  }

  private OIndexFullText mockFullTextIndex() {
    final OIndexFullText uniqueIndex = mock(OIndexFullText.class);
    when(uniqueIndex.getInternal()).thenReturn(uniqueIndex);

    final OIndexDefinition definition = mock(OIndexDefinition.class);
    when(definition.getParamCount()).thenReturn(1);

    when(uniqueIndex.getDefinition()).thenReturn(definition);

    return uniqueIndex;
  }
}
