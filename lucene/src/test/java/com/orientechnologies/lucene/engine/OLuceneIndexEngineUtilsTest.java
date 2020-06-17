package com.orientechnologies.lucene.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.search.SortField;
import org.junit.Test;

public class OLuceneIndexEngineUtilsTest {

  @Test
  public void buildSortFields() throws Exception {
    ODocument metadata =
        new ODocument()
            .field(
                "sort",
                Arrays.asList(
                    new ODocument()
                        .field("field", "score")
                        .field("reverse", false)
                        .field("type", "INT")
                        .toMap()));

    final List<SortField> fields = OLuceneIndexEngineUtils.buildSortFields(metadata);

    assertThat(fields).hasSize(1);
    final SortField sortField = fields.get(0);

    assertThat(sortField.getField()).isEqualTo("score");
    assertThat(sortField.getType()).isEqualTo(SortField.Type.INT);
    assertThat(sortField.getReverse()).isFalse();
  }

  @Test
  public void buildIntSortField() throws Exception {

    final ODocument sortConf =
        new ODocument().field("field", "score").field("reverse", true).field("type", "INT");

    final SortField sortField = OLuceneIndexEngineUtils.buildSortField(sortConf);

    assertThat(sortField.getField()).isEqualTo("score");
    assertThat(sortField.getType()).isEqualTo(SortField.Type.INT);
    assertThat(sortField.getReverse()).isTrue();
  }

  @Test
  public void buildDocSortField() throws Exception {

    final ODocument sortConf = new ODocument().field("type", "DOC");

    final SortField sortField = OLuceneIndexEngineUtils.buildSortField(sortConf);

    assertThat(sortField.getField()).isNull();
    assertThat(sortField.getType()).isEqualTo(SortField.Type.DOC);
    assertThat(sortField.getReverse()).isFalse();
  }
}
