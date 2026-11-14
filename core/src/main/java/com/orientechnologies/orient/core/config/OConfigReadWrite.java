package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.core.exc.StreamWriteException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import java.io.IOException;
import java.nio.file.Path;

public class OConfigReadWrite {

  public OConfig readJSON(Path file) throws IOException {
    return read(file, new ObjectMapper());
  }

  public void writeJSON(Path file, OConfig config) throws IOException {
    write(file, config, new ObjectMapper());
  }

  public OConfig readYML(Path file) throws IOException {
    return read(file, new YAMLMapper());
  }

  public void writeYML(Path file, OConfig config) throws IOException {
    write(file, config, new YAMLMapper());
  }

  protected OConfig read(Path file, ObjectMapper mapper) throws IOException {
    var reader = mapper.readerFor(OConfig.class);
    return reader.readValue(file.toFile());
  }

  public static <T> T read(Class<T> configClass, JsonNode config) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    var reader = mapper.readerFor(configClass);
    return reader.readValue(config);
  }

  protected void write(Path file, OConfig config, ObjectMapper mapper)
      throws IOException, StreamWriteException, DatabindException {
    var writer = mapper.writerFor(OConfig.class);
    writer.writeValue(file.toFile(), config);
  }

  public OConfig readYMLString(String data) throws IOException {
    return readString(data, new YAMLMapper());
  }

  public String writeYMLString(OConfig config) throws IOException {
    return writeString(config, new YAMLMapper());
  }

  protected OConfig readString(String data, ObjectMapper mapper) throws IOException {
    var reader = mapper.readerFor(OConfig.class);
    return reader.readValue(data);
  }

  protected String writeString(OConfig config, ObjectMapper mapper)
      throws IOException, StreamWriteException, DatabindException {
    var writer = mapper.writerFor(OConfig.class);
    return writer.writeValueAsString(config);
  }

  public OConfig readYMLStringVariables(String data) throws IOException {
    return readStringVariables(data, new YAMLMapper());
  }

  private OConfig readStringVariables(String data, ObjectMapper mapper) throws IOException {
    var reader = mapper.reader();
    var tree = reader.readTree(data);
    replaceVariables(tree, mapper);
    var configReader = mapper.readerFor(OConfig.class);
    return configReader.readValue(tree);
  }

  private void replaceVariables(JsonNode tree, ObjectMapper mapper) {
    if (tree.isObject()) {
      for (var property : tree.properties()) {
        if (property.getValue().isTextual()) {
          var value = property.getValue().asText();
          value = replaceVariablesInText(value);
          property.setValue(mapper.getNodeFactory().textNode(value));
        } else {
          replaceVariables(property.getValue(), mapper);
        }
      }

    } else if (tree.isArray()) {
      var values = (ArrayNode) tree;
      for (int i = 0; i < values.size(); i++) {
        var val = values.get(i);
        if (val.isTextual()) {
          var value = val.asText();
          value = replaceVariablesInText(value);
          values.set(i, mapper.getNodeFactory().textNode(value));
        } else {
          replaceVariables(val, mapper);
        }
      }
    }
  }

  private String replaceVariablesInText(String value) {
    return OSystemVariableResolver.resolveSystemVariables(value);
  }
}
