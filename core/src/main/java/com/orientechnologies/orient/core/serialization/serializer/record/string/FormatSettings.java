package com.orientechnologies.orient.core.serialization.serializer.record.string;

public class FormatSettings {
  public boolean includeVer;
  public boolean includeType;
  public boolean includeId;
  public boolean includeClazz;
  public boolean attribSameRow;
  public boolean alwaysFetchEmbeddedDocuments;
  public int indentLevel;
  public String fetchPlan = null;
  public boolean keepTypes = true;
  public boolean dateAsLong = false;
  public boolean prettyPrint = false;

  public FormatSettings(final String iFormat) {
    if (iFormat == null) {
      includeType = true;
      includeVer = true;
      includeId = true;
      includeClazz = true;
      attribSameRow = true;
      indentLevel = 0;
      fetchPlan = "";
      keepTypes = true;
      alwaysFetchEmbeddedDocuments = true;
    } else {
      includeType = false;
      includeVer = false;
      includeId = false;
      includeClazz = false;
      attribSameRow = false;
      alwaysFetchEmbeddedDocuments = false;
      indentLevel = 0;
      keepTypes = false;

      if (iFormat != null && !iFormat.isEmpty()) {
        final String[] format = iFormat.split(",");
        for (String f : format)
          if (f.equals("type")) includeType = true;
          else if (f.equals("rid")) includeId = true;
          else if (f.equals("version")) includeVer = true;
          else if (f.equals("class")) includeClazz = true;
          else if (f.equals("attribSameRow")) attribSameRow = true;
          else if (f.startsWith("indent"))
            indentLevel = Integer.parseInt(f.substring(f.indexOf(':') + 1));
          else if (f.startsWith("fetchPlan")) fetchPlan = f.substring(f.indexOf(':') + 1);
          else if (f.startsWith("keepTypes")) keepTypes = true;
          else if (f.startsWith("alwaysFetchEmbedded")) alwaysFetchEmbeddedDocuments = true;
          else if (f.startsWith("dateAsLong")) dateAsLong = true;
          else if (f.startsWith("prettyPrint")) prettyPrint = true;
          else if (f.startsWith("graph") || f.startsWith("shallow"))
            // SUPPORTED IN OTHER PARTS
            ;
          else throw new IllegalArgumentException("Unrecognized JSON formatting option: " + f);
      }
    }
  }
}
