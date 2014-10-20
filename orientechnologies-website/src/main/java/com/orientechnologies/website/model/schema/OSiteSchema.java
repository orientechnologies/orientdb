package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

public class OSiteSchema {

  private static Map<Class<?>, EnumSet<?>> schemas  = new HashMap<Class<?>, EnumSet<?>>();

  private static List<Class<?>>            vertices = new ArrayList<Class<?>>();

  private static List<Class<?>>            edges    = new ArrayList<Class<?>>();
  static {
    addVertexClass(Comment.class);
    addVertexClass(Issue.class);
    addVertexClass(Client.class);
    addVertexClass(Member.class);
    addVertexClass(Repository.class);
    addVertexClass(Developer.class);
    addVertexClass(Organization.class);

    addEdgeClass(HasComment.class);
    addEdgeClass(HasDeveloper.class);
    addEdgeClass(HasIssue.class);
    addEdgeClass(HasMember.class);
    addEdgeClass(HasRepo.class);
    addEdgeClass(HasClient.class);
  }

  public static void addVertexClass(Class cls) {

    vertices.add(cls);
    addSchemaClass(cls);
  }

  public static void addSchemaClass(Class cls) {

    schemas.put(cls, EnumSet.allOf(cls));

  }

  public static void addEdgeClass(Class cls) {
    edges.add(cls);
    addSchemaClass(cls);
  }

  public static void createSchema(ODatabaseDocumentTx db) {

    if (!db.exists()) {
      db.create();
      fillSchema(db);
    }

  }

  public static void fillSchema(ODatabaseDocumentTx db) {
    OSchema schema = db.getMetadata().getSchema();

    OClass v = schema.getClass("V");
    OClass e = schema.getClass("E");

    for (Class<?> clazz : schemas.keySet()) {
      OClass cls = schema.createClass(clazz);
      if (vertices.contains(clazz)) {
        cls.setSuperClass(v);
      } else {
        cls.setSuperClass(e);
      }
      if (OTypeHolder.class.isAssignableFrom(clazz)) {
        for (Enum<?> anEnum : schemas.get(clazz)) {
          OType t = ((OTypeHolder) anEnum).getType();
          cls.createProperty(anEnum.toString(), t);
        }
      }
    }

    OSiteSchemaPopupator.populateData(db);
  }

  // Vertices
  public enum Comment implements OTypeHolder {
    TEXT("text") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    };
    private final String name;

    Comment(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public enum Issue implements OTypeHolder {
    DESCRIPTION("description") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    },
    CREATED_AT("createdAt") {
      @Override
      public OType getType() {
        return OType.DATETIME;
      }
    };
    private final String name;

    Issue(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public enum Organization implements OTypeHolder {
    NAME("name") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    },
    CODENAME("codename") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    };

    private final String name;

    Organization(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public enum Repository implements OTypeHolder {
    NAME("name") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    };
    private final String description;

    Repository(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public enum Member implements OTypeHolder {
    NAME("name") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    },
    CODENAME("codename") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    };

    private final String description;

    Member(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public enum Client implements OTypeHolder {
    NAME("name") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    };

    private final String description;

    Client(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public enum Developer implements OTypeHolder {
    NAME("name") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    },
    USERNAME("username") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    },
    TOKEN("token") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    },
    EMAIL("email") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    };
    private final String description;

    Developer(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  // Edges

  public enum HasMember {
    ;
    private final String description;

    HasMember(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public enum HasIssue {
    ;
    private final String description;

    HasIssue(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public enum HasRepo {
    ;
    private final String description;

    HasRepo(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public enum HasComment {
    ;
    private final String description;

    HasComment(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public enum HasDeveloper {
    ;
    private final String description;

    HasDeveloper(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public enum HasClient {
    ;
    private final String description;

    HasClient(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public interface OTypeHolder {
    public OType getType();
  }
}
