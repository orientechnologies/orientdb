package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.*;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

import java.util.*;

public class OSiteSchema {

  private static Map<Class<?>, EnumSet<?>> schemas  = new HashMap<Class<?>, EnumSet<?>>();

  private static List<Class<?>>            vertices = new ArrayList<Class<?>>();

  private static List<Class<?>>            edges    = new ArrayList<Class<?>>();
  static {
    addVertexClass(Comment.class);
    addVertexClass(Issue.class);
    addVertexClass(Client.class);
    addVertexClass(Repository.class);
    addVertexClass(User.class);
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
  public enum Comment implements OTypeHolder<CommentDTO> {
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
    public ODocument toDoc(CommentDTO doc, OrientBaseGraph graph) {
      return null;
    }

    @Override
    public CommentDTO fromDoc(ODocument doc) {
      return null;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public enum Issue implements OTypeHolder<com.orientechnologies.website.model.schema.dto.Issue> {
    TITLE("title") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    },
    DESCRIPTION("description") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    },
    NUMBER("number") {
      @Override
      public OType getType() {
        return OType.INTEGER;
      }
    },
    STATE("state") {
      @Override
      public OType getType() {
        return OType.STRING;
      }
    },
    LABELS("labels") {
      @Override
      public OType getType() {
        return OType.EMBEDDEDSET;
      }
    },
    ASSIGNEE("assignee") {
      @Override
      public OType getType() {
        return OType.LINK;
      }
    },
    CREATED_AT("createdAt") {
      @Override
      public OType getType() {
        return OType.DATETIME;
      }
    },
    CLOSED_AT("closedAt") {
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
    public ODocument toDoc(com.orientechnologies.website.model.schema.dto.Issue entity, OrientBaseGraph graph) {

      ODocument doc;
      if (entity.getId() == null) {
        doc = new ODocument(entity.getClass().getSimpleName());
      } else {
        doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
      }
      doc.field(DESCRIPTION.toString(), entity.getDescription());
      doc.field(CREATED_AT.toString(), entity.getCreatedAt());
      doc.field(CLOSED_AT.toString(), entity.getClosedAt());
      doc.field(TITLE.toString(), entity.getTitle());
      doc.field(LABELS.toString(), entity.getLabels());
      doc.field(NUMBER.toString(), entity.getNumber());
      doc.field(STATE.toString(), entity.getState());
      doc.field(ASSIGNEE.toString(), (entity.getAssignee() != null ? new ORecordId(entity.getAssignee().getId()) : null));
      return doc;
    }

    @Override
    public com.orientechnologies.website.model.schema.dto.Issue fromDoc(ODocument doc) {
      com.orientechnologies.website.model.schema.dto.Issue issue = new com.orientechnologies.website.model.schema.dto.Issue();
      issue.setId(doc.getIdentity().toString());
      issue.setTitle((String) doc.field(TITLE.toString()));
      issue.setDescription((String) doc.field(DESCRIPTION.toString()));
      issue.setState((String) doc.field(STATE.toString()));
      issue.setClosedAt((Date) doc.field(CLOSED_AT.toString()));
      issue.setCreatedAt((Date) doc.field(CREATED_AT.toString()));
      issue.setLabels(new ArrayList<String>((Collection<? extends String>) doc.field(LABELS.toString())));
      issue.setNumber((Integer) doc.field(NUMBER.toString()));
      issue.setAssignee(User.NAME.fromDoc((ODocument) doc.field(ASSIGNEE.toString())));
      return issue;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public enum Organization implements OTypeHolder<com.orientechnologies.website.model.schema.dto.Organization> {
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
    public ODocument toDoc(com.orientechnologies.website.model.schema.dto.Organization entity, OrientBaseGraph graph) {
      ODocument doc;
      if (entity.getId() == null) {
        doc = new ODocument(entity.getClass().getSimpleName());
      } else {
        doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
      }
      doc.field(NAME.toString(), entity.getName());
      doc.field(CODENAME.toString(), entity.getCodename());
      return doc;
    }

    @Override
    public com.orientechnologies.website.model.schema.dto.Organization fromDoc(ODocument doc) {
      com.orientechnologies.website.model.schema.dto.Organization organization = new com.orientechnologies.website.model.schema.dto.Organization();
      organization.setId(doc.getIdentity().toString());
      organization.setName((String) doc.field(OSiteSchema.Organization.NAME.toString()));
      organization.setCodename((String) doc.field(OSiteSchema.Organization.CODENAME.toString()));
      return organization;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public enum Repository implements OTypeHolder<com.orientechnologies.website.model.schema.dto.Repository> {
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
    },
    DESCRIPTION("description") {
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
    public ODocument toDoc(com.orientechnologies.website.model.schema.dto.Repository entity, OrientBaseGraph graph) {
      ODocument doc = null;
      if (entity.getId() == null) {
        doc = new ODocument(Repository.class.getSimpleName());
      } else {
        doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
      }
      doc.field(CODENAME.toString(), entity.getCodename());
      doc.field(NAME.toString(), entity.getName());
      doc.field(DESCRIPTION.toString(), entity.getDescription());
      return doc;
    }

    @Override
    public com.orientechnologies.website.model.schema.dto.Repository fromDoc(ODocument doc) {
      com.orientechnologies.website.model.schema.dto.Repository repo = new com.orientechnologies.website.model.schema.dto.Repository();
      repo.setCodename((String) doc.field(OSiteSchema.Repository.CODENAME.toString()));
      repo.setDescription((String) doc.field(OSiteSchema.Repository.DESCRIPTION.toString()));
      repo.setId(doc.getIdentity().toString());
      return repo;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public enum Client implements OTypeHolder<ClientDTO> {
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
    public ODocument toDoc(ClientDTO doc, OrientBaseGraph graph) {
      return null;
    }

    @Override
    public ClientDTO fromDoc(ODocument doc) {
      return null;
    }

    @Override
    public String toString() {
      return description;
    }
  }

  public enum User implements OTypeHolder<com.orientechnologies.website.model.schema.dto.User> {
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

    @Override
    public com.orientechnologies.website.model.schema.dto.User fromDoc(ODocument doc) {
      com.orientechnologies.website.model.schema.dto.User user = new com.orientechnologies.website.model.schema.dto.User();
      user.setEmail((String) doc.field(OSiteSchema.User.EMAIL.toString()));
      user.setId(doc.getIdentity().toString());
      user.setLogin((String) doc.field(OSiteSchema.User.USERNAME.toString()));
      user.setToken((String) doc.field(OSiteSchema.User.TOKEN.toString()));
      return user;
    }

    @Override
    public ODocument toDoc(com.orientechnologies.website.model.schema.dto.User entity, OrientBaseGraph graph) {
      ODocument doc = null;
      if (entity.getId() == null) {
        doc = new ODocument(User.class.getSimpleName());
      } else {
        doc = graph.getRawGraph().load(new ORecordId(entity.getId()));
      }
      doc.field(OSiteSchema.User.USERNAME.toString(), entity.getLogin());
      doc.field(OSiteSchema.User.TOKEN.toString(), entity.getToken());
      doc.field(OSiteSchema.User.EMAIL.toString(), entity.getEmail());
      return doc;
    }

    private final String description;

    User(String description) {
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

  public interface OTypeHolder<T> {
    public OType getType();

    public T fromDoc(ODocument doc);

    public ODocument toDoc(T doc, OrientBaseGraph graph);
  }
}
