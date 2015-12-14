import gremlin.scala._
import org.apache.tinkerpop.gremlin.orientdb._

class OrientSpec extends OrientSpecBehaviours {

  val graph = new OrientGraphFactory(s"memory:test-${math.random}")
    .setupPool(5)
    .getNoTx.asScala
  //  val graph = new OrientGraphFactory("remote:localhost/graphtest", "root", "root").getNoTx().asScala

  "edges" should {

    "be found if they have the same label as vertices" in {
      val label = "label"
      val v1 = graph.addVertex(label)
      val v2 = graph.addVertex(label)
      val e1 = v1.addEdge(label, v2)
      val e2 = v2.addEdge(label, v1)

      graph.E(e2.id).toList should have length 1
      graph.E().toList should have length 2
    }
  }
}

class OrientSpecUnprefixed extends OrientSpecBehaviours {
  
  val graph = new OrientGraphFactory(s"memory:test-${math.random}")
    .setupPool(5)
    .setLabelAsClassName(true)
    .getNoTx.asScala

  override val vertexLabel1 = "vlabel1"
  override val vertexLabel2 = "vlabel2"
  override val edgeLabel1 = "elabel1"
  override val edgeLabel2 = "elabel2"

  "edges" should {

    "fail if they have the same label as vertices" in {
      val label = "label"
      val v1 = graph.addVertex(label)
      val v2 = graph.addVertex(label)
      intercept[IllegalArgumentException] {
        v1.addEdge(label, v2)
      }
    }
  }
}

