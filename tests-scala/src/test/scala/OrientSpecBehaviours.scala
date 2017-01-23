import java.util.{ArrayList => JArrayList}

import com.orientechnologies.orient.core.sql.executor.OResultSet
import gremlin.scala._
import org.apache.tinkerpop.gremlin.orientdb._
import org.scalatest.{BeforeAndAfterEach, ShouldMatchers, WordSpec}

import scala.collection.JavaConversions._

abstract class OrientSpecBehaviours extends WordSpec with ShouldMatchers with BeforeAndAfterEach {

  val graph: ScalaGraph

  val testLabel = "testLabel"
  val testProperty = Key[String]("testProperty")
  
  val vertexLabel1 = "vLabel1"
  val vertexLabel2 = "vLabel2"
  val edgeLabel1 = "eLabel1"
  val edgeLabel2 = "eLabel2"
    
  override def beforeEach(): Unit = {
    graph.E.toList.foreach(_.remove())
    graph.V.toList.foreach(_.remove())
  }

  "vertices" should {
    "be found if they exist" in {
      val v1 = graph.addVertex()
      val v2 = graph.addVertex()
      val v3 = graph.addVertex()

      graph.V(v1.id, v3.id).toList should have length 2
      graph.V().toList should have length 3
    }

    "not be found if they don't exist" in {
      val list = graph.V("#3:999").toList
      list should have length 0
    }

    "set property after creation" in {
      val v = graph + (testLabel, testProperty -> "testValue1")

      // v.property(testProperty).value shouldBe "testValue1"
      graph.V(v.id).value(testProperty).head shouldBe "testValue1"
    }

    "set property during creation" in {
      val property1 = "key1" → "value1"
      val property2 = "key2" → "value2"
      val v = graph.addVertex(Map(property1, property2))
      graph.V(v.id).values[String]("key1", "key2").toList shouldBe List("value1", "value2")
    }

    "delete property" in {
      val v = graph.addVertex()
      val key = "testProperty"
      v.setProperty(testProperty, "testValue1")
      v.property(testProperty).value shouldBe "testValue1"
      v.removeProperty(testProperty)
      intercept[IllegalStateException] {
        v.property(testProperty).value
      }
    }

    "use labels" in {
      val v1 = graph.addVertex(vertexLabel1)
      val v2 = graph.addVertex(vertexLabel2)
      val v3 = graph.addVertex()

      val labels = graph.V.label.toSet
      // labels should have size 2
      labels should contain(vertexLabel1)
      labels should contain(vertexLabel2)
    }

    "delete" in {
      val v1 = graph.addVertex()
      val v2 = graph.addVertex()
      graph.V().toList.size shouldBe 2
      graph.V().toList.foreach(_.remove())
      graph.V().toList.size shouldBe 0
    }
  }

  "edges" should {
    "be found if they exist" in {
      val v1 = graph.addVertex()
      val v2 = graph.addVertex()
      val e1 = v1.addEdge(edgeLabel1, v2)
      val e2 = v2.addEdge(edgeLabel2, v1)

      graph.E(e2.id).toList should have length 1
      graph.E().toList should have length 2
    }

    "not be found if they don't exist" in {
      val list = graph.E("#3:999").toList
      list should have length 0
    }

    "set property after creation" in {
      val v1 = graph.addVertex()
      val v2 = graph.addVertex()
      val e = v1.addEdge(edgeLabel1, v2)

      e.setProperty(testProperty, "testValue1")

      e.property(testProperty).value shouldBe "testValue1"
      graph.E(e.id).value(testProperty).head shouldBe "testValue1"
    }

    "set property during creation" in {
      val v1 = graph.addVertex()
      val v2 = graph.addVertex()

      val e = v1 --- (edgeLabel1, testProperty -> "testValueEdge") --> v2
      graph.E(e.id).value(testProperty).head shouldBe "testValueEdge"
    }

    "delete" in {
      val v1 = graph.addVertex()
      val v2 = graph.addVertex()
      val e1 = v1.addEdge(edgeLabel1, v2)
      val e2 = v2.addEdge(edgeLabel2, v1)

      graph.E(e1.id).toList should have length 1
      graph.E(e1.id).head().remove()
      graph.E(e1.id).toList should have length 0

      v1.outE(edgeLabel1).toList() should have length 0
    }

    "do not delete entry if there are multiple edges" in {
      val v1 = graph.addVertex()
      val v2 = graph.addVertex()
      val e1 = v1.addEdge(edgeLabel1, v2)
      val e2 = v1.addEdge(edgeLabel1, v1)

      graph.E(e1.id).toList should have length 1
      graph.E(e1.id).head().remove()
      graph.E(e1.id).toList should have length 0

      v1.outE(edgeLabel1).toList() should have length 1
    }

    "be removed if vertex is deleted" in {
      val v1 = graph.addVertex()
      val v2 = graph.addVertex()
      val e1 = v1.addEdge(edgeLabel1, v2)
      val e2 = v2.addEdge(edgeLabel2, v1)

      v2.inE(edgeLabel1).toList() should have length 1
      v2.outE(edgeLabel2).toList() should have length 1

      v1.remove();
      v2.inE(edgeLabel1).toList() should have length 0
      v2.outE(edgeLabel2).toList() should have length 0
    }
  }

  "traversals" should {
    "follow outE" in new TinkerpopFixture {
      def traversal = graph.V(marko.id).outE
      traversal.label.toSet shouldBe Set("knows", "created")
      traversal.label.toList should have size 3
    }

    "follow outE for a label" in new TinkerpopFixture {
      def traversal = graph.V(marko.id).outE("knows")
      traversal.label.toSet shouldBe Set("knows")
      traversal.label.toList should have size 2
    }

    "follow inV" in new TinkerpopFixture {
      def traversal = graph.V(marko.id).outE.inV
      traversal.value[String]("name").toSet shouldBe Set("vadas", "josh", "lop")
    }

    "follow out" in new TinkerpopFixture {
      def traversal = graph.V(marko.id).out
      traversal.value[String]("name").toSet shouldBe Set("vadas", "josh", "lop")
    }

    "follow out for a label" in new TinkerpopFixture {
      def traversal = graph.V(marko.id).out("knows")
      traversal.value[String]("name").toSet shouldBe Set("vadas", "josh")
    }

    "follow in" in new TinkerpopFixture {
      def traversal = graph.V(josh.id).in
      traversal.value[String]("name").toSet shouldBe Set("marko")
    }

    "follow inE" in new TinkerpopFixture {
      def traversal = graph.V(josh.id).inE
      traversal.label.toSet shouldBe Set("knows")
    }

    "value" in new TinkerpopFixture {
      def traversal = graph.V(marko.id).out.value[Int]("age") //.filter(_.value[Int]
      traversal.toSet shouldBe Set(27, 32)
    }

    "properties" in new TinkerpopFixture {
      def traversal = graph.V(marko.id).out.properties("age")
      traversal.toSet.map(_.value) shouldBe Set(27, 32)
    }

    "filter" in new TinkerpopFixture {
      def traversal = graph.V(marko.id).out.filter(_.property[Int]("age").orElse(0) > 30)
      traversal.value[String]("name").toSet shouldBe Set("josh")
    }
  }

  "execute arbitrary OrientSQL" in {
    (1 to 20) foreach { _ ⇒
      graph.addVertex()
    }

    val javaGraph = graph.asJava.asInstanceOf[OrientGraph]
    val results: Seq[_] = javaGraph.executeSql("select from V limit 10") match {
      case r: OResultSet   ⇒ r.toSeq
      case other              ⇒ println(other.getClass()); println(other); ???
    }
    results should have length 10
  }

  trait TinkerpopFixture {
    val Person = "person"
    val Software = "software"
    val Knows = "knows"
    val Created = "created"

    val Name = Key[String]("name")
    val Lang = Key[String]("lang")
    val Age = Key[Int]("age")
    val Weight = Key[Double]("weight")

    val marko = graph + (Person, Name -> "marko", Age -> 29)
    val vadas = graph + (Person, Name -> "vadas", Age -> 27)
    val lop = graph + (Software, Name -> "lop", Lang -> "java")
    val josh = graph + (Person, Name -> "josh", Age -> 32)
    val ripple = graph + (Software, Name -> "ripple", Lang -> "java")
    val peter = graph + (Person, Name -> "peter", Age -> 35)

    marko --- (Knows, Weight -> 0.5d) --> vadas
    marko --- (Knows, Weight -> 1.0d) --> josh
    marko --- (Created, Weight -> 0.4d) --> lop
    josh --- (Created, Weight -> 1.0d) --> ripple
    josh --- (Created, Weight -> 0.4d) --> lop
    peter --- (Created, Weight -> 0.2d) --> lop
  }
}

