package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 3/11/14
 */
public class OrientDBLockExceptionTest {
	private static String className = "Employee";
	private static String keyName = "fName";

	public void runTest() {
		OrientBaseGraph graph = new OrientGraph("plocal:./testdb");
		OClass clazz = graph.getVertexType(className);
		if (clazz == null) {
			graph.createVertexType(className);
			graph.createEdgeType("Connected");
			graph.createKeyIndex(keyName, Vertex.class, new Parameter("class", className));
		}

		Set<String> keys = graph.getIndexedKeys(Vertex.class, true);
		System.out.println("Keys = " + keys);
		graph.shutdown();

		List<Future<Void>> futureList = new ArrayList<Future<Void>>();
		ExecutorService executorService = Executors.newFixedThreadPool(10);
		for (int i = 0; i < 2; i++) {
			Future<Void> future = executorService.submit(new WorkerThread(i));
			futureList.add(future);
		}
		for (Future<Void> f : futureList) {
			try {
				f.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		graph = new OrientGraph("plocal:./testdb");
		System.out.println("Adding links");
		for (int i = 0; i < 9; i++) {

			Iterable<Vertex> srcNodes = graph.getVertices("Employee.fName", "name-0-" + i);
			Iterable<Vertex> srcNodes1 = graph.getVertices("Employee.fName", "name-1-" + i);
			for (Vertex source : srcNodes) {
				for (Vertex destination : srcNodes1) {
					System.out.println("*********** Adding edges....");
					OrientEdge edge = (OrientEdge) ((OrientVertex)source).addEdge("Connected", destination);
					edge.setProperty("test", "test");
					graph.commit();
				}
			}
		}
		graph.shutdown();

		executorService.shutdown();
		System.out.println("Processing done");
	}

	public static void main(String[] args) {
		OrientDBLockExceptionTest test = new OrientDBLockExceptionTest();
		test.runTest();
	}

	public static class WorkerThread implements Callable<Void> {

		private int count;

		public WorkerThread(int count) {
			this.count = count;
		}

		@Override
		public Void call() throws Exception {
			OrientBaseGraph graph = new OrientGraph("plocal:./testdb");
			for (int i = 0; i < 11; i++) {
				Vertex vtx = graph.addVertex("class:"+className);
				vtx.setProperty(keyName, "name-" + this.count + "-" + i);
				vtx.setProperty("lname", "lname" + i);
				graph.commit();
			}
			graph.shutdown();
			System.out.println("Thread done");
			return null;
		}
	}
}