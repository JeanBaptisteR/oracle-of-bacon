package com.serli.oracle.of.bacon.repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class Neo4JRepository {
	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4JRepository.class);

	private static final String ACTOR_PARAM = "actorName";
	private static final String SEARCH_REQUEST = "MATCH path=shortestPath(" + //
			"(bacon:Actor {name:\"Bacon, Kevin (I)\"})-[PLAYED_IN*]-(other:Actor {name:{" + ACTOR_PARAM + "}}))" + //
			"RETURN path";

	private final Driver driver;

	public Neo4JRepository() {
		driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "root"));
	}

	public List<GraphData> getConnectionsToKevinBacon(String actorName) {
		Session session = driver.session();
		LOGGER.info("Will launch Neo4J search for actor name {}", actorName);
		try {
			StatementResult result = session.run(SEARCH_REQUEST, Values.parameters(ACTOR_PARAM, actorName));
			List<GraphItem> graphItems = new ArrayList<>();
			if (result.hasNext()) {
				Record record = result.next();
				Path path = record.get("path").asPath();
				// Map nodes
				StreamSupport.stream(path.nodes().spliterator(), false).map((node) -> {
					long id = node.id();
					String value = null;
					String type = null;
					if (node.hasLabel(LABEL_ACTOR)) {
						value = node.get(VALUE_NAME).asString();
						type = LABEL_ACTOR;
					} else if (node.hasLabel(LABEL_MOVIE)) {
						value = node.get(VALUE_TITLE).asString();
						type = LABEL_MOVIE;
					}
					return new GraphNode(id, value, type);
				}).forEach(graphItems::add);
				// Map relationships
				StreamSupport.stream(path.relationships().spliterator(), false).map((relation) -> {
					return new GraphEdge(relation.id(), relation.startNodeId(), relation.endNodeId(), VALUE_PLAYED_IN);
				}).distinct().forEach(graphItems::add);
			}
			LOGGER.debug("Neo4J returned {} graph items", graphItems.size());
			return graphItems.stream().map(GraphData::new).collect(Collectors.toList());
		} catch (Exception e) {
			LOGGER.error("Can't search for actor {}", actorName, e);
			return Arrays.asList();
		}
	}

	private static final String LABEL_MOVIE = "Movie";
	private static final String LABEL_ACTOR = "Actor";
	private static final String VALUE_TITLE = "title";
	private static final String VALUE_NAME = "name";
	private static final String VALUE_PLAYED_IN = "PLAYED_IN";

	private static class GraphData {
		private GraphItem data;

		public GraphData(GraphItem data) {
			this.data = data;
		}
	}

	private static abstract class GraphItem {
		public final long id;

		private GraphItem(long id) {
			this.id = id;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			GraphItem graphItem = (GraphItem) o;

			return id == graphItem.id;
		}

		@Override
		public int hashCode() {
			return (int) (id ^ (id >>> 32));
		}
	}

	private static class GraphNode extends GraphItem {
		public final String type;
		public final String value;

		public GraphNode(long id, String value, String type) {
			super(id);
			this.value = value;
			this.type = type;
		}
	}

	private static class GraphEdge extends GraphItem {
		public final long source;
		public final long target;
		public final String value;

		public GraphEdge(long id, long source, long target, String value) {
			super(id);
			this.source = source;
			this.target = target;
			this.value = value;
		}
	}
}
