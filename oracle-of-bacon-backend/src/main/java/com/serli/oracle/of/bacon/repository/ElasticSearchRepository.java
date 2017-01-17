package com.serli.oracle.of.bacon.repository;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.gson.JsonObject;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

public class ElasticSearchRepository {

	public static final String INDEX_NAME = "actors";
	public static final String TYPE_NAME = "actor";

	private final JestClient jestClient;

	public ElasticSearchRepository() {
		jestClient = createClient();

	}

	public static JestClient createClient() {
		JestClient jestClient;
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200").multiThreaded(true).readTimeout(60000).build());
		jestClient = factory.getObject();
		return jestClient;
	}

	public List<String> getActorsSuggests(String searchQuery) {
		try {
			String queryTerm = "{ \"query\" : { \"match\": { \"name\" :  { \"query\": \"" + searchQuery + "\" , \"operator\" : \"and\" } } } }";
			Search search = new Search.Builder(queryTerm).addIndex(INDEX_NAME).addType(TYPE_NAME).build();
			SearchResult result = jestClient.execute(search);
			return result.getHits(JsonObject.class).stream().map(json -> json.source.get("name").getAsString()).distinct()
					.collect(Collectors.toList());
		} catch (IOException e) {
			return Arrays.asList();
		}
	}
}