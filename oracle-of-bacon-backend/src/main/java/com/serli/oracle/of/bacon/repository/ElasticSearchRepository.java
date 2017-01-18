package com.serli.oracle.of.bacon.repository;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringEscapeUtils;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;

public class ElasticSearchRepository {

	public static final String INDEX_NAME = "actors";
	public static final String TYPE_NAME = "actor";
	public static final String SUGGEST_FIELD = "suggestion";

	private final JestClient jestClient;

	public ElasticSearchRepository() {
		jestClient = createClient();

	}

	public static JestClient createClient() {
		JestClient jestClient;
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(
				new HttpClientConfig.Builder("http://localhost:9200").multiThreaded(true).readTimeout(60000).build());
		jestClient = factory.getObject();
		return jestClient;
	}

	@SuppressWarnings("unchecked")
	public List<String> getActorsSuggests(String searchQuery) {
		try {
			String querySuggest = "{ \"suggest\" : {"//
					+ "\"text\" : \"" + StringEscapeUtils.ESCAPE_JSON.translate(searchQuery) + "\" , "//
					+ "\"completion\" : { \"field\" : \"" + SUGGEST_FIELD + "\" } "//
					+ "} } ";
			Suggest suggest = new Suggest.Builder(querySuggest).addIndex(INDEX_NAME).build();
			SuggestResult result = jestClient.execute(suggest);
			return result.getSuggestions("suggest").stream()//
					.flatMap(s -> s.options.stream())//
					.map(s -> (Map<String, Object>) s.get("_source"))//
					.map(s -> s.get("name").toString())//
					.distinct()//
					.collect(Collectors.toList());
		} catch (IOException e) {
			return Arrays.asList();
		}
	}
}