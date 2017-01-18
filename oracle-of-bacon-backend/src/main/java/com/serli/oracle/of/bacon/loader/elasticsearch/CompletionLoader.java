package com.serli.oracle.of.bacon.loader.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;

public class CompletionLoader {
	private static final Logger LOGGER = LoggerFactory.getLogger(CompletionLoader.class);

	private static final List<String> STR_TO_REMOVE = Arrays.asList("(I)", "(II)", ",", "'");

	private static int BULK_SIZE = 100000;

	private static AtomicInteger count = new AtomicInteger(0);

	private static JestClient client;

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("Expecting 1 arguments, actual : " + args.length);
			System.err.println("Usage : completion-loader <actors file path>");
			System.exit(-1);
		}

		String inputFilePath = args[0];
		client = ElasticSearchRepository.createClient();
		createIndex();
		createMapping();
		try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
			Lists.partition(bufferedReader.lines().skip(1).collect(Collectors.toList()), BULK_SIZE).stream()
					.forEach(CompletionLoader::executeBulkIndex);
		}
		LOGGER.info("Inserted total of {} actors", count.get());
	}

	private static void createIndex() throws IOException {
		CreateIndex createIndex = new CreateIndex.Builder(ElasticSearchRepository.INDEX_NAME).build();
		JestResult indexResult = client.execute(createIndex);
		LOGGER.info("Index created : {}", indexResult.getResponseCode());
	}

	private static void createMapping() throws IOException {
		PutMapping putMapping = new PutMapping.Builder(ElasticSearchRepository.INDEX_NAME,
				ElasticSearchRepository.TYPE_NAME,
				"{ \"" + ElasticSearchRepository.TYPE_NAME + "\" :"//
						+ " {" + "\"properties\" : {" //
						+ "\"" + ElasticSearchRepository.SUGGEST_FIELD + "\" : {" + "\"type\" : \"completion\"" + "},"//
						+ "\"name\" : {" + "\"type\": \"string\"" + "} "//
						+ "} } }").build();
		JestResult mappingResult = client.execute(putMapping);
		LOGGER.info("Mapping created : {}", mappingResult.getResponseCode());
	}

	private static void executeBulkIndex(List<String> actorList) {
		Bulk bulkAction = actorList.stream()//
				.map(CompletionLoader::cleanName)//
				.map((actorName) -> new Index.Builder(//
						ImmutableMap.of("name", actorName, //
								ElasticSearchRepository.SUGGEST_FIELD, createSuggestValue(actorName)))//
										.index(ElasticSearchRepository.INDEX_NAME)//
										.type(ElasticSearchRepository.TYPE_NAME).build())//
				.collect(Collectors.collectingAndThen(//
						Collectors.toList(), //
						(actions) -> new Bulk.Builder().addAction(actions).build()));
		try {
			client.execute(bulkAction);
			count.set(count.get() + actorList.size());
			LOGGER.info("Bulk executed (total {} actors indexed)", count.get());
		} catch (IOException e) {
			LOGGER.error("Problem executing bulk action {}", e.getMessage(), e);
		}

	}

	private static Map<String, List<String>> createSuggestValue(String actorName) {
		ArrayList<String> inputList = new ArrayList<>();
		List<String> nameParts = Arrays.stream(cleanNameForSuggest(actorName).split(" ")).collect(Collectors.toList());
		inputList.add(actorName);// Full name
		inputList.addAll(nameParts);// Each parts
		inputList.add(Lists.reverse(nameParts).stream()//
				.collect(Collectors.joining(" "))); // Full name (part reversed)
		return ImmutableMap.of("input", inputList);
	}

	private static String cleanName(String actorName) {
		return StringUtils.removeEnd(StringUtils.removeStart(actorName, "\""), "\"");
	}

	private static String cleanNameForSuggest(String actorName) {
		for (String toRemove : STR_TO_REMOVE) {
			actorName = StringUtils.remove(actorName, toRemove);
		}
		return actorName;
	}
}
