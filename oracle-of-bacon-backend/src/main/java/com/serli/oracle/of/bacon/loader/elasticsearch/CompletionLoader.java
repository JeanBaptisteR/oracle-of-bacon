package com.serli.oracle.of.bacon.loader.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

public class CompletionLoader {

	private static int BULK_SIZE = 50000;

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

		try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
			Lists.partition(bufferedReader.lines().skip(1).collect(Collectors.toList()), BULK_SIZE).stream()
					.forEach(CompletionLoader::executeBulkIndex);
		}
		System.out.println("Inserted total of " + count.get() + " actors");
	}

	private static void executeBulkIndex(List<String> actorList) {
		Bulk bulkAction = actorList.stream()//
				.map((actorName) -> new Index.Builder(ImmutableMap.of("name", actorName))//
						.index(ElasticSearchRepository.INDEX_NAME)//
						.type(ElasticSearchRepository.TYPE_NAME).build())//
				.collect(Collectors.collectingAndThen(//
						Collectors.toList(), //
						(actions) -> new Bulk.Builder().addAction(actions).build()));
		try {
			client.execute(bulkAction);
			count.set(count.get() + actorList.size());
			System.out.println("Bulk executed ( total " + count.get() + " actors indexed)");
		} catch (IOException e) {
			System.err.println("Problem executing bulk action " + e.getMessage());
		}

	}
}
