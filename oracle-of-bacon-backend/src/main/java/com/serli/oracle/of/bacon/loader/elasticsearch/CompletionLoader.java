package com.serli.oracle.of.bacon.loader.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

public class CompletionLoader {
	private static final String INDEX_NAME = "actors";
	private static final String TYPE_NAME = "actor";

	private static AtomicInteger count = new AtomicInteger(0);

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("Expecting 1 arguments, actual : " + args.length);
			System.err.println("Usage : completion-loader <actors file path>");
			System.exit(-1);
		}

		String inputFilePath = args[0];
		JestClient client = ElasticSearchRepository.createClient();

		try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
			Bulk.Builder builder = new Bulk.Builder();
			bufferedReader.lines().skip(1)// Skip header
			/*.limit(100000)*/.forEach(actorName -> {
						builder.addAction(new Index.Builder(//
								new StringBuilder("{ \"name\": ")//
										.append(actorName)//Actor already have "
										.append(" }").toString()//
						).index(INDEX_NAME).type(TYPE_NAME).build());
						count.incrementAndGet();
					});// Add to bulk action
			// Execute bulk
			Bulk bulk = builder.build();
			System.out.println("Bulk created");
			client.execute(bulk);
		}
		System.out.println("Inserted total of " + count.get() + " actors");
	}
}
