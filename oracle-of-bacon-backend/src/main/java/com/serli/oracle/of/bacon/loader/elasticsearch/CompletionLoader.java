package com.serli.oracle.of.bacon.loader.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

public class CompletionLoader {
	private static final String INDEX_NAME = "actors";
	private static final String TYPE_NAME = "actor";
	private static int BULK_SIZE = 100000;

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
			List<String> lines = bufferedReader.lines().skip(1).collect(Collectors.toList());
			for (String actorName : lines) {
				addAction(builder, actorName);
				if (count.getAndIncrement() % BULK_SIZE == 0) {
					executeBulk(client, builder, lines.size());
					builder = new Bulk.Builder();
				}
			}
			executeBulk(client, builder, lines.size());
		}
		System.out.println("Inserted total of " + count.get() + " actors");
	}

	private static void executeBulk(JestClient client, Bulk.Builder builder, int size) throws IOException {
		Bulk bulk = builder.build();
		System.out.println("Bulk created");
		client.execute(bulk);
		System.out.println("Bulk executed (" + count.get() + "/" + size + " lines)");
	}

	private static void addAction(Bulk.Builder builder, String actorName) {
		builder.addAction(new Index.Builder(//
				new StringBuilder("{ \"name\": ").append(actorName).append(" }").toString()//
		).index(INDEX_NAME).type(TYPE_NAME).build());
	}
}
