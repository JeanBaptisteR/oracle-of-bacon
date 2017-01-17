package com.serli.oracle.of.bacon.repository;

import static com.mongodb.client.model.Filters.eq;

import java.util.Optional;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDbRepository {

    private final MongoClient mongoClient;
    private MongoDatabase workshop;

    public MongoDbRepository() {
        mongoClient = new MongoClient("localhost", 27017);
        workshop = mongoClient.getDatabase("workshop");
    }

    public Optional<Document> getActorByName(String name) {
        MongoCollection<Document> actors = workshop.getCollection("actors");
        Document document = actors.find(eq("name", name)).first();
        return document!=null?Optional.of(document):Optional.empty();
    }
}
