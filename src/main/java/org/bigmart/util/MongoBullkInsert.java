package org.bigmart.util;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.typesafe.config.Config;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoBullkInsert {
    private Logger log = Logger.getLogger(MongoBullkInsert.class);
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    public void connect(Config sinkConfig){
        MongoCredential creds = MongoCredential.createCredential(
                sinkConfig.getString("mongo_user"),
                sinkConfig.getString("mongo_db"),
                sinkConfig.getString("mongo_pass").toCharArray());

        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();

        optionsBuilder.connectTimeout(120000);
        optionsBuilder.socketTimeout(120000);
        optionsBuilder.serverSelectionTimeout(120000);

        MongoClientOptions options = optionsBuilder.build();

        mongoClient = new MongoClient(
                new ServerAddress(
                        sinkConfig.getString("mongo_host"),
                        Integer.parseInt(sinkConfig.getString("mongo_port"))
                ), creds, options);

//        mongoClient = new MongoClient(new MongoClientURI(sinkConfig.getString("mongoconnection_uri")));

        MongoDatabase database = mongoClient.getDatabase(sinkConfig.getString("mongo_db"));
            collection = database.getCollection(sinkConfig.getString("sink_collection"));
    }

    public void sink(List<Document> records){
        try {
            collection.insertMany(records);
            log.info("Successfully Inserted records...");
        }catch (Exception e){
            log.error("Error while Inserting records...");
            log.error(e.getMessage(), e);
        }
    }
}
