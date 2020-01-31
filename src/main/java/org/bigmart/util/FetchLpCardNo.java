package org.bigmart.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.typesafe.config.Config;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class FetchLpCardNo {
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    public void connect(Config sourceConfig){
        mongoClient = new MongoClient(new MongoClientURI(sourceConfig.getString("mongoconnection_uri")));
        MongoDatabase database = mongoClient.getDatabase(sourceConfig.getString("mongo_db"));
        collection = database.getCollection(sourceConfig.getString("source_collection"));
    }

    public List<String> fetch(){
        // Getting the iterable object
        FindIterable<Document> iterDoc = collection.find().projection(Projections.fields(
                Projections.include("lpcardNo")));

        List<String> lpCardNoList = new ArrayList<>();
        for (Document document : iterDoc) {
            lpCardNoList.addAll(document.getList("lpcardNo", String.class));
        }

//        return "("+ lpCardNoList.stream().collect(Collectors.joining("','", "'", "'")) + ")";
        return lpCardNoList;
    }
}
