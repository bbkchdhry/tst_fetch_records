package org.bigmart.util;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.bigmart.sink.Launcher;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FetchLpCardNo {
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    private Config sourceConfig;

    public void connect(Config sourceConfig){
        this.sourceConfig = sourceConfig;
        mongoClient = new MongoClient(new MongoClientURI(sourceConfig.getString("mongoconnection_uri")));
        database = mongoClient.getDatabase(sourceConfig.getString("mongo_db"));
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

    public List<String> fetch(String start_date, String end_date){
        System.out.println(start_date);
        System.out.println(end_date);
        // Getting the iterable object
        BasicDBObject gtQuery = new BasicDBObject();
        gtQuery.put("billdate", new BasicDBObject("$gte", start_date).append("$lt", end_date));
        FindIterable<Document> iterDoc = database.getCollection(sourceConfig.getString("sink_collection")).find(gtQuery).projection(Projections.fields(
                Projections.include("lpcardno")));

        List<String> lpCardNoList = new ArrayList<>();
        for (Document document : iterDoc) {
            lpCardNoList.add(document.getString("lpcardno"));
        }

        System.out.println(lpCardNoList.size());
        return lpCardNoList;
    }

    public static void main(String[] args) {
        Config config = ConfigFactory.parseFile(new File("/etc/bigmart_fetchdata/fetch.conf"));
        FetchLpCardNo fetchLpCardNo = new FetchLpCardNo();
        fetchLpCardNo.connect(config);

        fetchLpCardNo.fetch("2020-02-11", "2020-02-12");
//        List<String> lpcardno_fetched = fetchLpCardNo.fetch();
//        List<String> recorded_lpcardno = new ArrayList<>();
//
//        try {
//            BufferedReader bfr = new BufferedReader(new FileReader("/etc/bigmart_fetchdata/live"));
//            String lpcardno;
//            while ((lpcardno = bfr.readLine()) != null) {
//                recorded_lpcardno.add(lpcardno);
//            }
//            bfr.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        if (recorded_lpcardno.size() == 0) {
//            for (List<String> lp_lst : Partition.ofSize(lpcardno_fetched, 500)) {
//                System.out.println("(" + lp_lst.stream().collect(Collectors.joining("','", "'", "'")) + ")");
//            }
//        } else {
//            lpcardno_fetched.removeAll(recorded_lpcardno);
//            if (lpcardno_fetched.size() == 0) {
//                // same value in both recorded and nlpcardno!!!
//                System.out.println("same value in both recorded and nlpcardno!!!");
//                for (List<String> lp_lst : Partition.ofSize(recorded_lpcardno, 500)) {
//                    System.out.println("(" + lp_lst.stream().collect(Collectors.joining("','", "'", "'")) + ")");
//                }
//            } else {
//                // new lpcardno found!!!
//                System.out.println("new lpcardno is found!!!");
//                System.out.println("sending request for old lpcardno!!!");
//
//                for (List<String> lp_lst : Partition.ofSize(recorded_lpcardno, 500)) {
//                    System.out.println("(" + lp_lst.stream().collect(Collectors.joining("','", "'", "'")) + ")");
//                }
//
//                System.out.println("sending request for new lpcardno!!!");
//
//                for (List<String> lp_lst : Partition.ofSize(lpcardno_fetched, 500)) {
//                    System.out.println("(" + lp_lst.stream().collect(Collectors.joining("','", "'", "'")) + ")");
//                }
//            }
//        }

    }

}
