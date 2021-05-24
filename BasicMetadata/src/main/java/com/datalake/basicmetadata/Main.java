package com.datalake.basicmetadata;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.io.FileNotFoundException;
import org.bson.Document;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpHost;

import org.apache.tika.exception.TikaException;
import org.bson.conversions.Bson;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.xml.sax.SAXException;

/**
 *
 * @author Pegdwende
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    private static int i = 1, j=0;
    private static final String MONGO_DB_NAME = "aura_pmi_dl"; 
    private static final String MONGO_COLLECTION_NAME = "document_profiles"; 
    private static final String INDEX_ALIAS = "document_index";
    private static final int SHARDS = 50;
    private static final int REPLICAS = 3;
    private static final int INDEX_DISTRIBUTION = 5; 
    private static String pathRacine = "/home/ubuntu/hal/data/";
    //Index deletion
    private static  String indexNameFr = INDEX_ALIAS+"_fr";
    private static  String indexNameEn = INDEX_ALIAS+"_en"; 
     private static Util util = new Util();
      private static   ESConnector elastic = new ESConnector();
       //private static   MongoConnector mongo = new MongoConnector();
    
     public static String compute(String path) throws InterruptedException {
         //METADATA GENERATION
             System.out.println("**************************** "+i+" | "+j+" ******************************");
             System.out.println(path);
            Instant t1 = Instant.now();
            try{
                DocumentProfile docProfile = new DocumentProfile(path, pathRacine);
                Instant t2 = Instant.now(); Duration d = Duration.between(t1, t2);

                System.out.println("~~~~~~~~~~~~~~~~~ Metadata generation: " + d.toMillis()/1000 + " seconds");
                if(docProfile.getFileContent() != null){
                    //INSERTION IN MONGODB
                    //Document mongoDoc = docProfile.toMongoObject();
                    //mongo.addDocument(mongoDoc);
                    Instant t3 = Instant.now(); d = Duration.between(t2, t3);
                    System.out.println("----------------- Insertion in MongoDB: " + d.toMillis()/1000 + " seconds");

                    //Insertion in ELASTICSEARCH
                    Map<String, Object> esDoc = elastic.docProfileToMap(docProfile);
                    //Distribuing data accross french and english indexes
                    if(docProfile.getLanguage().equals("fr")){
                        elastic.addDocument(esDoc, indexNameFr);
                    }else{
                        elastic.addDocument(esDoc, indexNameEn);
                    }
                    docProfile = null;
                    Instant t4 = Instant.now(); d = Duration.between(t3, t4);
                    System.out.println("^^^^^^^^^^^^^^^^^^^^ Insertion in Elasticsearch: " + d.toMillis()/1000 + " seconds");
                    i++;
                }else{
                    System.out.println("A PROBLEM OCCURRED WITH THIS FILE");
                    j++;
                }
            }
            catch(IOException | SAXException | NullPointerException | TikaException ex ){
                System.out.println("A PROBLEM OCCURRED WITH THIS FILE");
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                j++;
            }
            LocalTime localTime = LocalTime.now();
            System.out.println(localTime.toString());
        return localTime.toString();
    }
    
    
    
    
    
    public static void main(String[] args) throws InterruptedException,
        ExecutionException{
        //Initializing
        
        //Getting path of the database
        
         
        List<String> listePath = util.getListFiles(pathRacine);
        
        
        //ElasticSearch
         
         elastic.deleteIndex(indexNameFr);
         elastic.deleteIndex(indexNameEn);
         //elastic.deleteIndex(INDEX_ALIAS);
         
        //Creation of indexes 
        
        String mapping = elastic.createMapping(SHARDS, REPLICAS, INDEX_ALIAS);
        elastic.createIndex(indexNameFr, mapping);
        
        
        mapping = elastic.createMapping(SHARDS, REPLICAS, INDEX_ALIAS);
        elastic.createIndex(indexNameEn, mapping);
        
 
        
        
        //MongoDB
        
        //mongo.initCollection( MONGO_DB_NAME, MONGO_COLLECTION_NAME);
        
        //try {
        
        Instant t0 = Instant.now();
        
        
        //Prepare tasks
        List<Callable<String>> tasks = new ArrayList<Callable<String>>();
        for (final String path : listePath) {
            Callable<String> c = new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return compute(path);
                }
            };
            tasks.add(c);
        }
        
        
        
        
        
        
        
        
        ExecutorService exec = Executors.newFixedThreadPool(6);
        // some other exectuors you could try to see the different behaviours
        // ExecutorService exec = Executors.newFixedThreadPool(3);
        // ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            long start = System.currentTimeMillis();
            List<Future<String>> results = exec.invokeAll(tasks);
            int sum = 0;
            
            long elapsed = System.currentTimeMillis() - start;
            System.out.println(String.format("Elapsed time: %d ms", elapsed));
            System.out.println(String.format("... but compute tasks waited for total of %d ms; speed-up of %.2fx", sum, sum / (elapsed * 1d)));
        } finally {
            exec.shutdown();
        }
        
   
        
        //Closing connexions
        //mongo.close();
        elastic.close();
        Instant tz = Instant.now();Duration d = Duration.between(tz, t0);
        System.out.println("##################  GENERATION TIME: " + d.toMillis()/1000 + " seconds");
        System.out.println("ERRORS COUNT: " + j );
        //} catch (IOException | SAXException | TikaException ex) {
        //Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        //}
        
         
    }

}
