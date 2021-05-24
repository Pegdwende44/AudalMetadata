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
import org.bson.Document;


/**
 *
 * @author Pegdwende
 */
public class MongoConnector {
    
    private MongoClient mongoClient;
    MongoCollection collection;
    //Constructeur par défaut    
    public MongoConnector(){
        this.mongoClient = new MongoClient("localhost", 27017);
    }
      
    
    //Constructeur avec paramètres personnalisés
    public MongoConnector(String host, int port){
                this.mongoClient = new MongoClient(host, port);
    }
    
    
    public void initCollection(String dbName, String collectionName){
        MongoDatabase database = mongoClient.getDatabase(dbName);
        //Selection/Création d'une collection
        collection = database.getCollection(collectionName);
        collection.deleteMany(new Document());//Deleting former documents
    }
    
    
    public void addDocument(Document doc){
        collection.insertOne(doc);
    }
    
    public void close(){
        mongoClient.close();
    }
    
}
