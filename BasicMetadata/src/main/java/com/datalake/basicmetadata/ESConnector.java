package com.datalake.basicmetadata;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Pegdwende
 */
public class ESConnector {
    
    
   private RestHighLevelClient client;
    
    
    public ESConnector(){
        //Connexion et création du mapping
            client = new RestHighLevelClient(
                    RestClient.builder(
                            //new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 9200, "http")));
            
    }
    
    public  String createMapping(int nbShards, int nbReplicas, String alias){
       String result =  "{\n" +
        "    \"settings\" : {\n" +
        "        \"number_of_shards\" : "+nbShards+",\n" +
        "        \"number_of_replicas\" : "+nbReplicas+"\n" +
        "    },\n" +
        "    \"mappings\" : {\n" +
        "       \"properties\" : {\n" +
        "           \"content\" : { \"type\" : \"text\" , "+
        "                           \"index_options\" : \"offsets\" ,"+ 
        "                           \"index_phrases\" : \"true\", "+
        "                           \"store\" : \"true\"}, \n" +
        "           \"identifier\" : { \"type\" : \"keyword\" }\n" +
        "       }\n" +
        "    },\n" +
        "    \"aliases\" : {\n" +
        "        \""+alias+"\" : {}\n" +
        "    }\n" +
        "}";
       return result;
                
}
    
     public void createIndex(String indexName, String mapping){
       try {
           CreateIndexRequest request = new CreateIndexRequest(indexName);
           request.settings(Settings.builder()
                   .put("index.number_of_shards", 3)
                   .put("index.number_of_replicas", 2)
           );
           request.source(mapping, XContentType.JSON);
           request.setTimeout(TimeValue.timeValueMinutes(2));
           CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
       } catch (IOException ex) {
           Logger.getLogger(ESConnector.class.getName()).log(Level.SEVERE, null, ex);
       }

    }
     
     
    public void deleteIndex(String indexName){
       try {
           
           GetIndexRequest existRequest = new GetIndexRequest(indexName);
            boolean exists = client.indices().exists(existRequest, RequestOptions.DEFAULT);
           
         if(exists){
       
           DeleteIndexRequest request = new DeleteIndexRequest(indexName);
           request.timeout(TimeValue.timeValueMinutes(2)); 
           //request.
           AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
           System.out.println(deleteIndexResponse);
              
        }
            
       } catch (IOException ex) {
           Logger.getLogger(ESConnector.class.getName()).log(Level.SEVERE, null, ex);
       }
    }
    
    public Map<String, Object> docProfileToMap(DocumentProfile docProfile){
        Map<String, Object> result = new HashMap<>();
        result.put("identifier", docProfile.getId());
        result.put("content", docProfile.getFileContent());
        return result;
    }
    
    public void addDocument(Map<String, Object> document, String indexName){
        IndexRequest indexRequest = new IndexRequest(indexName).source(document);
        indexRequest.timeout(TimeValue.timeValueSeconds(30));
       
        //indexRequest
       try {
           IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
           System.out.println(indexResponse);
       } catch (IOException ex) {
           Logger.getLogger(ESConnector.class.getName()).log(Level.SEVERE, null, ex);
       }
    }
    
    
    public void close(){
       try {
           client.close();
       } catch (IOException ex) {
           Logger.getLogger(ESConnector.class.getName()).log(Level.SEVERE, null, ex);
       }
    }

}
