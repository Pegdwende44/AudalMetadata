package com.datalake.basicmetadata;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


//import com.googlecode.mp4parser.util.Path;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author Pegdwende
 */
public class Util {
    public Date dateFromString(String chaine){
        //"3/15/2013 3:01:53 PM -06:00"
               // 2017-07-18T19:50:30Z
             //M/dd/yyyy hh:mm:ss aaa XXX
        DateFormat parseFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); 
        Date dt = null;
        try {
          dt = parseFormat.parse(chaine);
        }catch (ParseException e) {
          e.printStackTrace();
            System.out.println("Erreur de conversion"+chaine);
        } 
        
        return dt;
    }  
        
    
        /* 
             t1 = Instant.now();
            Document doc4 = new Document("D:/AURA-PMI/Data/V3/ENTREPRISES DU NUMERIQUE/ACHETERLOUER/Presse/AP_LEREVENU_072007_ACHETERLOUER.pdf");
            t2 = Instant.now();
            d = Duration.between(t1, t2);
            System.out.println("xxxxxxxxxxxxxxxxxxxx Time taken: "+ d.toMillis() +" milliseconds");
            
        */
      
    
    
    public List<String> getListFiles(String path){
        List<String> listePath = new ArrayList();
        
       
        try (Stream<java.nio.file.Path> walk = Files.walk(Paths.get(path))) {

		listePath = walk.filter(Files::isRegularFile)
				.map(x -> x.toString()).collect(Collectors.toList());

	} catch (IOException e) {
		e.printStackTrace();
	}
        return listePath;
    }
    

/*DateFormat printFormat = new SimpleDateFormat("M/dd/yyyy hh:mm:ss aaa XXX"); 
printFormat.setTimeZone(TimeZone.getTimeZone("GMT-05")); 
String newDateString = printFormat.format(dt);
System.out.println(newDateString);*/
}
