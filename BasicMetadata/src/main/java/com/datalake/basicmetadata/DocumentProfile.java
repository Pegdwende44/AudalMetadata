package com.datalake.basicmetadata;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import static opennlp.tools.cmdline.SystemInputStreamFactory.encoding;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.language.ProfilingHandler;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageHandler;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.bson.Document;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 *
 * @author Pegdwende
 */
public class DocumentProfile {
    private String id;
    private String path;
    private String title;
    private String author;
    private String language;
    private boolean scanned;
    private String mimeType;
    private String application;
    private Date creationDate;
    private Date lastModificationDate;
    private Date insertionDate;
    private Integer nbPages;
    private String fileContent;
    //private List<String> additionalMetadata;

    public DocumentProfile(String pathToDocument, String rootPath) throws FileNotFoundException, IOException, SAXException, TikaException {
        Util util = new Util();
        TikaConfig tikaConfig = new TikaConfig("tikaConfig.xml");
        Detector detector = tikaConfig.getDetector();
        Parser autoDetectParser = new AutoDetectParser(tikaConfig);
         
        //Parser
        Parser parser = new AutoDetectParser();
          
        //Handler
        BodyContentHandler handler = new BodyContentHandler(-1);
        //ContentHandler  handler = new BodyContentHandler(100 * 1024 * 1024);
        
        //Context
        ParseContext context = new ParseContext();
        TesseractOCRConfig config = new TesseractOCRConfig();
        PDFParserConfig pdfConfig = new PDFParserConfig();
        pdfConfig.setExtractInlineImages(true);
        //
    
        //
         
        context = new ParseContext();//Redefinition du context
        context.set(TesseractOCRConfig.class, config);
        context.set(PDFParserConfig.class, pdfConfig);
        
        //Object Metadata
        Metadata metadata = new Metadata();

        //Loading the file       
        File file = new File(pathToDocument);
        FileInputStream inputstream = new FileInputStream(file);
        
        
      


        //Traitement
        parser.parse(inputstream, handler, metadata, context);
        inputstream.close();
        
        //Check if the file is scanned  
        List<String> metadataEntries = Arrays.asList(metadata.names());
        if (metadataEntries.contains("pdf:charsPerPage")){
             String tempSanned = metadata.get("pdf:charsPerPage"); 
            if(!tempSanned.isEmpty() && !tempSanned.equals("")){
                this.scanned = tempSanned.equals("0");
            }
            else{
               this.scanned = false; 
            }
        }
        //Check if certain characters mapping failed
        boolean mappingFailed = false;
        if (metadataEntries.contains("pdf:unmappedUnicodeCharsPerPage")){
             String tempFailed = metadata.get("pdf:unmappedUnicodeCharsPerPage"); 
            if(!tempFailed.isEmpty() && !tempFailed.equals("")){
                mappingFailed = (new Integer(tempFailed) > 0);
            }
            
        }
        
        
        
        
        
        String content = "";
        if (scanned || mappingFailed){
            OcrTool ocr = new OcrTool();
            ocr.generateImageFiles(file);
            content = ocr.extractText();
        }else{
             content = handler.toString();
            
        }
        this.fileContent = content;
        
        //System.out.println(content);
        
        //Detect the language
        LanguageIdentifier lgIdentifier = new LanguageIdentifier(content);
        String tempLanguage = lgIdentifier.getLanguage();
        this.language = tempLanguage; // metadata.get(TikaCoreProperties.LANGUAGE);//;


        //Creation of the identifier
        String tempId = Long.toHexString(System.currentTimeMillis());
         this.id = tempId;
        
        //Getting the Path
        this.path = file.getAbsolutePath();

        //Getting the file Title
        this.title = file.getName();

        //Getting the file Author
        String tempAuthor = metadata.get(TikaCoreProperties.CREATOR);
        if ( tempAuthor != null && !tempAuthor.equals("")){
            this.author = tempAuthor;
        }
        
        //Getting the file MIME type
        Tika tika = new Tika();
        String tempMimeType = tika.detect(file);
        if (!tempMimeType.equals(null) && !tempMimeType.equals("")){
            this.mimeType = tempMimeType;
        }
        
        //Detecting the application used to generate the PDF
        String tempApplication = metadata.get("producer");
        if(tempApplication != null && !tempApplication.equals("")){
            this.application = metadata.get(TikaCoreProperties.CREATOR_TOOL);
        }else{
            tempApplication = metadata.get("xmp:CreatorTool");
            if(tempApplication != null && !tempApplication.equals("")){
                this.application = tempApplication;
            }
        }


        //Getting the file date of creation
        String tempCreationDate = metadata.get(TikaCoreProperties.CREATED);
        if (tempCreationDate != null) {
            this.creationDate = util.dateFromString(tempCreationDate);
        } else {
            tempCreationDate = metadata.get("date");
            if (tempCreationDate != null) {
                this.creationDate = util.dateFromString(tempCreationDate);
            }
        }

        //Getting the file date of last modification
        String tempLastModificationDate = metadata.get(TikaCoreProperties.MODIFIED);

        if (tempLastModificationDate != null) {
            this.lastModificationDate = util.dateFromString(tempLastModificationDate);
        }else{
            this.lastModificationDate = this.creationDate;
        }

        //Getting the file number of pages
        String tempNbPages = metadata.get("xmpTPg:NPages");  
        if (!tempNbPages.equals(null) && !tempNbPages.equals("")) {
            this.nbPages = new Integer(tempNbPages);
        }

        //Generating the insertion time
        this.insertionDate = new Date();
        
        //Detecting metadata from the filepath
       /* File tempPath = file.getParentFile();
        File rootFile = new File(rootPath);
        int i = 0;
        additionalMetadata = new ArrayList<>();
        while(!tempPath.getAbsolutePath().equals(rootFile.getAbsolutePath())){
            additionalMetadata.add(tempPath.getName());
            tempPath = tempPath.getParentFile();
        }
      */
        
    }

    public String getLanguage() {
        return language;
    }

    

    
    public Document toMongoObject(){
        Document document = new Document("_id", id);
        
        
        //Document properties = new Document();
        document.put("path", path);
        document.put("scanned", scanned);
        document.put("insertionDate", insertionDate);
        document.put("title", title);
        if(language != null){
            document.put("language", language);
        }
        if(creationDate != null){
            document.put("creationDate", creationDate);
        }
        if(lastModificationDate != null){
            document.put("lastModificationDate", lastModificationDate);
        }
        if(mimeType != null){
            document.put("mimeType", mimeType);
        }
        if(author != null){
            document.put("author", author);
        }
        if(application != null){
            document.put("application", application);
        }
        if(nbPages != null){
            document.put("nbPages", nbPages);
        }
        //document.put("properties", properties);
        //Tags from the filePath
        /*String[] pathTags = { "DocumentCategory", "Enterprise"};
        if(additionalMetadata != null){
            for(int i = 0; i < additionalMetadata.size(); i++){
                document.put(pathTags[i], additionalMetadata.get(i));
            }
        }*/
        //System.out.println(language);
        return document;
    }

    public String getFileContent() {
        return fileContent;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getId() {
        return id;
    }
    
    
    
    
    
    
    
    @Override
    public String toString() {
        return "Document{" + "id=" + id + ", \n path=" + path + ",\n title=" + title  + ",\n author=" + author + ",\n language=" + language + ",\n mimeType=" + mimeType + ",\n application=" + application + ",\n creationDate=" + creationDate + ",\n lastModificationDate=" + lastModificationDate + ",\n nbPages=" + nbPages + '}';
    }

    

}
