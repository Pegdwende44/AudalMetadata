package com.datalake.basicmetadata;


import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sourceforge.tess4j.TesseractException;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Pegdwende
 */
public class OcrTool {

    private net.sourceforge.tess4j.Tesseract instance;
    private List<String> generatedFiles;

    //Constructor
    public OcrTool() {
        //Initialisation
        instance = new net.sourceforge.tess4j.Tesseract();
        //Defining Tesseract data folder
        instance.setDatapath("/usr/share/tesseract-ocr/4.00/tessdata");
        //default Language
        instance.setLanguage("fra");
        generatedFiles = new ArrayList();
    }

    //Transform a scan PDF into a set of images
    public void generateImageFiles(File sourceFile) {
        String destinationDir = sourceFile.getParent();
        PDDocument document;
        try {
            document = PDDocument.load(sourceFile);
            PDFRenderer pdfRenderer = new PDFRenderer(document);
            int nbPages = document.getNumberOfPages();
            generatedFiles = new ArrayList();
            for (int page = 0; page < nbPages; ++page) {
                BufferedImage bim = pdfRenderer.renderImageWithDPI(page, 300, ImageType.RGB);
                String fileName = destinationDir + "image-" + page + ".png";
                ImageIOUtil.writeImage(bim, fileName, 300);
                generatedFiles.add(fileName);

                
            }
            document.close();
        } catch (IOException ex) {
            Logger.getLogger(OcrTool.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
    //Extract text from image file
    public String extractText(){
        String result = "";
        for(String imgFilename : generatedFiles){
            File imgFile = new File(imgFilename);
            String pageText;
            try {
                pageText = instance.doOCR(imgFile);
                result = result + "\n"+ pageText;
            //System.out.println("**********************"+result);
            //Deleting img file
            imgFile.delete();
            } catch (TesseractException ex) {
                Logger.getLogger(OcrTool.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
        return result;
    }

}
