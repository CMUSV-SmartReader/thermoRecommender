package cmusv.ThermoRecommender;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.driver.MahoutDriver;

import com.mongodb.DBObject;

public class LuceneIndexer {
    private String baseDir = "temp/";
    private String luceneIndexFolder = baseDir + "indexDir";
    private String vectorOutput = baseDir + "mahout.vector";
    private String dictOutput = baseDir + "mahout.dictOutput";
    private String articleDir = baseDir + "articles";
    private String topicTermDistDir = baseDir + "topicTermDist";
    private String docTopicDistDir = baseDir + "docTopicDist";
    private String seqDir = baseDir + "seqDir";
    private String mahoutTempDir = baseDir + "mahout.Temp";
    
    protected IndexWriter indexWriter;  
   
    public LuceneIndexer(){
       SimpleFSDirectory directory;
    try {
        File indexFolder = new File(luceneIndexFolder);
        directory = new SimpleFSDirectory(indexFolder);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, new SmartChineseAnalyzer(Version.LUCENE_36) );
        indexWriter = new IndexWriter(directory, config);
    } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
       
   }
   
   public void addDocument(String docName, String docContent){
       Document doc = new Document();
       doc.add(new Field("name", docName,Field.Store.YES, Field.Index.NOT_ANALYZED));
       doc.add(new Field("content", docContent,Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS ));
       try {
        indexWriter.addDocument(doc);
    } catch (CorruptIndexException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
       
   }
   public void createIndexing(){
       try {
        indexWriter.close();
    } catch (CorruptIndexException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
   }
   public void mongoToLucene(MongoAdapter dbAdapter) throws Throwable{
       File tempFolder = new File(articleDir);
       for (File file : tempFolder.listFiles()) {
           file.delete();
       }   
       for(DBObject article:dbAdapter.getArticles(DateUtils.getDaysAgo(5))){
           
           if(article.get("desc") == null || article.get("_id") == null) continue;
           
           addDocument(article.get("_id").toString(), article.get("desc").toString());
           
           File file = new File(articleDir + "/"+ article.get("_id"));
           try {
               file.createNewFile();
               FileWriter fw = new FileWriter(file.getAbsoluteFile());
               BufferedWriter bw = new BufferedWriter(fw);
               bw.write(article.get("desc").toString());
               bw.close();
           } catch (IOException e) {
               e.printStackTrace();
           }
       }
       createIndexing();
       luceneToMahout();
   }
   public void luceneToMahout() throws Throwable{
       String[] arguments = {"lucene.vector", 
               "--dir", luceneIndexFolder, 
               "--output",vectorOutput, 
               "--dictOut", dictOutput, 
               "-f", "content", 
               "-i", "name", 
               "-w", "TF"};
       MahoutDriver.main(arguments);
   }
   public void runLDA() throws Throwable{
       int numTopics;
       int numTerms; 
       
       //double eta = 1.0/numTopics ;
       //double alpha = 1.0/numTopics;
       //String[] dictionary;
       //double modelWeight; 
       //TopicModel topicModel = new TopicModel( numTopics,  numTerms, eta,  alpha, dictionary, modelWeight) 
   }
    
}
