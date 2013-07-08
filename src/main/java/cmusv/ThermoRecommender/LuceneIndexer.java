package cmusv.ThermoRecommender;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

public class LuceneIndexer {
    protected IndexWriter indexWriter;  
   public LuceneIndexer(){
       SimpleFSDirectory directory;
    try {
        File indexFolder = new File("temp/indexDir");
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
       doc.add(new Field("name", docName,Field.Store.YES, Field.Index.ANALYZED));
       doc.add(new Field("content", docContent,Field.Store.YES, Field.Index.ANALYZED));
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
    
}
