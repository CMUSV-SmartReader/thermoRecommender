package cmusv.ThermoRecommender;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.apache.mahout.driver.MahoutDriver;
import org.apache.mahout.text.LuceneStorageConfiguration;
import org.apache.mahout.text.SequenceFilesFromLuceneStorage;
import org.apache.mahout.utils.vectors.VectorDumper;

import com.mongodb.DBObject;

import static java.util.Arrays.asList;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
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
    
     
    protected IndexWriterConfig config;
    protected FSDirectory directory;
    protected IndexWriter indexWriter;
    public LuceneIndexer(){
       
    try {
        File indexFolder = new File(luceneIndexFolder);
        directory = FSDirectory.open(indexFolder);
        
        config = new IndexWriterConfig(Version.LUCENE_43, new SmartChineseAnalyzer(Version.LUCENE_43) );
         indexWriter = new IndexWriter(directory, config);
    } catch (IOException e) {
        
        e.printStackTrace();
    }
       
   }
   
   public void addDocument(String docName, String docContent) throws CorruptIndexException, LockObtainFailedException, IOException{
       
       Document doc = new Document();
       @SuppressWarnings("deprecation")
    Field idField = new Field("name", docName,Field.Store.YES, Field.Index.NOT_ANALYZED);
       @SuppressWarnings("deprecation")
    Field field = new Field("content", docContent,Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS );

       doc.add(idField);
       doc.add(field);
 
       try {
        indexWriter.addDocument(doc);
    } catch (CorruptIndexException e) {
       
        e.printStackTrace();
    } catch (IOException e) {
      
        e.printStackTrace();
    }
       
   }
   public void createIndexing(){
       try {
        indexWriter.commit();
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
       
   }
   public void sequenceToVector() throws Throwable{
       
       String[] arguments = {"seq2sparse", 
               "-i", seqDir + "/" + "indexDir", 
               "-wt", "tfidf",
               "-a", "org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer",
               "--output",vectorOutput };
       MahoutDriver.main(arguments);
       
   }
   public void luceneToSequence() throws IOException {
       Configuration configuration = new Configuration();
       SequenceFilesFromLuceneStorage lucene2Seq = new SequenceFilesFromLuceneStorage();
       LuceneStorageConfiguration lucene2SeqConf = new LuceneStorageConfiguration(configuration, asList(new Path(luceneIndexFolder)), new Path(seqDir), "name", asList("content"));
       lucene2Seq.run(lucene2SeqConf);
       
   }
   public void runLDA() throws Throwable{
       String[] arguments = {"rowid",
               "-Dmapred.input.dir="+vectorOutput +"/tfidf-vectors/part-r-00000",
               "-Dmapred.output.dir="+ vectorOutput + "/matrix"
       };
       MahoutDriver.main(arguments);
       arguments = new String[]{"cvb", 
               "-i", vectorOutput +"/matrix/matrix",
               "-dict", vectorOutput + "/dictionary.file-0",
               "-o", topicTermDistDir,
               "-dt", docTopicDistDir,
               "--maxIter", "10",
               "--num_topics", "20"};
       MahoutDriver.main(arguments);
   }
   public void getTopics() throws Throwable {
       String[] arguments = {"vectordump",
               "-s", topicTermDistDir,
               "--dictionary",  vectorOutput + "/dictionary.file-0",
    "--dictionaryType", "sequencefile"
       };
       MahoutDriver.main(arguments);
   }
    
}
