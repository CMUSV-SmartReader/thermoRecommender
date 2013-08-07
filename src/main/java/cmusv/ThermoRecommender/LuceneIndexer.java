package cmusv.ThermoRecommender;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import org.apache.mahout.clustering.dirichlet.DirichletDriver;
import org.apache.mahout.clustering.dirichlet.models.DistanceMeasureClusterDistribution;
import org.apache.mahout.clustering.dirichlet.models.DistributionDescription;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.driver.MahoutDriver;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.text.LuceneStorageConfiguration;
import org.apache.mahout.text.SequenceFilesFromLuceneStorage;
import org.apache.mahout.utils.vectors.VectorDumper;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
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
               "-wt", "tf",
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
               "-Dmapred.input.dir="+vectorOutput +"/tf-vectors/part-r-00000",
               "-Dmapred.output.dir="+ vectorOutput + "/matrix"
       };
       MahoutDriver.main(arguments);
       
         arguments = new String[]{"cvb", 
               "-i", vectorOutput +"/matrix/matrix",
               "-dict", vectorOutput + "/dictionary.file-0",
               "-o", topicTermDistDir,
               "-dt", docTopicDistDir,
               "--maxIter", "1000",
               "--num_topics", "20"};
       MahoutDriver.main(arguments);
   }
   
   public HashMap<Integer, ArrayList<HashMap<String, Double>>> getTopics() throws Throwable {
       HashMap<Integer, String> docIDMapper = new HashMap<Integer, String>(); 
       Configuration configuration = new Configuration();
       for (Pair<IntWritable,Text> record : new SequenceFileIterable<IntWritable,Text>(new Path(vectorOutput +"/matrix/docIndex"), true, configuration)) {
           docIDMapper.put(record.getFirst().get(), record.getSecond().toString());
           }
       
       HashMap<Integer, ArrayList<HashMap<String, Double>>> docTopicDist = new HashMap<Integer, ArrayList<HashMap<String, Double>>> ();
       for (Pair<IntWritable,VectorWritable> record : new SequenceFileIterable<IntWritable,VectorWritable>(new Path(docTopicDistDir + "/part-m-00000"), true, configuration)) {
           String docID =  docIDMapper.get(record.getFirst().get());
           //topicDist.put(docID, value)
           for(Element t:record.getSecond().get().all()){
              Integer topicID = t.index();
              double relativeness = t.get();
              if(!docTopicDist.containsKey(topicID))  {
                  docTopicDist.put(topicID, new ArrayList<HashMap<String, Double>>() );
              }
              
              HashMap<String, Double> item = new HashMap<String, Double>();
              item.put(docID, relativeness);
              docTopicDist.get(topicID).add(item);
           }
        }
       for(Integer topicID: docTopicDist.keySet()){
           docTopicDist.put(topicID, Lists.newArrayList( Collections2.filter(docTopicDist.get(topicID),new Predicate<HashMap<String, Double>>(){
            @Override
            public boolean apply(HashMap<String, Double> item) {
                String doc1 = item.keySet().iterator().next();
                Double doc1Value = item.get(doc1);
                return (doc1Value >= 0.9);
            }
           })));
            
       }
       return docTopicDist;
   }
   public void createDocumentDistribution () throws Throwable{
        String[] arguments = {"seqdumper",
                "--seqFile", topicTermDistDir + "/part-m-00000"
        };
        MahoutDriver.main(arguments);
        arguments = new String[]{"seqdumper",
                "--seqFile", vectorOutput +"/matrix/docIndex",
                "--output", vectorOutput + "/matrix/docIndexDump"
       
        };
        MahoutDriver.main(arguments);
       
   }
   
    
}
