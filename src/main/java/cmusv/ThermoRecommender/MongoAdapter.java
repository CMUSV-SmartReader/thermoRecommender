package cmusv.ThermoRecommender;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.bson.types.ObjectId;



import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoAdapter {
    protected String dbHost;
    protected int dbPort;
    protected String dbName;
    protected String dbUser;
    protected String dbPassword;
    protected DB db;
    public MongoAdapter(){
        Map<String, String> env = System.getenv();
        
        this.dbHost = (env.containsKey("MONGO_HOST")) ? env.get("MONGO_HOST") : "test.lydian.tw";
        this.dbPort = (env.containsKey("MONGO_PORT")) ? Integer.parseInt(env.get("MONGO_PORT")) : 27017;
        this.dbName = (env.containsKey("MONGO_DB_NAME")) ? env.get("MONGO_DB_NAME"): "thermoreader-test";
        this.dbUser = (env.containsKey("MONGO_DB_USER")) ? env.get("MONGO_DB_USER") : null;
        this.dbPassword = (env.containsKey("MONGO_DB_PASSWORD")) ? env.get("MONGO_DB_PASSWORD") : null;
        
    }
    public MongoAdapter(String dbHost, int dbPort, String dbName, String dbUser, String dbPassword){
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbName = dbName;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
    }
    public DB getDB(){
        return db;
    }
    public HashSet<ObjectId> getArticleIds(){
        this.makeConnection();
        HashSet<ObjectId> articles = new HashSet<ObjectId>();
        return articles;
        
    }
    public boolean makeConnection(){
        try {
            MongoClient mongoClient = new MongoClient(dbHost, dbPort);
           
            db = mongoClient.getDB(dbName);
            if(db == null) return false;
            
            if(dbUser != null && dbPassword != null){
                return db.authenticate(dbUser, dbPassword.toCharArray());
            }
            return true;
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }
    }
    public boolean getArticles() {
        LuceneIndexer indexer = new LuceneIndexer();
        String collectionName = "Article"; 
        if(!db.collectionExists(collectionName)) return false;
        BasicDBObject query = new BasicDBObject();
        long DAY_IN_MS = 1000 * 60 * 60 * 24;
        Date daysAgo =new Date(new Date().getTime() - 5 * DAY_IN_MS); 
        query.put("publishDate", BasicDBObjectBuilder.start("$gte", daysAgo).get());
        DBCursor cursor = db.getCollection(collectionName).find(query);
        
        String folderName = "temp/articles/";
        File tempFolder = new File(folderName);
        for (File file : tempFolder.listFiles()) {
            file.delete();
        }   
        while(cursor.hasNext()){
            DBObject article = cursor.next();
            if(article.get("desc") == null || article.get("_id") == null) continue;
            indexer.addDocument(article.get("_id").toString(), article.get("desc").toString());
            File file = new File(folderName + article.get("_id"));
            try {
                file.createNewFile();
                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(article.get("desc").toString());
                bw.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        indexer.createIndexing();
        return true;
    }
    public void setRecommendation(HashMap<ObjectId, HashMap<ObjectId, Float>> recommendations){
        this.makeConnection();
        DBCollection user = this.db.getCollection("User");
        
        for(ObjectId userID: recommendations.keySet() ){
            List<BasicDBObject> articles = new ArrayList<BasicDBObject>();
            for(ObjectId articleId : recommendations.get(userID).keySet()){
                BasicDBObject article = new BasicDBObject();
                article.put("articleId", articleId);
                article.put("score", recommendations.get(userID).get(articleId));
                articles.add(article);
            }
            DBObject query = new BasicDBObject();
            query.put("_id", userID);
            
            BasicDBObject update = new BasicDBObject();
            update.append("$unset", new BasicDBObject().append("recommends", null));
            user.findAndModify(query, update);
            
            update = new BasicDBObject();
            update.append("$set", new BasicDBObject().append("recommends", articles));
            user.findAndModify(query, update);
        }
        
    }
}
