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
import org.bson.types.ObjectId;



import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
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
    private boolean makeConnection(){
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
    public DBCollection getCollection(String collection){
        this.makeConnection();
        return this.getDB().getCollection(collection);
    }
    public Iterable<DBObject> getArticles(Date fromDate) {
        this.makeConnection();
        String collectionName = "Article"; 
        if(!db.collectionExists(collectionName)) return null;
        
        BasicDBObject query = new BasicDBObject();
        query.put("publishDate", BasicDBObjectBuilder.start("$gte", fromDate).get());
        DBCursor cursor = db.getCollection(collectionName).find(query);
        return cursor; 

    }
    public void setRecommendation(HashMap<ObjectId, HashMap<ObjectId, Float>> recommendations){
        this.makeConnection();
        DBCollection user = this.db.getCollection("User");
        
        for(ObjectId userID: recommendations.keySet() ){
            List<BasicDBObject> articles = new ArrayList<BasicDBObject>();
            for(ObjectId articleId : recommendations.get(userID).keySet()){
                //if(articleId == null) continue;
                BasicDBObject article = new BasicDBObject();
                DBRef articleRef = new DBRef(db, "Article", articleId); 
                article.put("article", articleRef);
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
    public HashSet<ObjectId> getLatestArticleIds(){
        this.makeConnection();
        
        HashSet<ObjectId> articles = new HashSet<ObjectId>();
        DBCursor cursor = this.getDB().getCollection("Article").find(new BasicDBObject().append("publishDate", new BasicDBObject().append("$gte", new Date(new Date().getTime()-7 * 24 * 60 * 60 * 1000))));
        while(cursor.hasNext()){
            articles.add((ObjectId) cursor.next().get("_id"));
            
        }
        return articles;
    }
    public void updateArticlePopularity(HashMap<ObjectId, Double> popularityMatrix){
        DBCollection globalArticleRanking = this.getCollection("globalArticleRanking");
        globalArticleRanking.remove(new BasicDBObject());
        for(ObjectId id:popularityMatrix.keySet()){
            globalArticleRanking.insert(new BasicDBObject().append("_id", id).append("popularity", popularityMatrix.get(id)));
        }
    }
    public void updateArticleScore(HashMap<ObjectId, Double> scoreMatrix){
        DBCollection globalArticleRanking = this.getCollection("globalArticleRanking");
        for(ObjectId id:scoreMatrix.keySet()){
            globalArticleRanking.update(new BasicDBObject().append("_id", id), new BasicDBObject().append("$set", new BasicDBObject().append("score", scoreMatrix.get(id))));
        }
    }
    public HashMap<ObjectId, Double> getArticleStatistics(String field){
        DBCollection globalArticleRanking = getCollection("globalArticleRanking");
        HashMap<ObjectId, Double>  globalPreferences = new HashMap<ObjectId, Double>();
        DBCursor cursor = globalArticleRanking.find();
        while(cursor.hasNext()){
            DBObject ranking = cursor.next();
            globalPreferences.put((ObjectId)ranking.get("_id"), (Double)ranking.get(field));
        }
        return globalPreferences;
    }
    public Date getLastUpdateTime(String type){
        System.out.println(type);
        DBCollection configurations = getCollection("Configuration");
        DBObject conf = configurations.findOne(new BasicDBObject().append("type", type));
        if(conf == null) return null; 
        Date lastTime = (Date) conf.get("lastUpdateTime");
        return lastTime;
    }
    public void updateLastUpdateTime(String type){
        DBCollection configurations = getCollection("Configuration");
        configurations.update(new BasicDBObject().append("type", type), new BasicDBObject().append("$set", new BasicDBObject().append("lastUpdateTime", new Date())), true, false);
    }
}
