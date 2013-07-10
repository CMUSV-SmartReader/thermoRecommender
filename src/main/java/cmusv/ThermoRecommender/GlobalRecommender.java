package cmusv.ThermoRecommender;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import java.util.HashSet;
import java.util.logging.Logger;
import java.util.prefs.Preferences;

import org.bson.types.ObjectId;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.model.Preference;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

public class GlobalRecommender implements Recommender{
    private MongoAdapter dbAdapter;
    private DataModel dbm;
    private HashMap<Float, List<RecommendedItem>> recommends; 
    private static HashMap<ObjectId, Double> globalPreferences;
    private static Date lastDate; 
    public GlobalRecommender(DataModel dbm, MongoAdapter dbAdapter){
        this.dbAdapter = dbAdapter;
        this.dbm = dbm;
        this.recommends = new HashMap<Float, List<RecommendedItem>>();
        this.countArticlePopularity();
        this.createArticleScore();
    }
    
    public void countArticlePopularity(){
        if(lastDate!= null && !lastDate.before(new Date(new Date().getTime() - 5 * 60 * 60 * 1000)) && globalPreferences != null){
            return;
        }
        lastDate = new Date();
        System.err.println("Create Article Popularity");
        DBCollection userArticles = dbAdapter.getCollection("UserArticle");
        HashSet<ObjectId> articles = dbAdapter.getLatestArticleIds();
        HashMap<ObjectId, Double> popularityMatrix = new HashMap<ObjectId, Double>();
        long max = 0, min = 0;
        for(ObjectId id: articles){
            long popularity = userArticles.getCount(new BasicDBObject().append("article.$id", id));
            if(popularity > max ) max = popularity;
            if(popularity < min) min = popularity;
            popularityMatrix.put(id, (double) popularity);
        }
        double diff = (max-min==0)?0.001:(max-min);
        
        for(ObjectId id:articles){
            popularityMatrix.put(id, (popularityMatrix.get(id) - min)/ diff);
        }
        System.err.println("Push to DB");
        dbAdapter.updateArticlePopularity(popularityMatrix);
        System.err.println("Finish Create Article Popularity");
    }
    public void createArticleScore(){
        System.err.println("Create Article Score");
        globalPreferences = new HashMap<ObjectId, Double>();
        DBCollection articles = dbAdapter.getCollection("Article");
        for(ObjectId id:this.dbAdapter.getLatestArticleIds()){
            DBObject article = articles.findOne(new BasicDBObject().append("_id", id));
            Date lastDate = new Date(new Date().getTime() - 1000 * 60 * 60 * 12);
            if(article == null ){
                continue;
            }
            if(article.containsField("publishDate")){
                lastDate = (Date)article.get("publishDate");
            }
            Double diff = (new Date().getTime() - lastDate.getTime())/(1000d * 60 * 60 * 24);
            Double popularity ;
            if(!article.containsField("popularity")){
                popularity = 0d;
            }
            if(article.get("popularity").getClass().equals("java.lang.Integer")){
                popularity = (Integer) article.get("popularity") * 1.0d ;
            }
            else{
                popularity = (Double)  article.get("popularity") * 1.0d;
            }
            globalPreferences.put(id, popularity * Math.exp(1/(diff+1)));
        }
        System.err.println("Finish Create Article Score");
        /*
        */
    }
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        // TODO Auto-generated method stub
        
    }
    public List<RecommendedItem> recommend(long userID, int howMany)
            throws TasteException {
        System.out.println(userID);
        ObjectId uid = MahoutRecommenderAdapter.userIdMapping.get(userID);
        DBCollection userArticles = dbAdapter.getCollection("UserArticle");
        DBCursor cursor = userArticles.find(new BasicDBObject().append("user.$id", uid));
        List<RecommendedItem> result = new ArrayList<RecommendedItem>();
        while(cursor.hasNext()){
            DBObject userArticle =  cursor.next();
            
            if((Boolean)userArticle.get("isRead")) continue;
            ObjectId articleId = (ObjectId) ((DBRef)userArticle.get("article")).getId();
            double score = globalPreferences.get(articleId);
            result.add(new GenericRecommendedItem(articleId.hashCode(), (float) score ));
        }
        
        Collections.sort(result, new Comparator<RecommendedItem>(){
            public int compare(RecommendedItem r1,
                    RecommendedItem r2) {
                
                return -Float.compare(r1.getValue(), r2.getValue());
            }});
        result = result.subList(0, Math.min(howMany, result.size()));
        return result;
    }
    public List<RecommendedItem> recommend(long userID, int howMany,
            IDRescorer rescorer) throws TasteException {
        // TODO Auto-generated method stub
        return null;
    }
    public float estimatePreference(long userID, long itemID)
            throws TasteException {
        // TODO Auto-generated method stub
        return 0;
    }
    public void setPreference(long userID, long itemID, float value)
            throws TasteException {
        // TODO Auto-generated method stub
        
    }
    public void removePreference(long userID, long itemID)
            throws TasteException {
        // TODO Auto-generated method stub
        
    }
    public DataModel getDataModel() {
        return dbm;
    }
}
