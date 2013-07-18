package cmusv.ThermoRecommender;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.HashSet;
import org.bson.types.ObjectId;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

public class GlobalRecommender implements Recommender{
    private MongoAdapter dbAdapter;
    private DataModel dbm;
    private static HashMap<ObjectId, Double> globalPreferences;
    private static HashMap<ObjectId, Double> popularityMatrix;
    public GlobalRecommender(DataModel dbm, MongoAdapter dbAdapter){
        this.dbAdapter = dbAdapter;
        this.dbm = dbm;
    }
    public DataModel getDataModel() {
        return dbm;
    }
    public HashMap<ObjectId, Double> countArticlePopularity(){
        Date lastTime = dbAdapter.getLastUpdateTime("articlePopularity");
        if(lastTime != null && lastTime.after(DateUtils.getHoursAgo(3))){
            popularityMatrix = dbAdapter.getArticleStatistics("popularity");
            return popularityMatrix;
        }
        
        App.getLogger().debug("Normalize Article Popularity");
        
       
        DBCursor cursor = (DBCursor) dbAdapter.getArticles(DateUtils.getDaysAgo(10));
        popularityMatrix = new HashMap<ObjectId, Double>();
        long max = (int) cursor.copy().sort(new BasicDBObject().append("popularity", -1)).limit(1).next().get("popularity");
        long min = (int) cursor.copy().sort(new BasicDBObject().append("popularity", 1)).limit(1).next().get("popularity");;
        
        for(DBObject article: cursor){
            double popularity = ((int)article.get("popularity") - min + 0.0001) / (max-min+0.0001);
            popularityMatrix.put((ObjectId) article.get("_id"), (double) popularity);
        }
        App.getLogger().debug("Push to DB");
        dbAdapter.updateArticlePopularity(popularityMatrix);
        dbAdapter.updateLastUpdateTime("articlePopularity");
        App.getLogger().debug("Finish Create Article Popularity");
        return popularityMatrix;
    }
    public HashMap<ObjectId, Double> createArticleScore(){
        Date lastTime = dbAdapter.getLastUpdateTime("articleScore");
        if(lastTime != null && lastTime.after(DateUtils.getHoursAgo(1))){
            globalPreferences = dbAdapter.getArticleStatistics("score");
            return globalPreferences;
        }
        App.getLogger().debug("Create Article Score");
        
        countArticlePopularity();
        globalPreferences = new HashMap<ObjectId, Double>();
        DBCollection articles = dbAdapter.getCollection("Article");
        for(ObjectId id:this.dbAdapter.getLatestArticleIds()){
            DBObject article = articles.findOne(new BasicDBObject().append("_id", id));
            Date lastDate = new Date(new Date().getTime() - 1000 * 60 * 60 * 12);
            if(article == null ){
                continue;
            }
            if(article.containsField("updateDate")){
                lastDate = (Date)article.get("updateDate");
            }else if(article.containsField("publishDate")){
                lastDate = (Date)article.get("publishDate");
            }
            Double diff = (new Date().getTime() - lastDate.getTime())/(1000d * 60 * 60 * 24);
            Double popularity = (popularityMatrix.containsKey(id))?popularityMatrix.get(id): 0.0001;
            
            globalPreferences.put(id, popularity * Math.exp(1/(diff+1)));
        }
        dbAdapter.updateArticleScore(globalPreferences);
        dbAdapter.updateLastUpdateTime("articleScore");
        App.getLogger().debug("Finish Create Article Score");
        return globalPreferences;
    }
    
    public List<RecommendedItem> recommend(long userID, int howMany)
            throws TasteException {
        this.countArticlePopularity();
        this.createArticleScore();
        
        ObjectId uid = MahoutRecommenderAdapter.userIdMapping.get(userID);
        DBCollection userArticles = dbAdapter.getCollection("UserArticle");
        List<RecommendedItem> result = new ArrayList<RecommendedItem>();
        
        for(DBObject article:  dbAdapter.getArticles(DateUtils.getDaysAgo(5))){
        
            ObjectId articleID = (ObjectId)article.get("_id");
            double score = (!globalPreferences.containsKey(articleID))? 0.5: globalPreferences.get(articleID);
            
            if(userArticles.findOne(new BasicDBObject().append("user.$id", uid).append("article.$id", articleID)) != null)
               continue;
            result.add(new GenericRecommendedItem(articleID.hashCode(), (float) score ));
        }
        
        Collections.sort(result, new Comparator<RecommendedItem>(){
            public int compare(RecommendedItem r1,
                    RecommendedItem r2) {
                
                return -Float.compare(r1.getValue(), r2.getValue());
            }});
        result = result.subList(0, Math.min(howMany, result.size()));
        return result;
    }
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        // TODO Auto-generated method stub
        
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
    
}
