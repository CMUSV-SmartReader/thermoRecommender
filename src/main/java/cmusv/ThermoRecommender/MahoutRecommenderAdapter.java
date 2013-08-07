package cmusv.ThermoRecommender;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.recommender.RandomRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.bson.types.ObjectId;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.MongoException;

public class MahoutRecommenderAdapter {
    protected MongoAdapter reader;
    protected static HashMap<Long, ObjectId> userIdMapping;
    protected static HashMap<Long, ObjectId> articleIdMapping;
    Recommender recommender;
     
    public MahoutRecommenderAdapter(MongoAdapter reader){
       
        this.reader = reader;
    }
    
    public DataModel createDBModel(){
        
        if(userIdMapping== null) userIdMapping = new HashMap<Long, ObjectId>();
        if(articleIdMapping== null) articleIdMapping = new HashMap<Long, ObjectId>();
        
        Date fromDate = DateUtils.getDaysAgo(5);
        
        HashMap<Long, ArrayList<Preference>> prefs = new HashMap<Long, ArrayList<Preference>>();
        Iterable articleCursor = (Iterable) reader.getArticles(fromDate);
        
        for(DBObject user: reader.getCollection("User").find()){
            ObjectId userId = (ObjectId) user.get("_id");
            userIdMapping.put((long) userId.hashCode(), userId);
        }
        
        for(DBObject article : ((DBCursor)articleCursor).copy()){
            ObjectId articleId = (ObjectId) article.get("_id");
            articleIdMapping.put((long) articleId.hashCode(), articleId);
        }
        for(long uid: userIdMapping.keySet()){
            ObjectId userId = userIdMapping.get(uid);
            
            DBObject user = reader.getCollection("User").findOne(new BasicDBObject().append("_id",  userId));
            List<ObjectId> userFeeds = null;
            if(user.containsField("feeds")){
                userFeeds = new ArrayList<ObjectId>(Collections2.transform((BasicDBList)user.get("feeds"), new Function<Object, ObjectId>(){
                    @Override
                    public ObjectId apply(Object input) {
                        return (ObjectId) ((DBRef)input).getId();
                    }
                }));
            }
            BasicDBObject query = (BasicDBObject)((DBCursor)articleCursor).getQuery();
            if(userFeeds != null) query.append("feed.$id", new BasicDBObject().append("$in", userFeeds));
            HashSet<ObjectId> userArticles = new HashSet<ObjectId>();
            for(DBObject article: reader.getCollection("Article").find(query)){
                userArticles.add((ObjectId) article.get("_id"));
            }
            HashSet<ObjectId> readArticles = new HashSet<ObjectId>();
            for(DBObject userArticle: reader.getCollection("UserArticle").find( new BasicDBObject().append("user.$id", userId))){
                readArticles.add((ObjectId) userArticle.get("_id"));
            }
            for(ObjectId articleId: userArticles){
                long aid = articleId.hashCode();
                float rating = (float) ((readArticles.contains(articleId))?0.9f:0.005f);
                if(!prefs.containsKey(userId)) 
                    prefs.put((long)userId.hashCode(), new ArrayList<Preference>());
                prefs.get((long)userId.hashCode()).add(new GenericPreference(uid, aid, rating));
            }
        }
        
        FastByIDMap<PreferenceArray> userData = new FastByIDMap<PreferenceArray>();
        for(Long userID :prefs.keySet()){
            userData.put(userID, new  GenericUserPreferenceArray(prefs.get(userID)));
        }
        return new GenericDataModel(userData);
    }
    
    
    public void setRecommender(Recommender recommender){
        this.recommender = recommender;
    }
    public void recommendArticles(int numRecommendations){
        
        try {
            DataModel dbm = this.createDBModel();
            //
            if(recommender == null) 
                recommender = new RandomRecommender(dbm); 
            LongPrimitiveIterator it = dbm.getUserIDs();
            
            HashMap<ObjectId, HashMap<ObjectId, Float>> result = new HashMap<ObjectId, HashMap<ObjectId, Float>>();
            while(it.hasNext()){
                Long userId = it.next();
                App.getLogger().debug("UserId: " + userIdMapping.get(userId));
                List<RecommendedItem> items = recommender.recommend(userId, numRecommendations);
                System.out.println("Recommended items:" +items.size());
                HashMap<ObjectId, Float> transferedItems = new HashMap<ObjectId, Float>();
                for(RecommendedItem item: items){
                    transferedItems.put(articleIdMapping.get(item.getItemID()), item.getValue());
                }
                if(items.size() < numRecommendations){
                  //reader.getCollection("Article").find(new BasicDBObject());
                }
                result.put(userIdMapping.get(userId), transferedItems);
            }
            
            reader.setRecommendation(result);
            
        } catch (TasteException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MongoException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

}
