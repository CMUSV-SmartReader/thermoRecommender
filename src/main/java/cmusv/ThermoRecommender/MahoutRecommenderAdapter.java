package cmusv.ThermoRecommender;

import java.util.ArrayList;
import java.util.HashMap;
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
        
        HashMap<Long, ArrayList<Preference>> prefs = new HashMap<Long, ArrayList<Preference>>();
        DBCollection userArticles = reader.getCollection("UserArticle");
        DBCursor cursor = userArticles.find();
        
        while (cursor.hasNext()) {
            DBObject userArticle = cursor.next();
            
            ObjectId userID = (ObjectId) ((DBRef)userArticle.get("user")).getId();
            Long uID = (long) userID.hashCode();
            userIdMapping.put(uID, userID);
            
            ObjectId articleID = (ObjectId) ((DBRef)userArticle.get("article")).getId();
            Long aID = (long) articleID.hashCode();
            articleIdMapping.put(aID, articleID);
            
            
            float rating = (float) ((Boolean) (userArticle.get("isRead"))?0.9f:0.0f);
            if(!prefs.containsKey(userID)) 
                prefs.put((long)userID.hashCode(), new ArrayList<Preference>());
            prefs.get(uID).add(new GenericPreference(uID, aID, rating));
        }
        FastByIDMap<PreferenceArray> userData = new FastByIDMap<PreferenceArray>();
        for(Long userID :prefs.keySet()){
            userData.put(userID, new  GenericUserPreferenceArray(prefs.get(userID)));
        }
        cursor =reader.getCollection("User").find();
        while(cursor.hasNext()){
            ObjectId uID = (ObjectId) cursor.next().get("_id");
            Long userID = (long) uID.hashCode();
            
            if(!userData.containsKey(userID)){
                userData.put(userID, new GenericUserPreferenceArray(0));
                userIdMapping.put(userID, uID);
            }
        }
        return new GenericDataModel(userData);
    }
    
    public void clustering(){
        
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
                System.out.println("UserId: " + userIdMapping.get(userId));
                List<RecommendedItem> items = recommender.recommend(userId, numRecommendations);
                HashMap<ObjectId, Float> transferedItems = new HashMap<ObjectId, Float>();
                for(RecommendedItem item: items){
                    transferedItems.put(articleIdMapping.get(item.getItemID()), item.getValue());
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
