package cmusv.ThermoRecommender;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;

import org.apache.mahout.cf.taste.common.TasteException;
import org.bson.types.ObjectId;
import org.easymock.EasyMock;

import junit.framework.TestCase;
import static org.easymock.EasyMock.*;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;


public class GlobalRecommenderTest extends TestCase {
    private MongoAdapter dbAdapter;
    private MahoutRecommenderAdapter mahoutAdapter;
    private GlobalRecommender recommender;
    public GlobalRecommenderTest() throws IOException{
        dbAdapter = new MongoAdapter("localhost", 27017, "thermoreader", null, null);
        mahoutAdapter = new MahoutRecommenderAdapter(dbAdapter);
        recommender = new GlobalRecommender(mahoutAdapter.createDBModel(), dbAdapter);;
    }
    
    public void testArticlePopularityCount(){
        System.out.println("test");
        System.out.println(recommender.countArticlePopularity());
        assertEquals("", true, recommender.countArticlePopularity());
    }
    public void testArticleScore(){
        System.out.println(recommender.createArticleScore());
    }
    public void testRecommendation() throws TasteException{
        recommender.recommend(new ObjectId("51cf362ee4b08233e8167fcc").hashCode(), 20);
    }
}
