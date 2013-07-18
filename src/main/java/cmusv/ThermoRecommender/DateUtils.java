package cmusv.ThermoRecommender;

import java.util.Date;

public class DateUtils {
    public static Date getDaysAgo(int dayAgo){
        return new Date(new Date().getTime() - dayAgo * 24 * 60 * 60 * 1000);
    }
    public static Date getHoursAgo(int hourAgo){
        return new Date(new Date().getTime() - hourAgo * 60 * 60 * 1000);
    }
}
