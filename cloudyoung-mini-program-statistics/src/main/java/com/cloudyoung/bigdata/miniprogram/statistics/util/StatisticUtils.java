package com.cloudyoung.bigdata.miniprogram.statistics.util;

public class StatisticUtils {

    public static String getPageTypeTarget(String page_type){
        String targetName = null;
        switch (page_type){
            case "1": targetName="article"; break;
            case "2": targetName="video"; break;
            case "3": targetName="author"; break;
            case "4": targetName="serial"; break;
            case "5": targetName="topic"; break;
            case "6": targetName="activity"; break;
        }
        return targetName;
    }

    public static String getEventTypeTarget(String event_id){
        String targetName = null;
        switch (event_id){
            case "4": targetName="thumb_num"; break;
            case "5": targetName="comment_num"; break;
            case "6": targetName="enshrine_num"; break;
            case "7": targetName="nolike_num"; break;
            case "8": targetName="share_num"; break;
            case "9": targetName="attent_num"; break;
            case "10": targetName="attent_cancel_num"; break;
        }
        return targetName;
    }

    public static String getIntoTypeTarget(String into_type){
        String targetName = null;
        switch (into_type){
            case "0": targetName="all"; break;
            case "1": targetName="feed"; break;
            case "2": targetName="article_detail"; break;
            case "3": targetName="serial_detail"; break;
            case "4": targetName="author_detail"; break;
            case "5": targetName="share"; break;
            case "6": targetName="share_detail"; break;
            case "7": targetName="video_detail"; break;
        }
        return targetName;
    }


    public static String getHotNavigateTarget(String hot_type){
        String targetName = null;
        switch (hot_type){
            case "0": targetName="all"; break;
            case "1": targetName="video"; break;
            case "2": targetName="article"; break;
            case "3": targetName="author"; break;
            case "4": targetName="serial"; break;
            case "5": targetName="user"; break;
        }
        return targetName;
    }


}
