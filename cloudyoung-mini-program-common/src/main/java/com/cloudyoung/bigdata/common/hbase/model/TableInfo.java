package com.cloudyoung.bigdata.common.hbase.model;

public class TableInfo {

    private String nameSpace;
    private String name;
    private int split;
    private String family;
    private int ttl;
    private boolean is_result_table;
    private String db_database;
    private String db_table;

    public boolean isIs_result_table() {
        return is_result_table;
    }

    public void setIs_result_table(boolean is_result_table) {
        this.is_result_table = is_result_table;
    }

    public String getDb_database() {
        return db_database;
    }

    public void setDb_database(String db_database) {
        this.db_database = db_database;
    }

    public String getDb_table() {
        return db_table;
    }

    public void setDb_table(String db_table) {
        this.db_table = db_table;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }


    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSplit() {
        return split;
    }

    public void setSplit(int split) {
        this.split = split;
    }

}
