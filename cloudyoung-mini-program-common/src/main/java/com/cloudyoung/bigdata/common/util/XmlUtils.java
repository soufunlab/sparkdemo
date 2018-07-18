package com.cloudyoung.bigdata.common.util;

import com.cloudyoung.bigdata.common.Constant;
import com.cloudyoung.bigdata.common.hbase.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class XmlUtils {
    private static Logger logger = LoggerFactory.getLogger(XmlUtils.class);

    public static void main(String[] args) throws Exception {

//        Map<String, TableInfo> tableInfoMap = parseOptions("/hbase-table-info.xml");
        Map<String, Object> map = parseGlobalXML("E:\\yunyang\\mini-progrom\\cloudyoung-mini-program-calculate\\conf\\mini-program-global.xml");
        System.out.println(map.get("hbase"));
    }

    /**
     * hbase 表xml解析
     *
     * @return
     */
    public static Map<String, TableInfo> parseOptions(InputStream inputStream) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(inputStream);
        return parseTableInfo(document);
    }

    public static Map<String, TableInfo> parseOptions(String xmlPath) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(xmlPath);
        return parseTableInfo(document);
    }


    private static Map<String, TableInfo> parseTableInfo(Document document){
        document.getDocumentElement();
        NodeList tableList = document.getElementsByTagName("table");
        Map<String, TableInfo> tableInfoHashMap = new HashMap<>();
        for (int i = 0, len = tableList.getLength(); i < len; i++) {
            Node node = tableList.item(i);
            Element tableElement = (Element) node;
            String tableName = XmlUtils.parseElement(tableElement, "name");
            String nameSpace = XmlUtils.parseElement(tableElement, "namespace");
            String family = XmlUtils.parseElement(tableElement, "family");
            int split = Integer.parseInt(XmlUtils.parseElement(tableElement, "split"));
            int ttl = Integer.parseInt(XmlUtils.parseElement(tableElement, "ttl"));
            boolean is_result_table = Boolean.parseBoolean(XmlUtils.parseElement(tableElement,"is_result_table"));
            TableInfo hBaseTableInfo = new TableInfo();
            hBaseTableInfo.setName(tableName);
            hBaseTableInfo.setNameSpace(nameSpace);
            hBaseTableInfo.setSplit(split);
            hBaseTableInfo.setFamily(family);
            hBaseTableInfo.setTtl(ttl);
            hBaseTableInfo.setIs_result_table(is_result_table);
            if(is_result_table){
                String db_database = XmlUtils.parseElement(tableElement, "db_database");
                String db_table = XmlUtils.parseElement(tableElement, "db_table");
                hBaseTableInfo.setDb_database(db_database);
                hBaseTableInfo.setDb_table(db_table);
            }
            String key;
            if (nameSpace != null && !"".equals(nameSpace)) {
                key = nameSpace + Constant.splitChar + tableName;
            } else {
                key = tableName;
            }
            tableInfoHashMap.put(key, hBaseTableInfo);
        }
        return tableInfoHashMap;
    }


    public static String parseElement(Element element, String name) {
        String result = null;
        NodeList nodeList = element.getElementsByTagName(name);
        int len = nodeList.getLength();
        if (len > 0) {
            result = nodeList.item(0).getTextContent().trim();
        }
        return result;
    }

    public static Map<String, Object> parseGlobalXML(String xmlPath) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(xmlPath);
        document.getDocumentElement();
        NodeList tableList = document.getElementsByTagName("global");
        Node node = tableList.item(0);
        return recursionNode(node);
    }

    public static Map<String, Object> parseGlobalXML( InputStream inputStream) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(inputStream);
        document.getDocumentElement();
        NodeList tableList = document.getElementsByTagName("global");
        Node node = tableList.item(0);
        return recursionNode(node);
    }

    public static Map<String, Object> recursionNode(Node node1) {
        NodeList nodeList = node1.getChildNodes();
        int len = nodeList.getLength();
        Map<String, Object> map = new HashMap<>();
        for (int index = 0; index < len; index++) {
            Node node = nodeList.item(index);
            NodeList childNodeList = node.getChildNodes();
            int childNodeListLen = childNodeList.getLength();
            String nodeName = node.getNodeName();
            if (!nodeName.equals("#text")) {
                if (childNodeListLen > 1) {
                    Map<String, Object> nodeMap = recursionNode(node);
                    map.put(nodeName, nodeMap);
                } else {
                    String value = node.getTextContent();
                    map.put(nodeName, value);
                }
            }
        }
        return map;
    }


}
