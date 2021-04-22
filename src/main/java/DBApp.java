import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.lang.Object;
import java.util.List;
import java.util.Set;
import java.util.*;

public class DBApp implements DBAppInterface{

    public static void writeDataLineByLine(String filePath,String[]data)
    {

        try {
            // create CSVReader object filereader as a parameter
            CSVReader reader = new CSVReader((new FileReader(new File(filePath))));
            // read all the previous written lines
            List allLines = reader.readAll();
            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(new FileWriter(new File(filePath)));
            // add data to end of the list
            allLines.add(data);
            writer.writeAll(allLines);
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void init() {
        // this does whatever initialization you would like
        // or leave it empty if there is no code you want to
        // execute at application startup

    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        //EXCEPTION FOR HASHTABLE SIZES
        // following method creates one table only
        // strClusteringKeyColumn is the name of the column that will be the primary
        // key and the clustering column as well. The data type of that column will
        // be passed in htblColNameType
        // htblColNameValue will have the column name as key and the data
        // type as value
        // htblColNameMin and htblColNameMax for passing minimum and maximum values
        // for data in the column. Key is the name of the column
        Set<String> nameType = colNameType.keySet();
        //Set<String> nameMin = colNameMin.keySet();
        //Set<String> nameMax = colNameMax.keySet();

        Iterator<String> itrType = nameType.iterator();
       /* Iterator<String> itrMin = nameMin.iterator();
        Iterator<String> itrMax = nameMax.iterator();

        String colName;
        String colType;
        String cKey;
        String indexed;
        String min;
        String max;*/
        String[]result = new String[7];
        result[0]=tableName;

        while (itrType.hasNext()) {
            // Getting Key
            result[1]=itrType.next(); //colName
            result[2]=colNameType.get(result[1]); //colType
            result[3]= (result[1].equals(clusteringKey))?"True":"False"; //clustering key
            result[4]="False"; //indexed
            result[5]=colNameMin.get(result[1]); //min
            result[6]=colNameMax.get(result[1]); //max
            //System.out.println(result + " before ");
            writeDataLineByLine("src/main/resources/metadata.csv",result);
            //System.out.println(result + " after ");
        }
        //create new table ?
    }
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        // following method creates one index – either multidimensional
        // or single dimension depending on the count of column names passed.

    }

    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        // following method inserts one row only.
        // htblColNameValue must include a value for the primary key

    }

    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        // following method updates one row only
        // htblColNameValue holds the key and new value
        // htblColNameValue will not include clustering key as column name
        // strClusteringKeyValue is the value to look for to find the rows to update.

    }

    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        // following method could be used to delete one or more rows.
        // htblColNameValue holds the key and value. This will be used in search
        // to identify which rows/tuples to delete.
        //htblColNameValue enteries are ANDED together

    }

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

    public static void main(String[] args) throws  DBAppException{
        //String[]data = {"City Shop","ID","java.lang.Integer","True","True","0","10000"};
        //writeDataLineByLine("src/main/resources/metadata.csv",data);
        /*Hashtable hash = new Hashtable();
        hash.put("A", "Ahmed");
        hash.put("D", "Donia");
        hash.put("S", "Salma w Seif");
        Set<String> nameType = hash.keySet();
        Hashtable hash2 = new Hashtable();
        hash.put("D", "1");
        hash.put("S", "2");
        Set<String> nameTypeAwy = hash.keySet();
        System.out.println(nameType);
        System.out.println(nameTypeAwy);*/
        String strTableName = "Student";
        DBApp dbApp = new DBApp( );
        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");

        Hashtable htblColNameMin = new Hashtable( );
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "AAAA");
        htblColNameMin.put("gpa", "0.7");

        Hashtable htblColNameMax = new Hashtable( );
        htblColNameMax.put("id", "1000");
        htblColNameMax.put("name", "ZZZZ");
        htblColNameMax.put("gpa", "5.0");


        dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax );

    }
}
