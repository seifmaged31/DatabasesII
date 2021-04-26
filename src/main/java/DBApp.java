import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.*;
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
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException,IOException {
        //EXCEPTION FOR HASHTABLE SIZES
        // following method creates one table only
        // strClusteringKeyColumn is the name of the column that will be the primary
        // key and the clustering column as well. The data type of that column will
        // be passed in htblColNameType
        // htblColNameValue will have the column name as key and the data
        // type as value
        // htblColNameMin and htblColNameMax for passing minimum and maximum values
        // for data in the column. Key is the name of the column
        Validators.validateCreateTable(tableName,clusteringKey,colNameType,colNameMin,colNameMax);
        Set<String> nameType = colNameType.keySet();
            Iterator<String> itrType = nameType.iterator();
            String[] result = new String[7];
            result[0] = tableName;

            while (itrType.hasNext()) {
                // Getting Key
                result[1] = itrType.next(); //colName
                result[2] = colNameType.get(result[1]) +""; //colType
                result[3] = (result[1].equals(clusteringKey)) ? "True" : "False"; //clustering key
                result[4] = "False"; //indexed
                result[5] = "" + colNameMin.get(result[1]) ; //min
                result[6] = "" + colNameMax.get(result[1]) ; //max
                writeDataLineByLine("src/main/resources/metadata.csv", result);
            }
            Table table = new Table(tableName);
        }
        //set clustering key for table for later checks
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        // following method creates one index – either multidimensional
        // or single dimension depending on the count of column names passed.

    }

    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException {
        // following method inserts one row only
        // htblColNameValue must include a value for the primary key
        Row row = new Row("id", colNameValue);
       Table table = Table.deserializeTable(tableName);
       table.insert(row, tableName);

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

    public static void main(String[] args) throws  Exception{

        String strTableName = "seif";
        DBApp dbApp = new DBApp( );
//        Hashtable htblColNameType = new Hashtable( );
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.double");
//
//        Hashtable min = new Hashtable();
//        min.put("id", "1");
//        min.put("name", "a");
//        min.put("gpa", "1");
//
//        Hashtable max = new Hashtable();
//        max.put("id", "100");
//        max.put("name","zzzzzzzzzzzzzzz");
//        max.put("gpa","4.0");
//        dbApp.createTable( strTableName, "id", htblColNameType,min, max);
//
//        Hashtable htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 2343432 ));
//        htblColNameValue.put("name", new String("Ahmed Noor" ) );
//        htblColNameValue.put("gpa", new Double( 0.95 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );

        Page table=null;
        try{
            FileInputStream fileIn =
                    new FileInputStream(new File("src/main/resources/Data/" + "seif_1" +".class"));
            ObjectInputStream in= new ObjectInputStream(fileIn);
            table = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (FileNotFoundException | ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(table);
    }
}
