import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.*;
import java.text.ParseException;
import java.util.*;
public class DBApp implements DBAppInterface{

    Validators validator = new Validators();
    public static void writeDataLineByLine(String filePath,String[]data)
    {

        try {
            // create CSVReader object filereader as a parameter
            CSVReader reader = new CSVReader((new FileReader(filePath)));
            // read all the previous written lines
            List allLines = reader.readAll();
            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(new FileWriter(filePath));
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
        String path = "src/main/resources/data/";
        File file = new File(path);
        if (!file.exists()){
            file.mkdirs();
        }
        try {
            // create CSVReader object filereader as a parameter
            CSVReader reader = new CSVReader((new FileReader("src/main/resources/metadata.csv")));
            // read all the previous written lines
            List allLines = reader.readAll();
            if(allLines.size()==0){
                // create CSVWriter object filewriter object as parameter
                CSVWriter writer = new CSVWriter(new FileWriter("src/main/resources/metadata.csv"));
                // add data to end of the list
                String[] header = new String[1];
                header[0]="Table Name, Column Name, Column Type, Clustering Key, Indexed, Min, Max";
                allLines.add(header);
                writer.writeAll(allLines);
                writer.close();
            }

        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


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
        validator.validateCreateTable(tableName,clusteringKey,colNameType,colNameMin,colNameMax);
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
                validator.validateTypesTable(result[2],result[5]);
                validator.validateTypesTable(result[2],result[6]);
                writeDataLineByLine("src/main/resources/metadata.csv", result);
            }
            Table table = new Table(tableName);
            table.serializeTable(tableName);
        }
        //set clustering key for table for later checks
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        // following method creates one index – either multidimensional
        // or single dimension depending on the count of column names passed.

    }
    public boolean tableExists(String tableName){
        try {

            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;
            // we are going to read data line by line
            while ((nextRecord = reader.readNext()) != null) {
                if(nextRecord[0].equals(tableName))
                    return true;
            }

        }
        catch(Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException {
        // following method inserts one row only
        // htblColNameValue must include a value for the primary key
        validator.validateClusteringKey(tableName, colNameValue);
        validator.validateTypesInsertion(colNameValue);
        validator.validateInsertion(tableName,colNameValue);
        validator.validateColNames(tableName,colNameValue);
        validator.validateRange(tableName,colNameValue);
        String clusteringKey = getClusteringKey(tableName);
        Row row = new Row(clusteringKey, colNameValue);
        Table table =Table.deserializeTable(tableName);
        table.insert(row, tableName);

    }

    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException, ParseException {
        // following method updates one row only
        // htblColNameValue holds the key and new value
        // htblColNameValue will not include clustering key as column name
        // strClusteringKeyValue is the value to look for to find the rows to update.

        validator.validateUpdate(tableName,columnNameValue);
        validator.validateRange(tableName,columnNameValue);
        Object value = validator.getClusteringValue(validator.getClusteringType(tableName),clusteringKeyValue);
        Table table = Table.deserializeTable(tableName);
        String clusteringKey = getClusteringKey(tableName);
        table.update(tableName,columnNameValue, value,clusteringKey);


    }

    public void deleteFromTable(String tableName, Hashtable<String , Object> columnNameValue) throws DBAppException,IOException {
        // following method could be used to delete one or more rows.
        // htblColNameValue holds the key and value. This will be used in search
        // to identify which rows/tuples to delete.
        //htblColNameValue enteries are ANDED together
         String clusteringKey = getClusteringKey(tableName);
        Table table = Table.deserializeTable(tableName);
        if(columnNameValue.keySet().contains(clusteringKey))
             table.deleteBinary(tableName,columnNameValue,columnNameValue.get(clusteringKey),clusteringKey);
        else
            table.deleteLinear(tableName,columnNameValue);

    }

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

    public String getClusteringKey(String tableName){
        try {

            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;
            // we are going to read data line by line
            while ((nextRecord = reader.readNext()) != null) {
                if(nextRecord[0].equals(tableName))
                    if(nextRecord[3].equals("True")){
                        return nextRecord[1];
                    }
            }

        }
        catch(Exception e){
            e.printStackTrace();
        }
        return "";
    }



    public static void main(String[] args) throws  Exception{
        DBApp test = new DBApp();
        test.init();

    }
}
