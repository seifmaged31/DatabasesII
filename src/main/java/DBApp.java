import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
    public void createIndex(String tableName, String[] columnNames) throws DBAppException, IOException {
        // following method creates one index – either multidimensional
        // or single dimension depending on the count of column names passed.
        boolean found;
        Object min;
        Object max;
        Hashtable<String,Object> tempHash = new Hashtable();
        for(String column:columnNames){
            tempHash.put(column,"");
        }
        String[] columns = new String[columnNames.length];
        int count=0;
        for(String column:tempHash.keySet()){
            columns[count]=column;
            count++;
        }
        ArrayList<Range> ranges = new ArrayList<>();
        try {
            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;
            // we are going to read data line by line
            while ((nextRecord = reader.readNext()) != null) {
                if(nextRecord[0].equals(tableName))
                {
                  if(Arrays.asList(columnNames).contains(nextRecord[1])) {
                      nextRecord[4] = "true";
                      min= getValue(nextRecord[5],nextRecord[2].toLowerCase());
                      max = getValue(nextRecord[6],nextRecord[2].toLowerCase());
                      ranges.add(new Range(min,max,nextRecord[2].toLowerCase()));// ranges b tarteeb el hierarchy
                  }
                }

            }

        }
        catch(Exception e){
            e.printStackTrace();
        }
        ArrayList indices = Table.getIndices(tableName,tempHash);
        GridIndex gridIndex = new GridIndex(tableName,columns,ranges,indices);
        Table table = Table.deserializeTable(tableName);
        gridIndex.serializeGrid();
        placeCells(gridIndex,table,indices);
        gridIndex.serializeGrid();
//        table.serializeTable(tableName);

    }
    public Object getValue(String value,String type) throws ParseException {

        if(type.equals("java.lang.integer"))
            return Integer.parseInt(value);
        if(type.equals("java.lang.double"))
            return Double.parseDouble(value);
        if(type.equals("java.lang.string"))
            return value;
        return (new SimpleDateFormat("yyyy-MM-dd")).parse(value);

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
//        String[] colNames = new String[colNameValue.size()];
//        for(String colName: (ArrayList<String>)colNameValue.keySet())
//            colNames[((ArrayList<String>)((ArrayList<?>) colNameValue.keySet())).indexOf(colName)]=colName;
//        GridIndex gridIndex = GridIndex.deserializeGrid(tableName,colNames);
//        if(gridIndex!=null){
//            gridIndex.insertGrid(row,);
//        }
//         insert in the grid
        Table table = Table.deserializeTable(tableName);
        table.insert(row,tableName,colNameValue);// Continue writing there.

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
        ArrayList listOfIndices = Table.getIndices(tableName, columnNameValue);

        if(columnNameValue.keySet().contains(clusteringKey))
             table.deleteBinary(tableName,columnNameValue,columnNameValue.get(clusteringKey),clusteringKey);
        else
            table.deleteLinear(tableName,columnNameValue);


    }

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        Table table = Table.deserializeTable(sqlTerms[0]._strTableName);
        Iterator itr=null;
        Hashtable<String,String> tempHash = new Hashtable<>();
        for(SQLTerm sqlTerm:sqlTerms){
                tempHash.put(sqlTerm._strColumnName,"");
        }
        String[] colNames = new String[tempHash.size()];
        ArrayList<String> colNamesArrayList = new ArrayList<>(tempHash.keySet());
        for(String colName: colNamesArrayList)
            colNames[colNamesArrayList.indexOf(colName)]=colName;
        GridIndex gridIndex = GridIndex.deserializeGrid(sqlTerms[0]._strTableName,colNames);
        try {
            if(gridIndex!=null){
                itr= gridIndex.selectGrid(sqlTerms,arrayOperators);
            }
            else{
                itr = table.selectLinear(sqlTerms[0]._strTableName,sqlTerms,arrayOperators);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return itr;
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

    public void placeCells(GridIndex gridIndex,Table table,ArrayList indices){

        Set<PageInfo> pagesInfosSet = table.pages.keySet();
        ArrayList<PageInfo> pagesInfos = new ArrayList<>(pagesInfosSet);
        Collections.sort((List) pagesInfos);
        for(PageInfo pageInfo: pagesInfos){
            Page page = table.deserializePage(table.pages.get(pageInfo));
            for (Row row : page.rows) {
                gridIndex.insertGrid(row,table.pages.get(pageInfo),indices,page.rows.indexOf(row));
            }
//            table.serializePage(page,pageInfo.getPageNum());
        }
    }

}
