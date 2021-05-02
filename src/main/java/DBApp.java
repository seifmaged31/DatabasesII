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
        Object value = validator.getClusteringValue(validator.getClusteringType(tableName),clusteringKeyValue);
        Table table = Table.deserializeTable(tableName);
        String clusteringKey = getClusteringKey(tableName);
        table.update(tableName,columnNameValue, value,clusteringKey);

    }

    public void deleteFromTable(String tableName, Hashtable<String , Object> columnNameValue) throws DBAppException {
        // following method could be used to delete one or more rows.
        // htblColNameValue holds the key and value. This will be used in search
        // to identify which rows/tuples to delete.
        //htblColNameValue enteries are ANDED together

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
//    public static ArrayList getIndices (String tableName, Hashtable<String, Object> columnNameValue) throws IOException {
//        ArrayList list = new ArrayList();
//        int c=-1;
//        Set<String> keys = columnNameValue.keySet();
//        Iterator<String> itr = keys.iterator();
//        String cur= itr.next();
//
//        CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
//        String[] nextRecord;
//        while ((nextRecord = reader.readNext()) != null) {
//            if(nextRecord[0].equals(tableName)) {
//                c++;
//                if(nextRecord[1].equals(cur)){
//                    list.add(c);
//                    if(itr.hasNext())
//                        cur=itr.next();
//                    else
//                        break;
//                }
//
//            }
//
//        }
//
//
//        return list;
//    }



    public static void main(String[] args) throws  Exception{

//        String strTableName = "donia";
        DBApp dbApp = new DBApp();
        dbApp.init();
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

//        Hashtable htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 5 ));
//        htblColNameValue.put("name", new String("Ahmed Noor" ) );
//        htblColNameValue.put("gpa", new Double( 0.95 ) );
//        Hashtable htblColNameValue1 = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 9 ));
//        htblColNameValue.put("name", new String("Ahmed Noor" ) );
//        htblColNameValue.put("gpa", new Double( 0.95 ) );
//        Hashtable htblColNameValue2 = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 1 ));
//        htblColNameValue.put("name", new String("Ahmed Noor" ) );
//        htblColNameValue.put("gpa", new Double( 0.95 ) );
//        Row r = new Row("id", htblColNameValue);
//        Row r1 = new Row("id", htblColNameValue1);
//        Row r2 = new Row("id", htblColNameValue2);



        //   dbApp.insertIntoTable( strTableName , htblColNameValue );

       /* Page table=null;
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
        System.out.println(table);*/
//        Set<PageInfo> x = new HashSet<>();
//        PageInfo p1=new PageInfo(r);
//        p1.setPageNum(8);
//        PageInfo p2=new PageInfo(r);
//        p2.setPageNum(2);
//        PageInfo p3=new PageInfo(r);
//        p3.setPageNum(10);
//        x.add(p1);
//        x.add(p2);
//        x.add(p3);
//
//        ArrayList y = new ArrayList<>(x);
//        Collections.sort(y);
//        ((PageInfo)y.get(0)).setPageNum(5);
//        System.out.println(((PageInfo)y.get(0)).getPageNum() + "    "+ ((PageInfo)y.get(1)).getPageNum() + "   " + ((PageInfo)y.get(2)).getPageNum());
//
//        System.out.println(p2.getPageNum());

//        Hashtable<String, Object> row = new Hashtable();
//        row.put("first_name", "foo");
//        row.put("gpa", 1.1);

        //System.out.println(getIndices("students",row));

    }
}
