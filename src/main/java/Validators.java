import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;

/*class CustomizedDate{
    public CustomizedDate(Date date){
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
        String strDate= formatter.format(date);


    }
}*/

public class Validators {
    public static void validateCreateTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException, IOException {
        Set<String> nameType = colNameType.keySet();
        Set<String> nameMax = colNameMax.keySet();
        Set<String> nameMin = colNameMin.keySet();
        ArrayList<String> types = new ArrayList();
        types.add("java.lang.integer");
        types.add("java.lang.double");
        types.add("java.util.date");
        types.add("java.lang.string");
        if(!nameType.contains(clusteringKey)){
            throw new DBAppException("The clustering key doesn't exist.");
        }
        for(String key:nameType){
            if(!types.contains(colNameType.get(key).toLowerCase())){
                throw new DBAppException("Invalid data type.");
            }
        }
        if(nameMin.size() != nameMax.size() || nameMax.size()!= nameType.size() || nameMin.size()!=nameType.size()){
            throw new DBAppException("Missing column names");
        }
        if(!(nameMax.containsAll(nameType) && nameMax.containsAll(nameMin))){
            throw new DBAppException("Incompatible column names.");
        }

        try {
            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;
            // we are going to read data line by line
            while ((nextRecord = reader.readNext()) != null) {
                if(nextRecord[0].equals(tableName))
                    throw new DBAppException("The table name already exists.");
                validateTypesTable(nextRecord[2],nextRecord[5]);
                validateTypesTable(nextRecord[2],nextRecord[6]);
            }

        }
        catch(Exception e){

        }


    }
    public void validateTypesInsertion(String type, Object value) throws DBAppException{

            //System.out.println(value+" I am not an instance of String");
            if(type.toLowerCase().equals("java.lang.integer") && !(value instanceof Integer))
                throw new DBAppException("Incorrect data type");
            if(type.toLowerCase().equals("java.lang.double") && !(value instanceof Double))
                throw new DBAppException("Incorrect data type");
            if(type.toLowerCase().equals("java.lang.string") && !(value instanceof String))
                throw new DBAppException("Incorrect data type");
            if(type.toLowerCase().equals("java.lang.date") && !(value instanceof Date))
                throw new DBAppException("Incorrect data type");


    }
    public void validateTypesTable(String type, String value) throws DBAppException{
        //System.out.println(value + " I am an instance of String");
        boolean exception = false;
        if (type.toLowerCase().equals("java.lang.integer")) {
            try {
                Integer.parseInt(value);
            } catch (NumberFormatException e) {
                exception = true;
            }
            if (exception)
                throw new DBAppException("Incorrect data type");

        }
// Do I need to check on the String ?
        if (type.toLowerCase().equals("java.lang.double")) {
            try {
                Double.parseDouble(value);
            } catch (NumberFormatException e) {
                exception = true;
            }
            if (exception)
                throw new DBAppException("Incorrect data type");
        }
    }

    public void validateClusteringKey(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException{
        Set<String> colNames = colNameValue.keySet();
        boolean found=false;
        try {

            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;
            // we are going to read data line by line
            while ((nextRecord = reader.readNext()) != null) {
                if(nextRecord[0].equals(tableName))
                    if(nextRecord[3].equals("True")){
                        for(String colName: colNames){
                            if(colName.equals(nextRecord[1])){
                                found=true;
                                break;
                            }
                        }
                    }
            }

        }
        catch(Exception e){

            System.out.println("hi");
        }
            if(!found)
                throw new DBAppException("The clustering key doesn't exist.");
            System.out.println("bye");
    }
    public void validateInsertion (String tableName, Hashtable<String, Object> colNameValue) throws DBAppException{
        Set<String> keys = colNameValue.keySet();
        Iterator<String> itr = keys.iterator();

        boolean type=false;
        try {

            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;
            // we are going to read data line by line
            while ((nextRecord = reader.readNext()) != null) {
                //String current =(String) itr.next();
                if(nextRecord[0].equals(tableName)) {
                    if (!nextRecord[1].equals(itr.next())) {
                        colNameValue.put(nextRecord[1], null);
                    } else {
//                        if (!(((colNameValue.get(itr.next())).getClass()).getTypeName()).equals(nextRecord[2])) {
//                            System.out.println(colNameValue.get(itr.next()));
//                            type = true;
//                            break;
//                        }
                        validateTypesInsertion(nextRecord[2],colNameValue.get(itr.next()));
                    }
                }

            }

        }
        catch(Exception e){

            e.printStackTrace();
        }

//        if(type)
//            throw new DBAppException("A value entered for a column does not match the required data type.");

    }

    public static void main(String[] args) throws  DBAppException{
        Hashtable htblColNameValue = new Hashtable( );
        htblColNameValue.put("id", 7);
        htblColNameValue.put("name", "Ahmed");
        htblColNameValue.put("gpa", 0.95 );
        Validators v = new Validators();
        v.validateInsertion("seif", htblColNameValue);
//System.out.println(htblColNameValue.get("id").getClass().getTypeName());
        Set<String>  x= htblColNameValue.keySet();
        for (String y:x){
            System.out.println(y+ " " + htblColNameValue.get(y));
        }

       // System.out.println(Double.parseDouble("1.5"));

//        System.out.println(Date.parse("ab")); IllegalArgumentException , NumberFormatException
//        System.out.println(Integer.parseInt("ab"));
//        CustomizedDate date = new CustomizedDate();
//        Date date = new Date();
//        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
//        String strDate= formatter.format(date);
//        Validators v = new Validators();
          //v.validateTypes("java.lang.String","Ahmed");
//        System.out.println("Done");
    }
}
