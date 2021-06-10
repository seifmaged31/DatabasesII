import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Validators {


    public void validateCreateTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException, IOException {
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

        Boolean same = false;
        try {
            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;
            while ((nextRecord = reader.readNext()) != null) {
                if(nextRecord[0].equals(tableName))
                {
                    same=true;
                    break;
                }
            }

        }
        catch(Exception e){
                e.printStackTrace();
        }
        if(same)
            throw new DBAppException("This table name already exists");



    }
    public void validateTypesInsertion(Hashtable<String, Object> colNameValue) throws DBAppException{
           Set<String> colNames = colNameValue.keySet();
           for(String type:colNames){

                if(type.toLowerCase().equals("java.lang.integer") && !(colNameValue.get(type) instanceof Integer)) {
                    throw new DBAppException("Incorrect data type");
                }
                if(type.toLowerCase().equals("java.lang.double") && !(colNameValue.get(type) instanceof Double)) {
                    throw new DBAppException("Incorrect data type");
                }
                if(type.toLowerCase().equals("java.lang.string") && !(colNameValue.get(type) instanceof String)) {
                    throw new DBAppException("Incorrect data type");
                }
                if(type.toLowerCase().equals("java.util.date") && !(colNameValue.get(type) instanceof Date)) {
                    throw new DBAppException("Incorrect data type");
                }
            }

    }
    public void validateTypesTable(String type, String value) throws DBAppException{
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

        if (type.toLowerCase().equals("java.lang.double")) {
            try {
                Double.parseDouble(value);
            } catch (NumberFormatException e) {
                exception = true;
            }
            if (exception)
                throw new DBAppException("Incorrect data type");
        }
        if (type.toLowerCase().equals("java.util.date")) {
            try {
                (new SimpleDateFormat("yyyy-MM-dd")).parse(value);
            } catch (ParseException e) {
                exception = true;
            }

            if (exception)
                throw new DBAppException("Incorrect data type");
        }

    }
    public void validateColNames(String tableName, Hashtable<String, Object> colNameValue) throws IOException, DBAppException {
        CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
        String[] nextRecord;
        Set<String> set = colNameValue.keySet();
        ArrayList<String> colNames = new ArrayList<>(set);
        ArrayList<String> csvColNames = new ArrayList<>();
        while ((nextRecord = reader.readNext()) != null) {
            if(nextRecord[0].equals(tableName)){
                csvColNames.add(nextRecord[1]);
            }
        }
        if(!(csvColNames.containsAll(colNames))){
            throw new DBAppException("The column names do not match");
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

            e.printStackTrace();
        }
            if(!found)
                throw new DBAppException("The clustering key doesn't exist.");
    }
    public void validateInsertion (String tableName, Hashtable<String, Object> colNameValue) throws DBAppException{
        Set<String> keys = colNameValue.keySet();
        ArrayList<String> columns =  new ArrayList<>(keys);
        int i=0;
        boolean type=false;
        try {

            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;

            while ((nextRecord = reader.readNext()) != null && i<columns.size()) {

                if(nextRecord[0].equals(tableName)) {
                    if (!nextRecord[1].equals(columns.get(i))) {
                        colNameValue.put(nextRecord[1], "null");
                        continue;
                    } else {
                        if (!(((colNameValue.get(columns.get(i))).getClass()).getTypeName().toLowerCase()).equals(nextRecord[2].toLowerCase())) {

                            type = true;

                        }
                    }

                    i++;
                }


            }

        }
        catch(Exception e){

            e.printStackTrace();
        }

        if(type)
            throw new DBAppException("A value entered for a column does not match the required data type.");
        else
            System.out.println("we made it");

    }
    public void validateUpdate (String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        Set<String> colNames = columnNameValue.keySet();
        Iterator<String> itr = colNames.iterator();
        String cur= itr.next();
        boolean found=false;
        boolean columns=false;
        try {

            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;
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

                else{
                    if (nextRecord[1].equals(cur))
                        if(itr.hasNext())
                            cur=itr.next();
                        else
                        {
                            columns=true;
                            break;
                        }

                    }
            }

        }
        catch(Exception e){

            e.printStackTrace();
        }
        if(found)
            throw new DBAppException("Cannot update the clustering key.");
        if(!columns)
            throw new DBAppException("These columns are invalid and do not exist in the table.");

    }
    public String getClusteringType(String tableName){
        try {

            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;
            while ((nextRecord = reader.readNext()) != null) {
                if(nextRecord[0].equals(tableName) && nextRecord[3].equals("True")){
                    return nextRecord[2].toLowerCase();
                }

            }

        }
        catch(Exception e){

            e.printStackTrace();
        }
        return "";
    }
    public Object getClusteringValue (String type, String value) throws ParseException {

        switch (type.toLowerCase())
        {
            case "java.lang.integer":return (int)(Integer.parseInt(value));
            case "java.lang.double":return (Double)(Double.parseDouble(value));
            case "java.util.date":return (Date)((new SimpleDateFormat("yyyy-MM-dd")).parse(value));
            default: return (String) value;

        }

    }
    public void validateRange(String tableName,Hashtable<String, Object> colNameValue) throws IOException, DBAppException {

        boolean error=false;
        try {

            CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
            String[] nextRecord;
            int i=0;
            ArrayList keys = new ArrayList(colNameValue.keySet());
            while ((nextRecord = reader.readNext()) != null && i<keys.size()) {
                if(nextRecord[0].equals(tableName)){
                    if(nextRecord[1].equals(keys.get(i))){

                        Object value = colNameValue.get(keys.get(i));
                        Object min = getClusteringValue(nextRecord[2],nextRecord[5]);
                        Object max = getClusteringValue(nextRecord[2],nextRecord[6]);

                        if(!comparison(value,min,max)){
                            System.out.println(value);
                            System.out.println(min);
                            System.out.println(max);
                            error=true;
                            break;
                        }
                        i++;
                    }

                }

            }

        }
        catch(Exception e){

            e.printStackTrace();
        }
        if(error)
            throw new DBAppException("value(s) out of range");
    }
    public boolean comparison(Object target,Object min,Object max){
        if(target instanceof Integer){
            return ((Integer) target).compareTo((Integer) min)>=0 && ((Integer) target).compareTo((Integer) max)<=0 ;
        }
        if(target instanceof String){
            return ((String) target).compareTo((String) min)>=0 && ((String) target).compareTo((String) max)<=0 ;
        }
        if(target instanceof Date){
            return ((Date) target).compareTo((Date) min)>=0 && ((Date) target).compareTo((Date) max)<=0 ;
        }
        return ((Double) target).compareTo((Double) min)>=0 && ((Double) target).compareTo((Double) max)<=0 ;

    }

}
