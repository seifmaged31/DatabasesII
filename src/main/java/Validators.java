import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Set;

public class Validators {
    public static void validateCreateTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException, IOException {
        Set<String> nameType = colNameType.keySet();
        Set<String> nameMax = colNameMax.keySet();
        Set<String> nameMin = colNameMin.keySet();
        ArrayList<String> types = new ArrayList();
        types.add("java.lang.integer");
        types.add("java.lang.double");
        types.add("java.lang.date");
        types.add("java.lang.string");
        for(String key:nameType){
            //colNameType.get(key).toLowerCase();
            //System.out.println(key);
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
            }

        }
        catch(Exception e){

        }


    }
}
