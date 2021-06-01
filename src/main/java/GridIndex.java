import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.Vector;

public class GridIndex {
    Vector<Index> parentIndexList;
    String tableName;
    public GridIndex(String tableName,String[] columnNames) throws IOException {

        for(int j=0;j<columnNames.length;j++){

            String type=getType(columnNames[j]);
            Range range=getRange(columnNames[j]);//return arrayList of ranges instead
            String currName=columnNames[j];
            ArrayList<ArrayList<Index>> allIndexes=new ArrayList<>();
            Index currIndex=null;
            for(int i=0;i<10;i++){
                currName=columnNames[j];
                currName+=i;
                currIndex= new Index(currName,range,type);
                allIndexes.get(j).add(currIndex);

            }

        }

    }


    public Range getRange(String columnName) throws IOException {

        Range range = null;
        CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
        String[] nextRecord;
        // we are going to read data line by line
        while ((nextRecord = reader.readNext()) != null) {
            if(nextRecord[0].equals(tableName)){
                if(nextRecord[1].equals(columnName))
                    range=new Range(nextRecord[5],nextRecord[6]);
                break;
            }
        }
        return range;

    }

    public String getType(String columnName) throws IOException {

        String type = "";
        CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
        String[] nextRecord;
        // we are going to read data line by line
        while ((nextRecord = reader.readNext()) != null) {
            if(nextRecord[0].equals(tableName)){
                if(nextRecord[1].equals(columnName))
                    type=nextRecord[2];
                break;
            }
        }
        return type;

    }

    /*public String incrementString(String original, int increment){

        char[] charArray= original.toCharArray();

        for(int i=increment;i>0;i--){
            for(int j=charArray.length-1;j>=0;j--){

                while(charArray[j]!='z'){
                    charArray[j]+=1;
                    j--;
                }

            }


        }


    }*/

}
