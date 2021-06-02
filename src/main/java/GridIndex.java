import com.opencsv.CSVReader;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Arrays.*;

public class GridIndex implements Serializable {
   // Vector<Index> parentIndexList;
    String tableName;
    ArrayList<ArrayList<Index>> allIndexes;
    String[] columnNames;
    ArrayList<Range> ranges;

    public GridIndex(String tableName,String[] columnNames,ArrayList<Range> ranges) throws IOException {

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
    public void serializeGrid (){
        String path="src/main/resources/data/" + this.tableName;
        for (int i=0;i<columnNames.length;i++){
            path+="_"+columnNames[i];
        }
        path+=".class";
        try{
            FileOutputStream fileOut =
                    new FileOutputStream(new File(path));
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        }
        catch(IOException i){

        }
    }
    public static GridIndex deserializeGrid (String tableName, String[] columns) {
        GridIndex result = null;
        Table table = Table.deserializeTable(tableName);
        Arrays.sort(columns);
        String[] actualColumns=null;
        boolean found = false;
        for (String[] gridIndexName : table.gridIndexNames) {
            Arrays.sort(gridIndexName);
            if(Arrays.equals(columns, gridIndexName)) {
                found = true;
                actualColumns=gridIndexName;
                break;
            }

        }
        //case partial queries
        if (found) {
            String path="src/main/resources/data/" + tableName;
            for (int i=0;i<actualColumns.length;i++){
                path+="_"+actualColumns[i];
            }
            path+=".class";
            try {
                FileInputStream fileIn =
                        new FileInputStream(new File(path));
                ObjectInputStream in = new ObjectInputStream(fileIn);
                result = (GridIndex) in.readObject();
                in.close();
                fileIn.close();

            } catch (FileNotFoundException | ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return result;
    }

    public static ArrayList<Object> createRangeOnInt(int minVal,int maxVal){

        int range=maxVal-minVal;
        int increment=(range/10)+1;
        ArrayList ranges= new ArrayList(10);
        for(int i=0;i<10;i++) {
            minVal+=increment;
            ranges.add(minVal);
            System.out.println(ranges.get(i));
        }

        return ranges;
    }

    public static ArrayList<Object> createRangeOnDouble(double minVal,double maxVal){

        double range=maxVal-minVal;
        double increment=(range/10.0);
        ArrayList ranges= new ArrayList(10);
        for(int i=0;i<10;i++) {
            minVal+=increment;
            ranges.add(minVal);
            System.out.println(ranges.get(i));
        }

        return ranges;
    }
    public static ArrayList<Date> createRangeOnDate (Date minVal, Date maxVal){
        int range = 0;
        ArrayList<Date> res = new ArrayList<>();
        Date upperBound = new Date();
        Date minTemp = minVal;
        boolean flag = true;
        while(flag){
           if(minTemp.before(maxVal)) {
               range++;
               minTemp = addDay(minTemp);

           }
           else
               break;
        }
        //System.out.println(range);
        minTemp = minVal;
        for(int i=0; i<10; i++){
            for(int j=0;j<range/10;j++){
                minTemp = addDay(minTemp);
            }

            res.add(minTemp);
        }
        res.set(res.size() - 1, maxVal);
        return res;
    }
    public static ArrayList<Object> createRangeonString (String minVal, String maxVal){
        return null;

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
    public static Date addDay(Date date){
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
        Calendar c = Calendar.getInstance();
        c.setTime(date); // Using today's date
        c.add(Calendar.DATE, 1);
        String output = sdf.format(c.getTime());
        Date resDate = new Date(output);
        return resDate;
    }

    public static void main(String[] args) {
        Date date1 = new Date(100,1,1);
        Date date2 = new Date(101,6,1);
        System.out.println(createRangeOnDate(date1, date2));
    }
}
