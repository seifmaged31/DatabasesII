import com.opencsv.CSVReader;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Arrays.*;

public class GridIndex implements Serializable {
    String tableName;
    ArrayList<ArrayList<Index>> allIndexes;
    String[] columnNames;
    //ArrayList<Range> ranges;
    ArrayList<Range> columnsRange;

    public GridIndex(String tableName,String[] columnNames,ArrayList<Range> ranges) throws IOException {

        this.columnNames=columnNames;
        allIndexes=new ArrayList<>();
        columnsRange = new ArrayList<>();
        for(int j=0;j<columnNames.length;j++){

            String type=getType(columnNames[j]);
            Range range=getRange(columnNames[j]);//return arrayList of ranges instead , range el column kolo
            columnsRange.add(range);
            String currName=columnNames[j];
            //Arraylist<Range> ranges = divideRange()
            Index currIndex=null;
            for(int i=0;i<10;i++){
                currName=columnNames[j];
                currName+=i;
                //currIndex= new Index(currName,ranges.get(i),type);
                allIndexes.get(j).add(currIndex);
            }

        }

        for(int j=allIndexes.size()-1;j>=0;j--){

            for(int i=0;i<10;i++){
                if(j==allIndexes.size()-1){
                    allIndexes.get(j).get(i).setChildIndexList(null);
                    allIndexes.get(j).get(i).bucketName="";

                }
                else{
                    allIndexes.get(j).get(i).setChildIndexList(allIndexes.get(j+1));
                }


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
        //Arrays.sort(columns);
        String[] actualColumns=null;
        boolean found = false;
        for (String[] gridIndexName : table.gridIndexNames) {
            //Arrays.sort(gridIndexName);
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
    public static ArrayList<Object> createRangeOnString (String minVal, String maxVal){
        return null;

    }
    // []
   public void insertGrid(Row row,String path,ArrayList indices ){

        Vector<String> keyPointerValues = new Vector();
        for (int i=0;i<indices.size();i++){
            keyPointerValues.add(row.values.get((int) indices.get(i)));
        }
        KeyPointerPair keyPointerPair= new KeyPointerPair(keyPointerValues,path);
        ArrayList result = new ArrayList();
        for(ArrayList<Index> column:allIndexes){

        // go to the bucket
            Object rowValue = null;
            String curRowValue = row.values.get((int) indices.get(allIndexes.indexOf(column)));
            int count=0;
            try {
                count = allIndexes.indexOf(column);
                rowValue = getValue(curRowValue, columnsRange.get(count).type);
            } catch (ParseException e) {
                e.printStackTrace();
            }
//            //ArrayList dividedRanges = new ArrayList();
            for(Index index:column) {
                if(row.compareObject(rowValue,index.range.min)>=0 && row.compareObject(rowValue,index.range.max)<=0)
                {
                  result.add(column.indexOf(index)); // [0,4,3]   table_id_age_name_0_4_3.class
                  break;
                }

            }

        }
        String bucketPath = "src/main/resources/" + tableName;
       for (int i=0;i<columnNames.length;i++){
           bucketPath+="_"+columnNames[i];
       }
       for (int i=0;i<result.size();i++){
           bucketPath+="_"+result.get(i);
       }
       bucketPath+=".class";
       File file = new File(bucketPath);
       if(file.exists()){
           Bucket bucket = Bucket.deserializeBucket(bucketPath);
           try{
               if(bucket.isFull()){
                   bucket.addBucket(keyPointerPair); // think about overflow
               }
               else{
                   bucket.insert(keyPointerPair);
                   bucket.serializeBucket(bucketPath);
               }
           }
           catch(Exception e){
               e.printStackTrace();
           }
       }
       else{
           Bucket bucket = new Bucket(keyPointerPair);
           bucket.serializeBucket(bucketPath);

       }


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
    public Object getValue(String value,String type) throws ParseException {

        if(type.equals("java.lang.integer"))
            return Integer.parseInt(value);
        if(type.equals("java.lang.double"))
            return Double.parseDouble(value);
        if(type.equals("java.lang.string"))
            return value;
        return (new SimpleDateFormat("yyyy-MM-dd")).parse(value);

    }
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
       Range r1 = new Range(1,4,"java.lang.integer");
        Range r2 = new Range(5,8,"java.lang.integer");
        Range r3 = new Range(9,12,"java.lang.integer");
        ArrayList array = new ArrayList<>();
        array.add(r1);
        array.add(r2);
        array.add(r3);
        Range r4 = new Range (6,6,"java.lang.integer");
        //Collections.sort(array);
        System.out.println(Collections.binarySearch(array,r4));
    }
}
