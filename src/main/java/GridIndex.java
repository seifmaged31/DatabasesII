import com.opencsv.CSVReader;

import java.io.*;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Arrays.*;

public class GridIndex implements Serializable {
    String tableName;
    ArrayList<ArrayList<Index>> allIndexes;
    String[] columnNames;
    //ArrayList<Range> ranges;
    ArrayList<Range> columnsRange;
    ArrayList indices;

    public GridIndex(String tableName,String[] columnNames,ArrayList<Range> columnsRange, ArrayList indices) throws IOException {

        this.columnNames=columnNames;
        this.allIndexes=new ArrayList<>();
        this.columnsRange = columnsRange;
        this.indices=indices;
        this.tableName=tableName;
        for(int j=0;j<columnNames.length;j++){

            //String type=getType(columnNames[j]);
            //Range range=getRange(columnNames[j]);//return arrayList of ranges instead , range el column kolo
            //columnsRange.add(range);
            String currName=columnNames[j];
            ArrayList dividedRange = getDividedRange(columnsRange.get(j).type,columnsRange.get(j).min,columnsRange.get(j).max);
            Index currIndex=null;
            ArrayList addedList = new ArrayList<Index>();
            allIndexes.add(addedList);
            for(int i=0;i<10;i++){
                currName=columnNames[j];
                currName+=i;
                currIndex= new Index(currName,columnsRange.get(j).type,dividedRange.get(i));
                addedList.add(currIndex);
//                allIndexes.add(addedList);
//                allIndexes.get(j).add(currIndex);
            }

        }


//        for(int j=allIndexes.size()-1;j>=0;j--){
//
//            for(int i=0;i<10;i++){
//                if(j==allIndexes.size()-1){
//                    //allIndexes.get(j).get(i).setChildIndexList(null);
//                    allIndexes.get(j).get(i).bucketName="";
//
//                }
//                else{
//                    allIndexes.get(j).get(i).setChildIndexList(allIndexes.get(j+1));
//                }
//
//
//            }
//
//        }

    }


    public Range getRange(String columnName) throws IOException {

        Range range = null;
        CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
        String[] nextRecord;
        // we are going to read data line by line
        while ((nextRecord = reader.readNext()) != null) {
            if(nextRecord[0].equals(tableName)){
                if(nextRecord[1].equals(columnName))
                {
                    String type = nextRecord[2].toLowerCase();
                    Object min=null;
                    Object max=null;
                    switch(type){
                        case "java.lang.integer": {
                            min=Integer.parseInt(nextRecord[5]);
                            max=Integer.parseInt(nextRecord[6]);
                            break;
                        }
                        case "java.lang.double": {
                            min=Double.parseDouble(nextRecord[5]);
                            max=Double.parseDouble(nextRecord[6]);
                            break;
                        }
                        case "java.util.date": {

                            try {
                                min=(new SimpleDateFormat("yyyy-MM-dd")).parse(nextRecord[5]);
                                max=(new SimpleDateFormat("yyyy-MM-dd")).parse(nextRecord[6]);
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            break;
                        }
                        default:{
                            min=nextRecord[5];
                            max=nextRecord[6];
                        }
                    }
                    range=new Range(min,max,type);
                    break;
                }

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
//        Table table = Table.deserializeTable(tableName);
//        //Arrays.sort(columns);
 //        String[] actualColumns=null;
//        boolean found = false;
//        System.out.println(table.gridIndexNames.size());
//        for (String[] gridIndexName : table.gridIndexNames) {
//            //Arrays.sort(gridIndexName);
//
//            if(Arrays.equals(columns, gridIndexName)) {
//                found = true;
//                actualColumns=gridIndexName;
//                break;
//            }
//
//        }
//        //case partial queries
//        if (found) {
            String path="src/main/resources/data/" + tableName;
            for (int i=0;i<columns.length;i++){
                path+="_"+columns[i];
            }
            path+=".class";
            System.out.println(path);
            try {
                System.out.println("in try");
                FileInputStream fileIn =
                        new FileInputStream(path);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                result = (GridIndex) in.readObject();
                System.out.println("????");
                in.close();
                fileIn.close();

            } catch (FileNotFoundException | ClassNotFoundException e) {
                //e.printStackTrace();
                System.out.println("Grid Index is null.");
            } catch (IOException e) {
                //e.printStackTrace();
                System.out.println("Grid Index is null.");

            }

//        }
        return result;
    }

    public ArrayList getDividedRange(String type,Object min,Object max){
        switch(type){
            case "java.lang.integer" : return createRangeOnInt((int)min,(int)max);
            case "java.lang.double" : return createRangeOnDouble((Double) min,(Double) max);
            case "java.lang.string" : return createRangeOnString((String) min,(String) max);
            default : return createRangeOnDate((Date) min,(Date) max);
        }

    }

    public static ArrayList<Integer> createRangeOnInt(int minVal,int maxVal){

        int range=maxVal-minVal;
        int increment=(range/10)+1;
        ArrayList ranges= new ArrayList(10);
        for(int i=0;i<10;i++) {
            minVal+=increment;
            if(minVal<=maxVal)
             ranges.add(minVal);
            else
                ranges.add(maxVal);
            System.out.println(ranges.get(i));
        }

        return ranges;
    }

    public static ArrayList<Double> createRangeOnDouble(double minVal,double maxVal){ // Ahmood 3amal dih

        double range=maxVal-minVal;
        double increment=(range/10.0);
        ArrayList ranges= new ArrayList(10);
        for(int i=0;i<10;i++) {

            minVal+=increment;
            ranges.add(minVal);
            System.out.println(ranges.get(i));
        }
        ranges.set(ranges.size()-1, maxVal);
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
    public static ArrayList<String> createRangeOnString (String minVal, String maxVal){/*
        int range = maxVal.length() - minVal.length();
        ArrayList<String> temp = new ArrayList<>();//temp to store all permutations
        ArrayList<String> res = new ArrayList<>();
        for(int i = 0; i<range+1; i++){
            temp.addAll(createAllStrings(minVal.length() +i));
        }
        //System.out.print(temp);
        range = temp.size() / 10;
        for(int i =0; i<9; i++){
            res.add(temp.get(range*(i+1)));
        }
        res.add(maxVal);
        return res;*/
        int range = 0;
        int iterations = maxVal.length() - minVal.length();
        String startValue = minVal;
        ArrayList<String> res = new ArrayList<>();
        for(int i=0; i<iterations+1; i++)
            range+=26^minVal.length()+i;


        for (int i=0; i<9; i++){
            for(int j=0; j<range/10; j++){
                startValue = incrementString(startValue);
            }
            res.add(startValue);
        }
        res.add(maxVal);
        return res;
    }


    public static String incrementString(String input){
        if(input.isBlank())
            return "a";
        input = input.toLowerCase();
        int index=input.length()-1;
        StringBuilder modifiedInput = new StringBuilder(input);
        String result = "";
        while(input.charAt(index) == 'z'){
            if(index==0){
                for(int i=0;i<=input.length();i++)
                {
                    result+='a';
                }
                return result;
            }

            modifiedInput.setCharAt(index, 'a');
            index--;
        }

        modifiedInput.setCharAt(index, (char) (input.charAt(index)+1));
        result = modifiedInput.toString();
        return result;

    }

    public static ArrayList<String> createAllStrings(int n){

        double total= Math.pow(26.0,n);
        int segment=(int) total/10;
        ArrayList<String> generated=appendLetters("");
        ArrayList<String> current=new ArrayList<>();
        ArrayList<String> result=new ArrayList<>();
        String base="";
        //generated=appendLetters("");

        if(n==1){
            return generated;
        }

        for(int i=0;i<generated.size();i++){

            current=appendLetters(generated.get(i));
            if(current.get(0).length()==n)
                result.addAll(current);

            if(current.get(0).length()>n)
                break;
            generated.addAll(current);


        }

   return result;
    }


    public static ArrayList<String> appendLetters(String base){

        ArrayList<String> generated=new ArrayList<>();

        for (char added = 'a'; added <= 'z'; added++) {
            generated.add(base+added);
        }
        return generated;
    }
    /*

            for() generate all possible strings --> ArrayList  230
            230/10=23
            for()-->

     */
   public void insertGrid(Row row,String path,ArrayList indices, int rowNum ){

        Vector<String> keyPointerValues = new Vector();
        for (int i=0;i<indices.size();i++){
            keyPointerValues.add(row.values.get((int) indices.get(i)));
        }
        KeyPointerPair keyPointerPair= new KeyPointerPair(keyPointerValues,path, rowNum);
        ArrayList result = new ArrayList();
        for(ArrayList<Index> column:allIndexes){

            // go to the bucket
            Object rowValue = null;
            //System.out.println(allIndexes + "\n" + allIndexes.size());
            //System.out.println(column);
            String curRowValue = row.values.get((int) indices.get(this.allIndexes.indexOf(column)));
            int count=0;
            try {
                count = allIndexes.indexOf(column);
                rowValue = getValue(curRowValue, columnsRange.get(count).type);
            } catch (ParseException e) {
                e.printStackTrace();
            }
//            //ArrayList dividedRanges = new ArrayList();
            for(Index index:column) {
                if( row.compareObject(rowValue,index.maxValue)<=0)
                {
                  result.add(column.indexOf(index)); // [0,4,3]   table_id_age_name_0_4_3.class
                  break;
                }

            }

        }
        String bucketPath = "src/main/resources/data/" + tableName;
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
               Bucket bucketVariable = bucket;
               if(bucket.isFull()){
                   //Bucket  = bucket.next;
                   //bucket.addBucket(keyPointerPair); // think about overflow
                   while (bucketVariable.next!=null){
                       bucketVariable=bucketVariable.next;
                       if(!bucketVariable.isFull()){
                           bucketVariable.insert(keyPointerPair);
                           bucket.serializeBucket(bucketPath);
                           return;
                       }
                   }
                   bucketVariable.addBucket(keyPointerPair);
                   bucket.serializeBucket(bucketPath);
                   return;
               }
               else{
                   bucket.insert(keyPointerPair);
                   bucket.serializeBucket(bucketPath);
                   return;
               }

           }
           catch(Exception e){
               e.printStackTrace();
           }
       }
       else{
           Bucket bucket = new Bucket(keyPointerPair);
           bucket.serializeBucket(bucketPath);
           return;

       }


   }

   public void updateGrid(Row row, ArrayList indices, Hashtable<String,Object> newValues){
       Vector<String> keyPointerValues = new Vector();
       for (int i=0;i<indices.size();i++){
           keyPointerValues.add(row.values.get((int) indices.get(i)));
       }
       KeyPointerPair keyPointerPair= new KeyPointerPair(keyPointerValues,"", 0);
       ArrayList result = new ArrayList();
       for(ArrayList<Index> column:allIndexes){

           // go to the bucket
           Object rowValue = null;
           //System.out.println(allIndexes + "\n" + allIndexes.size());
           //System.out.println(column);
           String curRowValue = row.values.get((int) indices.get(this.allIndexes.indexOf(column)));
           int count=0;
           try {
               count = allIndexes.indexOf(column);
               rowValue = getValue(curRowValue, columnsRange.get(count).type);
           } catch (ParseException e) {
               e.printStackTrace();
           }
//            //ArrayList dividedRanges = new ArrayList();
           for(Index index:column) {
               if( row.compareObject(rowValue,index.maxValue)<=0)
               {
                   result.add(column.indexOf(index)); // [0,4,3]   table_id_age_name_0_4_3.class
                   break;
               }

           }

       }
       String bucketPath = "src/main/resources/data" + tableName;
       for (int i=0;i<columnNames.length;i++){
           bucketPath+="_"+columnNames[i];
       }
       for (int i=0;i<result.size();i++){
           bucketPath+="_"+result.get(i);
       }
       bucketPath+=".class";
       File file = new File(bucketPath);
       boolean done = false;
       String newPath=null;
       int newRowNum=0;
       if(file.exists()) {
           Bucket bucket = Bucket.deserializeBucket(bucketPath);
           Table table = Table.deserializeTable(tableName);
           while(bucket!=null && !done){
               for(KeyPointerPair key:bucket.keyPointerPairs){
                   if(key.compareTo(keyPointerPair)==0){
                       newPath=key.pointer;
                       newRowNum=key.rowNum;
                       bucket.keyPointerPairs.remove(key);
                       done=true;
                       break;
                   }
               }
               bucket=bucket.next;
           }
           row.update(indices,newValues);
           insertGrid(row,newPath,this.indices,newRowNum);



       }
   }

   public Iterator selectGrid(SQLTerm[] sqlTerms, String[] arrayOperators){
       System.out.println("grid");

       ArrayList<Statement> statements= new ArrayList<>();
       Hashtable<String, Object> colNameStatement = new Hashtable<>();
       Statement current=null;
       for(SQLTerm sqlTerm : sqlTerms){
           current=new Statement(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._strOperator,sqlTerm._objValue);
           statements.add(current);
           colNameStatement.put(current._strColumnName,current);
       }
       System.out.println(current);
       System.out.println(statements);
       System.out.println(colNameStatement.keySet());
       System.out.println(colNameStatement.values());
       ArrayList <Integer>result = new ArrayList(); // select * from student where id>7 and name=salma
       ArrayList<String> colNamesKeys = new ArrayList<String>(colNameStatement.keySet());// indices: [2]
       for(ArrayList<Index> column:allIndexes){
           // go to the bucket
           Object comparison = null;
           //String curRowValue = keyPointerValues.get(this.allIndexes.indexOf(column));
           int count=0;
           try {
               count = allIndexes.indexOf(column);
               Statement currStatement = (Statement) colNameStatement.get(colNamesKeys.get(count));
               System.out.println(currStatement);
               comparison= getValue(currStatement._objValue.toString(), columnsRange.get(count).type);
               System.out.println(comparison);
           } catch (ParseException e) {
               //e.printStackTrace();
           }
           for(Index index:column) {
               if( Row.compareObject(comparison,index.maxValue)<=0)
               {
                   result.add(column.indexOf(index)); //
                   break;
               }

           }

       }
       String bucketPath = "src/main/resources/data" + tableName;
       for (int i=0;i<columnNames.length;i++){
           bucketPath+="_"+columnNames[i];
       }
       for (int i=0;i<result.size();i++){
           bucketPath+="_"+result.get(i);
       }
       bucketPath+=".class";
       System.out.println(bucketPath);
       File file = new File(bucketPath);
       if(file.exists()) {
           System.out.println("file exists aywa");
           Bucket bucket = Bucket.deserializeBucket(bucketPath);
           Table table = Table.deserializeTable(tableName);
           while(bucket!=null){
               for(KeyPointerPair key:bucket.keyPointerPairs){
//                   if(key.compareTo(keyPointerPair)==0){
//                       int index = bucket.keyPointerPairs.indexOf(key);
//                       String path = bucket.keyPointerPairs.get(index).pointer;
//                       int rowNum = bucket.keyPointerPairs.get(index).rowNum;
//                       table.deleteGridIndex(path,rowNum);
//                       bucket.keyPointerPairs.remove(key);
//                   }
                   key.addRecord(colNameStatement);
               }
               bucket=bucket.next;
           }
           ArrayList<Statement> resultStatements = new ArrayList(colNameStatement.values());
           ArrayList output = new ArrayList();

           if(arrayOperators.length>0){
               for(int i=0;i<arrayOperators.length;i++){
                   if(i==0){
                       ArrayList <Row> operand1 = (resultStatements.get(0)).results;
                      // System.out.println("operand" + operand1);
                       ArrayList <Row> operand2 = (resultStatements.get(1)).results;
                       //System.out.println(operand2);
                       output= table.checkOperator(operand1,operand2,arrayOperators[0]);
//                       System.out.println("hashcode" + operand1.get(0).hashCode());
//                       System.out.println(operand2.get(0).hashCode());
//                       System.out.println(result);
                       resultStatements.remove(0);
                       if(resultStatements.size()>0)
                           resultStatements.remove(0);
                   }
                   else {
                       ArrayList operand1 = (resultStatements.get(0)).results;
                       result = table.checkOperator(operand1, result, arrayOperators[i]);
                       resultStatements.remove(0);
                   }

               }
           }
           else{
               return Arrays.asList(resultStatements.get(0).results).iterator();
           }

           return Arrays.asList(output).iterator();

       }
        return null;
   }


   public void deleteGrid(String tableName, Hashtable <String,Object> colNameValue){
       Vector<String> keyPointerValues = new Vector();
       ArrayList<String> colNames = new ArrayList(colNameValue.keySet());
       for (int i=0;i<colNames.size();i++){
           keyPointerValues.add(colNameValue.get(colNames.get(i)).toString());
       }
       KeyPointerPair keyPointerPair= new KeyPointerPair(keyPointerValues,"", 0);
       ArrayList <Integer>result = new ArrayList();
       for(ArrayList<Index> column:allIndexes){

           // go to the bucket
           Object rowValue = null;
           String curRowValue = keyPointerValues.get(this.allIndexes.indexOf(column));
           int count=0;
           try {
               count = allIndexes.indexOf(column);
               rowValue = getValue(curRowValue, columnsRange.get(count).type);
           } catch (ParseException e) {
               e.printStackTrace();
           }
           for(Index index:column) {
               if( Row.compareObject(rowValue,index.maxValue)<=0)
               {
                   result.add(column.indexOf(index)); // [0,4] id>7 and name="s"  table_id_name_0_4.class [[0,2],[1,2],[2,2] ... ]
                   break;
               }

           }

       }
       String bucketPath = "src/main/resources/data/" + tableName;
       for (int i=0;i<columnNames.length;i++){
           bucketPath+="_"+columnNames[i];
       }
       for (int i=0;i<result.size();i++){
           bucketPath+="_"+result.get(i);
       }
       bucketPath+=".class";
       File file = new File(bucketPath);
       //KeyPointerPair output=null;
       if(file.exists()) {
           Bucket bucket = Bucket.deserializeBucket(bucketPath);
           Table table = Table.deserializeTable(tableName);
           while(bucket!=null){
               for(KeyPointerPair key:bucket.keyPointerPairs){
                   if(key.compareTo(keyPointerPair)==0){
                       int index = bucket.keyPointerPairs.indexOf(key);
                       String path = bucket.keyPointerPairs.get(index).pointer;
                       int rowNum = bucket.keyPointerPairs.get(index).rowNum;
                       table.deleteGridIndex(path,rowNum);
                       bucket.keyPointerPairs.remove(key);
                   }
               }
               bucket=bucket.next;
           }
           //int index = Collections.binarySearch((List)bucket.keyPointerPairs,keyPointerPair);
           //match record
           //Page page = Table.deserializePage(bucket.keyPointerPairs.get(index).pointer);
           //int rowNum = bucket.keyPointerPairs.get(index).rowNum;
           //page.rows.removeElementAt(rowNum);

       }
//       return null;
   }

   public void updatePathInGrid(Row row,int oldRowNum ,String oldPath,String newPath,int newRowNum){
       Vector<String> keyPointerValues = new Vector();
       for (int i=0;i<indices.size();i++){
           keyPointerValues.add(row.values.get((int) indices.get(i)));
       }
       KeyPointerPair keyPointerPair= new KeyPointerPair(keyPointerValues,oldPath,oldRowNum);
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
               if( row.compareObject(rowValue,index.maxValue)<=0)
               {
                   result.add(column.indexOf(index)); // [0,4,3]   table_id_age_name_0_4_3.class
                   break;
               }

           }
       }
       String bucketPath = "src/main/resources/data" + tableName;
       for (int i=0;i<columnNames.length;i++){
           bucketPath+="_"+columnNames[i];
       }
       for (int i=0;i<result.size();i++){
           bucketPath+="_"+result.get(i);
       }
       bucketPath+=".class";
       Bucket bucket = Bucket.deserializeBucket(bucketPath);
       boolean found =false;
       while (bucket.next!=null && !found){
               bucket=bucket.next;
               found =bucket.updateKeyPointerPairPath(keyPointerPair,newPath,newRowNum);
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
        return (new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH).parse(value));

    }
    public static Date addDay(Date date){
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
        Calendar c = Calendar.getInstance();
        c.setTime(date); // Using today's date
        //c.set(date.getYear(),date.getMonth(),date.getDate());
        c.add(Calendar.DATE, 1);
        String output = sdf.format(c.getTime());
        Date resDate = new Date(output);
        String result=""+(resDate.getYear()+1900)+"-"+(resDate.getMonth()+1)+"-"+resDate.getDate();
        //System.out.println(result);
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
//        String output2 = sdf2.format(resDate);
        Date resDate2 = null;
        try {
            resDate2 = sdf2.parse(result);
        } catch (ParseException e) {
            e.printStackTrace();
        }
       // System.out.println(resDate2);
        return resDate2;
    }

    public static void main(String[] args) {
      System.out.println(incrementString(""));


       /* Date date = null;
        try {
            date =new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH).parse("Fri Mar 06 00:00:00 EET 1998");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(date);*/
//        Date date = new Date(121, 10, 17);
//        System.out.println("before" + date);
//        Date res =  addDay(date);
//        System.out.println("after "+ res);
//       Range r1 = new Range(1,4,"java.lang.integer");
//        Range r2 = new Range(5,8,"java.lang.integer");
//        Range r3 = new Range(9,12,"java.lang.integer");
//        ArrayList array = new ArrayList<>();
//        array.add(r1);
//        array.add(r2);
//        array.add(r3);
//        Range r4 = new Range (6,6,"java.lang.integer");
//        //Collections.sort(array);
//        System.out.println(Collections.binarySearch(array,r4));

//        Bucket b1 = new Bucket(new KeyPointerPair(new Vector<>(),"seif"));
//
//        Bucket b2 = null;
//        try {
//            b2 = b1;
//        } catch (CloneNotSupportedException e) {
//            e.printStackTrace();
//        }
//        Bucket b3 = new Bucket(new KeyPointerPair(new Vector<>(),"donia"));
//
//
//        Bucket b4 = b1;
//        Bucket b5 = new Bucket(new KeyPointerPair(new Vector<>(),"potato"));
//        System.out.println("1: " + b1);
//        System.out.println("4: " + b4);
//        System.out.println("3: " + b3);
//        System.out.println("5: " + b5);
//
//        b4.next=b3;
//        b4=b4.next;
//        b4.next=b5;
//
//        System.out.println(b1.next.next);
//        System.out.println(b1.next);
//        System.out.println("1: " + b1);
//        System.out.println("4: " + b4);
//    System.out.println("3: " + b3);
//    System.out.println("5: " + b5);
        // han run ehna lahza
        /*ArrayList<String> trial1= createRangeOnString("aaaa","zzzz");
        System.out.print(trial1);*/

//        b2.numOfKeys=2;
//        System.out.println(b1.numOfKeys + " " + b2.numOfKeys);


//        ArrayList<String> trial = createAllStrings(2);
//        System.out.println(trial); //ok continue :) cutteee

    }



}
