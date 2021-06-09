import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KeyPointerPair implements Comparable, Serializable {

    Vector<String> key;
    String pointer;
    int rowNum;
    public KeyPointerPair(Vector<String> key,String pointer, int rowNum){
        this.key=key;
        this.pointer=pointer;
        this.rowNum=rowNum;
    }

    @Override
    public int compareTo(Object o) {
        KeyPointerPair keyPointerPair = (KeyPointerPair)o;
        for(int i=0;i<keyPointerPair.key.size();i++){ // [1,donia,800]  [1,seif,500]
            Object current = getValue(this.key.get(i));
            Object compared = getValue(keyPointerPair.key.get(i));
            if(Row.compareObject(current,compared)==0){
                continue;
            }
            else{
                return Row.compareObject(current,compared);
            }

        }
        return 0;

    }
    public static Object getValue (String string){
        try{
            return (Integer)(Integer.parseInt(string));
        }
        catch(Exception e){

        }
        try{
            return (Double)(Double.parseDouble(string));
        }
        catch(Exception e){

        }
        try{
            return (Date)(new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH).parse(string));
        }
        catch(Exception e){

        }
        return (String)string;

    }
    public void addRecord (Hashtable<String, Object> columnNameStatement){ //[1,2]

        ArrayList<String> keys = new ArrayList<String>(columnNameStatement.keySet());// indices: [2]
        Object rowValue=null;
        for(String key:keys){
            String value= this.key.get(keys.indexOf(key));
            rowValue = getValue(value);
            Object comparedValue = ((Statement)columnNameStatement.get(key))._objValue;
            switch(((Statement)columnNameStatement.get(key))._strOperator){
                case "=": if(Row.compareObject(rowValue,comparedValue)==0)
                    ((Statement)columnNameStatement.get(key)).results.add(getRow(pointer,rowNum));break;

                case ">": if(Row.compareObject(rowValue,comparedValue)>0)
                    ((Statement)columnNameStatement.get(key)).results.add(getRow(pointer,rowNum));break;

                case "<": if(Row.compareObject(rowValue,comparedValue)<0)
                    ((Statement)columnNameStatement.get(key)).results.add(getRow(pointer,rowNum));break;

                case ">=": if(Row.compareObject(rowValue,comparedValue)>=0)
                    ((Statement)columnNameStatement.get(key)).results.add(getRow(pointer,rowNum));break;

                case "<=": if(Row.compareObject(rowValue,comparedValue)<=0)
                    ((Statement)columnNameStatement.get(key)).results.add(getRow(pointer,rowNum));break;

                default: if(Row.compareObject(rowValue,comparedValue)!=0)
                    ((Statement)columnNameStatement.get(key)).results.add(getRow(pointer,rowNum));break;

            }
        }

    }

    public Row getRow(String path,int rowNum){
        Page page = Table.deserializePage(path);
        return page.rows.get(rowNum);
    }

    public static void main(String[] args) {

        Vector<String> v1 = new Vector<String>();
        v1.add("seif");
        v1.add("21");
        v1.add("6");
        Vector<String> v2 = new  Vector<String>();
        v2.add("seif");
        v2.add("21");
        v2.add("0");
        Vector<String> v3 = new  Vector<String>();
        v3.add("seif");
        v3.add("16");
        v3.add("0");
        KeyPointerPair p1 = new KeyPointerPair(v1, "seif",0);
        KeyPointerPair p2 = new KeyPointerPair(v2, "donia",0);
        KeyPointerPair p3 = new KeyPointerPair(v3, "salma",0);

        Bucket b = new Bucket(p1);
        b.insert(p1);
        b.insert(p2);
      //  System.out.println(b.keyPointerPairs);
        int index = Collections.binarySearch((List)b.keyPointerPairs,p3);
        System.out.println(index);
//        for(KeyPointerPair key: b.keyPointerPairs){
//            System.out.println(key.key);
//        }
    }
}
