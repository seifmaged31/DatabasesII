import java.io.Serializable;
import java.util.*;

public class Row implements Comparable, Serializable {
    String clusteringKey;
    Object keyValue;
    Vector<String> values;

    public Row(String clusteringKey,Hashtable<String,Object> colNameValues){
        values = new Vector<>();
        this.clusteringKey=clusteringKey;
        keyValue= colNameValues.get(clusteringKey);

        //values.sort();
        Set<String> nameType = colNameValues.keySet();
        Iterator<String> itrType = nameType.iterator();
        while (itrType.hasNext()) {

            values.add(colNameValues.get(itrType.next()).toString());
        }



    }

    public Object getKeyValue(){
        return keyValue;

    }

    public int compareTo(Object o) {
        Row x= (Row) o;
        if(x.keyValue instanceof Integer){
            return ((Integer) this.keyValue).compareTo((Integer) x.keyValue);
        }
        if(x.keyValue instanceof String){
            return ((String) this.keyValue).compareTo((String) x.keyValue);
        }
        if(x.keyValue instanceof Date){
            return ((Date) this.keyValue).compareTo((Date) x.keyValue);
        }
        else {
            return ((Double) this.keyValue).compareTo((Double) x.keyValue);
        }


    }

    public static void main(String [] args)
    {
        /*Hashtable htblColNameType = new Hashtable( );
        Date d = new Date(1995, 11, 17);
        Date d1 = new Date(1998, 11, 17);
        htblColNameType.put("id", 1);
        htblColNameType.put("date", d1);
        htblColNameType.put("name", "salma");
        htblColNameType.put("gpa", 0.0);


        Hashtable htblColNameType2 = new Hashtable( );
        htblColNameType2.put("id", 2);
        htblColNameType2.put("date", d);
        htblColNameType2.put("name", "seif");
        htblColNameType2.put("gpa", 1.0);

        Row a=new Row("date",htblColNameType);
        Row b=new Row("date",htblColNameType2);

        System.out.println(b.compareTo(a));

        Vector<Row> xx=new Vector<>();

        xx.add(a);
        System.out.println(a);
        xx.add(b);
        System.out.println(b);
        Collections.sort(xx);
        System.out.println(xx.toString());*/
    }



}
