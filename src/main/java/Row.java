import java.io.Serializable;
import java.util.*;

public class Row implements Comparable, Serializable {
    String clusteringKey;
    private Object keyValue;
    Vector<String> values;

    public Row(String clusteringKey,Hashtable<String,Object> colNameValues){
        values = new Vector<>();
        this.clusteringKey=clusteringKey;
        keyValue= colNameValues.get(clusteringKey);
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
    public void update (ArrayList indices, Hashtable<String, Object> columnNameValue)
    {
        Set<String> keySet = columnNameValue.keySet();
        ArrayList<String> keys = new ArrayList<String>(keySet);
        for (String key:keys){
            this.values.set((int)indices.get(keys.indexOf(key)), columnNameValue.get(key).toString());

        }

    }
    public boolean matchRecord (ArrayList indices, Hashtable<String, Object> columnNameValue){
        Set<String> keySet = columnNameValue.keySet();     //keys: [name]
        ArrayList<String> keys = new ArrayList<String>(keySet);// indices: [2]
        for (String key:keys){
            if(!columnNameValue.get(key).toString().equals(this.values.get((int)indices.get(keys.indexOf(key))))){
                return false;
            }
        }
        return true;
    }

    public void setKeyValue(Object keyValue) {
        this.keyValue = keyValue;
    }




    public static void main(String [] args)
    {

    }



}
