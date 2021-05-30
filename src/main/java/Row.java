import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
    public void addRecord (ArrayList indices, Hashtable<String, Object> columnNameStatement){ //[1,2]
             //keys: [name]

        ArrayList<String> keys = new ArrayList<String>(columnNameStatement.keySet());// indices: [2]

        Object rowValue="";
        for(String key:keys){
            String value= this.values.get((int)indices.get(keys.indexOf(key)));
            try{
                rowValue = getValue(value,((Statement)columnNameStatement.get(key))._objValue);
            }
            catch (ParseException e){

            }

            Object comparedValue = ((Statement)columnNameStatement.get(key))._objValue;
            switch(((Statement)columnNameStatement.get(key))._strOperator){
                case "=": if(compareObject(rowValue,comparedValue)==0){
                    ((Statement)columnNameStatement.get(key)).results.add(this);break;
                }
                case ">": if(compareObject(rowValue,comparedValue)>0){
                    ((Statement)columnNameStatement.get(key)).results.add(this);break;
                }
                case "<": if(compareObject(rowValue,comparedValue)<0){
                    ((Statement)columnNameStatement.get(key)).results.add(this);break;
                }
                case ">=": if(compareObject(rowValue,comparedValue)>=0){
                    ((Statement)columnNameStatement.get(key)).results.add(this);break;
                }
                case "<=": if(compareObject(rowValue,comparedValue)<=0){
                    ((Statement)columnNameStatement.get(key)).results.add(this);break;
                }
                default: if(compareObject(rowValue,comparedValue)!=0){
                    ((Statement)columnNameStatement.get(key)).results.add(this);break;
                }
            }
        }

    }


    public void setKeyValue(Object keyValue) {
        this.keyValue = keyValue;
    }

    public Object getValue(String value,Object comparingValue) throws ParseException {
        if(comparingValue instanceof Integer)
            return Integer.parseInt(value);
        if(comparingValue instanceof Double)
            return Double.parseDouble(value);
        if(comparingValue instanceof String)
            return value;
        return (new SimpleDateFormat("yyyy-MM-dd")).parse(value);

    }

    public int compareObject(Object value1,Object value2) {
        if(value1 instanceof Integer)
            return ((Integer) value1).compareTo((Integer) value2);
        if(value1 instanceof Double)
            return ((Double) value1).compareTo((Double) value2);
        if(value1 instanceof String)
            return ((String) value1).compareTo((String) value2);
        return ((Date) value1).compareTo((Date) value2);

    }




    public static void main(String [] args)
    {

    }



}
