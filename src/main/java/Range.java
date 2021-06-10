import java.io.Serializable;
import java.util.Date;

public class Range implements Comparable, Serializable {

    Object min;
    Object max;
    String type;
    public Range(Object min,Object max,String type){
        this.min=min;
        this.max=max;
        this.type=type;
    }

    @Override

    public int compareTo(Object o) {
        Range range = (Range)o;
        if(this.compareObject(this.min,range.min)<0)
            return -1;
        if(this.compareObject(this.min,range.max)>0)
            return 1;
        return 0;
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
}
