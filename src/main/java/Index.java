import java.util.ArrayList;

public class Index {

    String name;
    Object minVal;
    Object maxVal;
    ArrayList<Object> ranges=new ArrayList<Object>();
//[range,,,,,,,,]
/*m in
max**
* Arraylist   salma ya3 awy ekhras ya hayawan
   seif akhooya w rogoola soofy soofy awy
   rageel bas soofy
            donia kaman rogoola w akhooya incest wincest
            * */
    public  Index(String name, Object minVal,Object maxVal){
        this.name = name;
        this.minVal = minVal;
        this.maxVal=maxVal;
        if(minVal instanceof Integer)
        {
            this.ranges=createRangeOnInt((int)minVal,(int)maxVal);
        }
        if(minVal instanceof Double)
        {
            this.ranges=createRangeOnDouble((double)minVal,(double)maxVal);
        }
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




}
