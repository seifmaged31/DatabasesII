import java.util.ArrayList;

public class Index {

    String name;
    String type;
    Range range;
    ArrayList<Index> childIndexList=new ArrayList();
    String bucketName;
    public  Index(String name, Range range,String type){
        this.name=name;
        this.type=type;
        this.range=range;

    }







}
