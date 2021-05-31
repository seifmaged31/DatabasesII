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


    public void setChildIndexList(ArrayList<Index> childIndexList){

        this.childIndexList=childIndexList;

    }

    public ArrayList<Index> getNestedList(Index child){

        return child.childIndexList;
    }


}
