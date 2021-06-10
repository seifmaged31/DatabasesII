import java.io.Serializable;
import java.util.ArrayList;

public class Index {

    String name;
    String type;
    Object maxValue;
    //ArrayList<Index> childIndexList=new ArrayList();
    String bucketName;

    public  Index(String name,String type,Object maxValue){
        this.name=name;
        this.type=type;
        this.maxValue=maxValue;
    }


//    public void setChildIndexList(ArrayList<Index> childIndexList){
//
//        this.childIndexList=childIndexList;
//
//    }
//
//    public ArrayList<Index> getNestedList(Index child){
//
//        return child.childIndexList;
//    }
//    public boolean hasChild (){
//        if(this.childIndexList!=null)
//            return true;
//        return false;
//    }


}
