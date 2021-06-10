import java.io.Serializable;

public class Index implements Serializable {

    String name;
    String type;
    Object maxValue;
    String bucketName;

    public  Index(String name,String type,Object maxValue){
        this.name=name;
        this.type=type;
        this.maxValue=maxValue;
    }


}
