import java.io.*;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Set;

public class Table implements Serializable{

    String tableName;
    Hashtable<PageInfo,String> pages;
    int pageNum;

    public Table (String tableName){
        this.tableName=tableName;
        pages = new Hashtable<PageInfo, String>();
        serializeTable(this.tableName);
        pageNum=0;
    }
    public void insert (Row row, String tableName) throws IOException{
       // Table table = this.deserializeTable(tableName);
        if (this.pages.isEmpty()){ //first insertion
            Page page = new Page(row);
            PageInfo info = new PageInfo(row);
            this.pageNum++;
            info.setPageNum(this.pageNum);
            serializePage(page,this.pageNum);
            pages.put(info,"src/main/resources/Data/" + this.tableName +"_"+ this.pageNum +".class" );
            return;
        }
        Set<PageInfo> pagesInfos = pages.keySet();

        /*PageInfo firstInfo =pagesInfos.iterator().next();
        if(pagesInfos.size()==1){
            if(!firstInfo.isFull()){
                Page page = deserializePage(pages.get(firstInfo));
                page.insert(row);
                this.updatePageInfo(firstInfo,row);
                serializePage(page,firstInfo.getPageNum());
                return;
            }

        }*/
        for(PageInfo pageInfo:pagesInfos){
            if(pageInfo.isFull()){

            }
        }


    }

    public void serializeTable (String tableName){
        try{
            FileOutputStream fileOut =
                    new FileOutputStream(new File("src/main/resources/Data/" + tableName +".class"));
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        }
        catch(IOException i){

        }
    }

    public static Table deserializeTable (String tableName){
        Table table=null;
        try{
            FileInputStream fileIn =
                    new FileInputStream(new File("src/main/resources/Data/" + tableName +".class"));
            ObjectInputStream in= new ObjectInputStream(fileIn);
            table = (Table) in.readObject();
        } catch (FileNotFoundException | ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
       return table;
    }

    public void serializePage (Page page, int pageNum){
        try{
            FileOutputStream fileOut =
                    new FileOutputStream(new File("src/main/resources/Data/" + this.tableName +"_"+ pageNum +".class"));
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
        }
        catch(IOException i){

        }
    }
    public Page deserializePage(String path){
        Page page = null;
        try{
            FileInputStream fileIn =
                    new FileInputStream(new File(path));
            ObjectInputStream in= new ObjectInputStream(fileIn);
            page = (Page) in.readObject();
        } catch (FileNotFoundException | ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    public  void updatePageInfo(PageInfo pageInfo, Row row){
        pageInfo.setNumOfRows(pageInfo.getNumOfRows()+1);
        if(row.compareTo(pageInfo.getMax())>0)
            pageInfo.setMax(row);
        if(pageInfo.getMin().compareTo(row)>0)
            pageInfo.setMin(row);
    }



}
