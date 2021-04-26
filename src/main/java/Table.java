import java.io.*;
import java.util.*;

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
            createPage(row);
            return;
        }
        Set<PageInfo> pagesInfosSet = pages.keySet();
        ArrayList<PageInfo> pagesInfos = new ArrayList<PageInfo>(pagesInfosSet);
        Collections.sort((List)pagesInfos);
        for(PageInfo pageInfo:pagesInfos){
            if(!pagesInfos.iterator().hasNext()){//this is the last page
                if(!pageInfo.isFull()){ // I have space to insert in this page
                    Page page = deserializePage(pages.get(pageInfo));
                    page.insert(row);
                    this.updatePageInfo(pageInfo,row);
                    serializePage(page,pageInfo.getPageNum());
                    return;
                }
                else { // I have no space to insert so, move

                    if(checkRange(row, pageInfo.getMin(), pageInfo.getMax())){ //the new row is within the page
                        Page page = deserializePage(pages.get(pageInfo));
                        Row lastElement = page.rows.lastElement();
                        page.rows.removeElementAt(page.rows.size()-1);
                        page.insert(row);
                        this.updatePageInfo(pageInfo, row);
                        serializePage(page, pageInfo.getPageNum());
                        createPage(lastElement);
                        return;

                    }
                    else{ // the new row isn't in the range
                        createPage(row);
                        return;
                    }
                }


            }
            int currentIndex =  ((List<?>) pagesInfos).indexOf(pageInfo);
            PageInfo nextPageInfo =(PageInfo) ((List<?>) pagesInfos).get(currentIndex+1);
            if(checkRange(row,pageInfo.getMin(),pageInfo.getMax()) || checkRange(row,pageInfo.getMax(),nextPageInfo.getMin())){ // any intermediate page that I need.
                    if(!pageInfo.isFull()) { // if the page has space
                        Page page = deserializePage(pages.get(pageInfo));
                        page.insert(row);
                        this.updatePageInfo(pageInfo, row);
                        serializePage(page, pageInfo.getPageNum());
                        return;
                    }
                    else{
                        if(!nextPageInfo.isFull()){ //shifting to the next page

                            Page page = deserializePage(pages.get(pageInfo));
                            Row lastElement = page.rows.lastElement();
                            page.rows.removeElementAt(page.rows.size()-1);
                            page.insert(row);
                            this.updatePageInfo(pageInfo, row);
                            serializePage(page, pageInfo.getPageNum());
                            Page nextPage= deserializePage(pages.get(nextPageInfo));
                            nextPage.insert(lastElement);
                            this.updatePageInfo(nextPageInfo, lastElement);
                            serializePage(nextPage, nextPageInfo.getPageNum());

                            return;


                        }
                        else{ // insert in overflow pages
                            Hashtable overflowPages = pageInfo.getOverflowPages();
                            Set pagesOverflowInfos = overflowPages.keySet();
                            if(pagesOverflowInfos.size() == 0){ // we don't have overflow pages
                                createOverflowPage(row,pageInfo);
                                return;
                            }
                            else{ // we have overflow pages

                            }
                        }

                    }
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

    public boolean checkRange (Row row, Row min, Row max){
        return row.compareTo(min)>=0 && row.compareTo(max)<=0;
    }

    public void createPage(Row row){
        Page page = new Page(row);
        PageInfo info = new PageInfo(row);
        this.pageNum++;
        info.setPageNum(this.pageNum);
        serializePage(page, this.pageNum);
        pages.put(info, "src/main/resources/Data/" + this.tableName + "_" + this.pageNum + ".class");
    }
    public void createOverflowPage(Row row,PageInfo mainPageInfo){
        Page overflowPage = new Page(row);
        PageInfo overflowInfo = new PageInfo(row);
        mainPageInfo.setOverflowNum(mainPageInfo.getOverflowNum()+1);
        overflowInfo.setPageNum(this.pageNum);
        serializePage(overflowPage, this.pageNum);
        mainPageInfo.getOverflowPages().put(overflowInfo, "src/main/resources/Data/" + this.tableName + "_" + this.pageNum +"_"+ overflowInfo.getPageNum() +".class");
    }

}
