import com.opencsv.CSVReader;

import java.io.*;
import java.util.*;

public class Table implements Serializable{

    String tableName;
    Hashtable<PageInfo,String> pages;
    int pageNum;

    public Table (String tableName){
        this.tableName=tableName;
        pages = new Hashtable<PageInfo, String>();
        //serializeTable(this.tableName);
        pageNum=0;
    }
    public void insert (Row row, String tableName) throws IOException {

        Table table = this.deserializeTable(tableName);
        if (this.pages.isEmpty()){ //first insertion
            //System.out.println("first insertion");
            createPage(row);
            serializeTable(tableName);
            return;
        }
        Set<PageInfo> pagesInfosSet = pages.keySet();
        ArrayList<PageInfo> pagesInfos = new ArrayList<PageInfo>(pagesInfosSet);
        Collections.sort((List)pagesInfos);
        for(PageInfo pageInfo:pagesInfos){
            if(pagesInfos.indexOf(pageInfo) == pagesInfos.size()-1){//this is the last page
                if(!pageInfo.isFull()){ // I have space to insert in this page
                    //System.out.println("insertion in last page");
                    Page page = deserializePage(pages.get(pageInfo));
                    page.insert(row);
                    this.updatePageInfo(pageInfo,row,page);
                    serializePage(page,pageInfo.getPageNum());
                    serializeTable(tableName);
                    //System.out.println("min of this page: "+ pageInfo.getMin().values + "max of this page:" + pageInfo.getMax().values);
                    return;
                }
                else { // I have no space to insert so, move

                    if(checkRange(row, pageInfo.getMin(), pageInfo.getMax()) || row.compareTo(pageInfo.getMin())<0){ //the new row is within the page
                        //System.out.println("insertion in this last page and create new page with last elem");
                        Page page = deserializePage(pages.get(pageInfo));
                        Row lastElement = page.rows.lastElement();
                        page.rows.removeElementAt(page.rows.size()-1);
                        pageInfo.setNumOfRows(pageInfo.getNumOfRows()-1);
                        //pageInfo.setMax(page.rows.lastElement());
                        page.insert(row);
                        this.updatePageInfo(pageInfo, row,page);
                        serializePage(page, pageInfo.getPageNum());
                        createPage(lastElement);
                        serializeTable(tableName);
                        //System.out.println("min of this page: "+ pageInfo.getMin().values + "max of this page:" + pageInfo.getMax().values);
                        return;

                    }
                    else{ // the new row isn't in the range
                        //System.out.println("created new page with row");
                        createPage(row);
                        serializeTable(tableName);
                        return;
                    }
                }


            }
            int currentIndex =  ((List<?>) pagesInfos).indexOf(pageInfo);
            PageInfo nextPageInfo =(PageInfo) ((List<?>) pagesInfos).get(currentIndex+1);
            if(checkRange(row,pageInfo.getMin(),pageInfo.getMax()) || checkRange(row,pageInfo.getMax(),nextPageInfo.getMin()) || row.compareTo(pageInfo.getMin())<0){ // any intermediate page that I need.
//                System.out.println("im within range of this page or smaller than next page or smaller than all the elements in the page" );
//                System.out.println("min of this page: "+ pageInfo.getMin().values + "max of this page:" + pageInfo.getMax().values);
//                System.out.println("min of next page: "+ nextPageInfo.getMin().values + "max of next page:" + nextPageInfo.getMax().values);
                if(!pageInfo.isFull()) { // if the page has space
                        //System.out.println("there is space in this page (within range)");
                        Page page = deserializePage(pages.get(pageInfo));
                        page.insert(row);
                        this.updatePageInfo(pageInfo, row,page);
                        serializePage(page, pageInfo.getPageNum());
                        serializeTable(tableName);
                        return;
                    }
                    else{ // the page has no space
                       // System.out.println("no space in first page");
                        if(!nextPageInfo.isFull()){ //shifting to the next page, as the next page has space.
                            //System.out.println("there is space in next page");
                            Page page = deserializePage(pages.get(pageInfo));
                            Row lastElement = page.rows.lastElement();
                            page.rows.removeElementAt(page.rows.size()-1);
                            pageInfo.setNumOfRows(pageInfo.getNumOfRows()-1);
                            //pageInfo.setMax(page.rows.lastElement());
                            page.insert(row);
                            this.updatePageInfo(pageInfo,row,page);
                            serializePage(page, pageInfo.getPageNum());
                            Page nextPage= deserializePage(pages.get(nextPageInfo));
                            nextPage.insert(lastElement);
                            this.updatePageInfo(nextPageInfo, lastElement,nextPage);
                            serializePage(nextPage, nextPageInfo.getPageNum());
                            serializeTable(tableName);
                            return;
                        }
                        else{
                            if(row.compareTo(pageInfo.getMax())>0){
                                createPage(row);
                                serializeTable(tableName);
                                return;
                            }
                            else {
                                //System.out.println("no space in next page");
                                Page page = deserializePage(pages.get(pageInfo));
                                Row lastElement = page.rows.lastElement();
                                page.rows.removeElementAt(page.rows.size() - 1);
                                pageInfo.setNumOfRows(pageInfo.getNumOfRows() - 1);
                                //pageInfo.setMax(page.rows.lastElement());
                                page.insert(row);
                                this.updatePageInfo(pageInfo, row,page);
                                serializePage(page, pageInfo.getPageNum());
                                createPage(lastElement);
                                serializeTable(this.tableName);
                                return;
                            }
                            }
                        }

                    }
            }
    }




    public void delete(Row row,String tableName){


    }

    public void update(String tableName, Hashtable<String, Object> columnNameValue, Object clusteringKeyValue,String clusteringKey) throws IOException {
    ArrayList<PageInfo> pagesInfo = new ArrayList<>(this.pages.keySet());
    ArrayList listOfIndices = getIndices(tableName, columnNameValue);
    Collections.sort(pagesInfo);
    ArrayList values = new ArrayList();
    pagesInfo.forEach(info->values.add(info.getMin().getKeyValue()));
    int indexOfPage =Collections.binarySearch(values,clusteringKeyValue);
    indexOfPage = (indexOfPage==-1)?0:(indexOfPage<0)?((indexOfPage+2)*-1):indexOfPage;
    Page page = this.deserializePage(this.pages.get(pagesInfo.get(indexOfPage)));
    Hashtable<String,Object> tempHash = new Hashtable<>(columnNameValue);//temporary hashtable to have the clustering key as column name
    tempHash.put(clusteringKey,clusteringKeyValue);
    Row comparisonRow = new Row(clusteringKey,tempHash);
    int indexOfRow =Collections.binarySearch((List)page.rows,comparisonRow);
    indexOfRow = (indexOfRow==-1)?0:(indexOfRow<0)?((indexOfRow+2)*-1):indexOfRow;
    Row rowToUpdate = page.rows.get(indexOfRow);
    rowToUpdate.update(listOfIndices,columnNameValue);
    serializePage(page,pagesInfo.get(indexOfPage).getPageNum());
    serializeTable(tableName);

    }


    public static ArrayList getIndices (String tableName, Hashtable<String, Object> columnNameValue) throws IOException {
        ArrayList list = new ArrayList();
        int c=-1;
        Set<String> keys = columnNameValue.keySet();
        Iterator<String> itr = keys.iterator();
        String cur= itr.next();

        CSVReader reader = new CSVReader((new FileReader(new File("src/main/resources/metadata.csv"))));
        String[] nextRecord;
        while ((nextRecord = reader.readNext()) != null) {
            if(nextRecord[0].equals(tableName)) {
                c++;
                if(nextRecord[1].equals(cur)){
                    list.add(c);
                    if(itr.hasNext())
                        cur=itr.next();
                    else
                        break;
                }

            }

        }


        return list;
    }


    public void serializeTable (String tableName){
        try{
            FileOutputStream fileOut =
                    new FileOutputStream(new File("src/main/resources/data/" + tableName +".class"));
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
                    new FileInputStream(new File("src/main/resources/data/" + tableName +".class"));
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
                    new FileOutputStream(new File("src/main/resources/data/" + this.tableName +"_"+ pageNum +".class"));
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
        }
        catch(IOException i){

        }
    }
    public void serializeOverflow (Page page, int pageNum, int overflow){
        try{
            FileOutputStream fileOut =
                    new FileOutputStream(new File("src/main/resources/data/" + this.tableName +"_"+ pageNum + "_" + overflow + ".class"));
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

    public  void updatePageInfo(PageInfo pageInfo, Row row,Page page){
        //System.out.println(pageInfo.getNumOfRows());
        ArrayList<Row> rows = new ArrayList<>(page.rows);
        pageInfo.setNumOfRows(pageInfo.getNumOfRows()+1);
        //System.out.println(pageInfo.getNumOfRows());
        pageInfo.setMax(rows.get(rows.size()-1));
        pageInfo.setMin(rows.get(0));
//        if(row.compareTo(pageInfo.getMax())>0)
//            pageInfo.setMax(row);
//        if(pageInfo.getMin().compareTo(row)>0)
//            pageInfo.setMin(row);
    }

    public boolean checkRange (Row row, Row min, Row max){
        return row.compareTo(min)>=0 && row.compareTo(max)<=0;
    }

    public void createPage(Row row){
        Page page = new Page(row);
        PageInfo info = new PageInfo(row);
        this.pageNum++;
        info.setPageNum(this.pageNum);
        //System.out.println("min of the created page: "+ info.getMin().values + "max of the created page:" + info.getMax().values);
        serializePage(page, this.pageNum);
        pages.put(info, "src/main/resources/data/" + this.tableName + "_" + this.pageNum + ".class");
    }


    public static void main(String[] args) throws IOException {
//        Table t1 = (Table) deserializeTable("donia");
//        Table t1 = new Table("donia"); t1.serializeTable(t1.tableName);
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("id", 4);
//        htblColNameValue.put("name", "donia");
//        htblColNameValue.put("gpa", 1.5 );
//        Row r1 = new Row("id",htblColNameValue);
//        t1.insert(r1,"donia");
//        System.out.println("Number of Pages: " + t1.pageNum);
//        Page p1= (Page) t1.deserializePage("src/main/resources/data/donia_1.class");
//        Page p2= (Page) t1.deserializePage("src/main/resources/data/donia_2.class");
//        Page p3= (Page) t1.deserializePage("src/main/resources/data/donia_3.class");
//        Page p4= (Page) t1.deserializePage("src/main/resources/data/donia_4.class");
//        for(Row row: p1.rows){
//            System.out.print(row.values + ", " );
//        }
//        System.out.println();
//        for(Row row: p2.rows){
//            System.out.print(row.values + ", " );
//        }
//        System.out.println();
//        for(Row row: p3.rows){
//            System.out.print(row.values + ", " );
//        }
//        System.out.println();
//        for(Row row: p4.rows){
//            System.out.print(row.values + ", " );
//        }


    }

}
