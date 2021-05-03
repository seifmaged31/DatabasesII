import com.opencsv.CSVReader;
import java.io.*;
import java.util.*;
//import java.nio.file.*;


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
    public void insert (Row row, String tableName) throws IOException, DBAppException {

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
        ArrayList values = new ArrayList();
        pagesInfos.forEach(info->values.add(info.getMin().getKeyValue()));
        int indexOfPage =Collections.binarySearch(values,row.getKeyValue());
        if(indexOfPage >=0)
            throw new DBAppException("This clustering key already exists.");
        indexOfPage = (indexOfPage==-1)?0:((indexOfPage+2)*-1); // [2, 4, 6 , 7]
        Page page = this.deserializePage(this.pages.get(pagesInfos.get(indexOfPage)));
        PageInfo pageInfo = pagesInfos.get(indexOfPage);
        //for(PageInfo pageInfo:pagesInfos){
            if(pagesInfos.indexOf(pageInfo) == pagesInfos.size()-1){//this is the last page
                if(!pageInfo.isFull()){ // I have space to insert in this page
                    //System.out.println("insertion in last page");
                    page = deserializePage(pages.get(pageInfo));
                    int indexOfRow =Collections.binarySearch((List)page.rows,row);
                    if(indexOfRow>=0)
                        throw new DBAppException("The clustering key already exists");
                    indexOfRow = (indexOfRow==-1)?0:((indexOfRow+1)*-1);
                    page.insert(row,indexOfRow);
                    this.updatePageInfoInsert(pageInfo,row,page);
                    serializePage(page,pageInfo.getPageNum());
                    serializeTable(tableName);
                    //System.out.println("min of this page: "+ pageInfo.getMin().values + "max of this page:" + pageInfo.getMax().values);
                    return;
                }
                else { // I have no space to insert so, move

                    if(checkRange(row, pageInfo.getMin(), pageInfo.getMax()) || row.compareTo(pageInfo.getMin())<0){ //the new row is within the page
                        //System.out.println("insertion in this last page and create new page with last elem");
                        page = deserializePage(pages.get(pageInfo));
                        Row lastElement = page.rows.lastElement();
                        page.rows.removeElementAt(page.rows.size()-1);
                        pageInfo.setNumOfRows(pageInfo.getNumOfRows()-1);
                        //pageInfo.setMax(page.rows.lastElement());
                        int indexOfRow =Collections.binarySearch((List)page.rows,row);
                        if(indexOfRow>=0)
                            throw new DBAppException("The clustering key already exists");
                        indexOfRow = (indexOfRow==-1)?0:((indexOfRow+1)*-1);
                        page.insert(row,indexOfRow);
                        this.updatePageInfoInsert(pageInfo, row,page);
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
            if(checkRange(row,pageInfo.getMin(),pageInfo.getMax()) || (checkRange(row,pageInfo.getMax(),nextPageInfo.getMin())) || row.compareTo(pageInfo.getMin())<0){ // any intermediate page that I need.
//                System.out.println("im within range of this page or smaller than next page or smaller than all the elements in the page" );
//                System.out.println("min of this page: "+ pageInfo.getMin().values + "max of this page:" + pageInfo.getMax().values);
//                System.out.println("min of next page: "+ nextPageInfo.getMin().values + "max of next page:" + nextPageInfo.getMax().values);
                if(!pageInfo.isFull()) { // if the page has space
                        //System.out.println("there is space in this page (within range)");
                        page = deserializePage(pages.get(pageInfo));
                        int indexOfRow =Collections.binarySearch((List)page.rows,row);
                        if(indexOfRow>=0)
                            throw new DBAppException("The clustering key already exists");
                        indexOfRow = (indexOfRow==-1)?0:((indexOfRow+1)*-1);
                        page.insert(row,indexOfRow);
                        this.updatePageInfoInsert(pageInfo, row,page);
                        serializePage(page, pageInfo.getPageNum());
                        serializeTable(tableName);
                        return;
                    }
                    else{ // the page has no space
                       // System.out.println("no space in first page");
                        if(!nextPageInfo.isFull()){ //shifting to the next page, as the next page has space.
                            //System.out.println("there is space in next page");

                            //pageInfo.setMax(page.rows.lastElement());

                            if (checkRange(row,pageInfo.getMax(),nextPageInfo.getMin())){
                                Page nextPage = deserializePage(pages.get(nextPageInfo));
                                int indexOfRow =Collections.binarySearch((List)nextPage.rows,row);
                                if(indexOfRow>=0)
                                    throw new DBAppException("The clustering key already exists");
                                indexOfRow = (indexOfRow==-1)?0:((indexOfRow+1)*-1);
                                nextPage.insert(row,indexOfRow);
                                this.updatePageInfoInsert(nextPageInfo, row, nextPage);
                                serializePage(nextPage, nextPageInfo.getPageNum());
                            }
                            else {
                                page = deserializePage(pages.get(pageInfo));
                                Row lastElement = page.rows.lastElement();
                                page.rows.removeElementAt(page.rows.size()-1);
                                pageInfo.setNumOfRows(pageInfo.getNumOfRows()-1);
                                int indexOfRow =Collections.binarySearch((List)page.rows,row);
                                if(indexOfRow>=0)
                                    throw new DBAppException("The clustering key already exists");
                                indexOfRow = (indexOfRow==-1)?0:((indexOfRow+1)*-1);
                                page.insert(row,indexOfRow);
                                this.updatePageInfoInsert(pageInfo, row, page);
                                serializePage(page, pageInfo.getPageNum());
                                Page nextPage = deserializePage(pages.get(nextPageInfo));
                                indexOfRow =Collections.binarySearch((List)nextPage.rows,row);
                                if(indexOfRow>=0)
                                    throw new DBAppException("The clustering key already exists");
                                indexOfRow = (indexOfRow==-1)?0:((indexOfRow+1)*-1);
                                nextPage.insert(lastElement,indexOfRow);
                                this.updatePageInfoInsert(nextPageInfo, lastElement, nextPage);
                                serializePage(nextPage, nextPageInfo.getPageNum());
                            }
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
                                page = deserializePage(pages.get(pageInfo));
                                Row lastElement = page.rows.lastElement();
                                page.rows.removeElementAt(page.rows.size() - 1);
                                pageInfo.setNumOfRows(pageInfo.getNumOfRows() - 1);
                                //pageInfo.setMax(page.rows.lastElement());
                                int indexOfRow =Collections.binarySearch((List)page.rows,row);
                                if(indexOfRow>=0)
                                    throw new DBAppException("The clustering key already exists");
                                indexOfRow = (indexOfRow==-1)?0:((indexOfRow+1)*-1);
                                page.insert(row,indexOfRow);
                                this.updatePageInfoInsert(pageInfo, row,page);
                                serializePage(page, pageInfo.getPageNum());
                                createPage(lastElement);
                                serializeTable(this.tableName);
                                return;
                            }
                            }
                        }

                    }

            //}
    }




    public void deleteBinary(String tableName, Hashtable<String, Object> columnNameValue, Object clusteringKeyValue,String clusteringKey) throws  DBAppException, IOException

    {

        ArrayList<PageInfo> pagesInfo = new ArrayList<>(this.pages.keySet());
        //System.out.println("size of pagesHashtable: "+ pages.size());
        //System.out.println("size of pagesInfo: "+ pagesInfo.size());
        ArrayList listOfIndices = getIndices(tableName, columnNameValue);
        System.out.println(listOfIndices.toString());
        Collections.sort(pagesInfo);
        ArrayList values = new ArrayList();
        pagesInfo.forEach(info->values.add(info.getMin().getKeyValue()));
        int indexOfPage =Collections.binarySearch(values,clusteringKeyValue);
        System.out.println("The wanted index: " + indexOfPage);
        indexOfPage = (indexOfPage==-1)?0:(indexOfPage<0)?((indexOfPage+2)*-1):indexOfPage;
        PageInfo pageInfo = pagesInfo.get(indexOfPage);
        Page page = this.deserializePage(this.pages.get(pageInfo));
        Row comparisonRow = new Row(clusteringKey,columnNameValue);
        int indexOfRow =Collections.binarySearch((List)page.rows,comparisonRow);
        if(indexOfRow<0)
            throw new DBAppException("No matching record.");
        Row rowToDelete = page.rows.get(indexOfRow);
        if(!rowToDelete.matchRecord(listOfIndices,columnNameValue))
            throw new DBAppException("No matching record.");
        page.delete(rowToDelete);
        updatePageInfoDelete(pageInfo, page);
        serializePage(page,pageInfo.getPageNum());
        //System.out.println("size of pagesInfo: "+ pagesInfo.size());
        System.out.println(pageInfo.isEmpty());
        if(pageInfo.isEmpty()){
                try {
                    //System.out.println("i have entered the try");
                    new FileOutputStream(this.pages.get(pageInfo)).close();
                    //System.out.println("The path to delete: "+this.pages.get(pageInfo));
                    //System.out.println("i am going inside the if");
                    if(new File(this.pages.get(pageInfo)).delete()) {
                        System.out.println("File Deleted");
                    }
                    else {
                        System.out.println("File not Found");
                    }

                } catch (Exception e) {
                    e.getStackTrace();
                }
            this.pages.remove(pageInfo);
            serializeTable(tableName);
            return;
        }
        else{
             serializePage(page, pageInfo.getPageNum());
             serializeTable(tableName);
             return;

        }


    }
    public void deleteLinear(String tableName, Hashtable<String, Object> columnNameValue) throws IOException, DBAppException{

        Boolean found = false;
        Set<PageInfo> pagesInfosSet = pages.keySet();
        ArrayList<PageInfo> pagesInfos = new ArrayList<PageInfo>(pagesInfosSet);
        Collections.sort((List)pagesInfos);
        ArrayList listOfIndices = getIndices(tableName, columnNameValue);
        for(int j = pagesInfos.size()-1;j>=0;j--){
            PageInfo pageInfo = pagesInfos.get(j);
            Page page = deserializePage(this.pages.get(pageInfo));

                for (int i = page.rows.size()-1;i>=0;i--) {
                    if (page.rows.get(i).matchRecord(listOfIndices, columnNameValue)) {
                        found = true;
                        page.delete(page.rows.get(i));
                        updatePageInfoDelete(pageInfo, page);
                        serializePage(page,pageInfo.getPageNum());
                        if (pageInfo.isEmpty()) {
                            try {
//                            System.out.println("i have entered the try");
                                new FileOutputStream(this.pages.get(pageInfo)).close();
//                            System.out.println("The path to delete: "+this.pages.get(pageInfo));
//                            System.out.println("i am going inside the if");
                                if (new File(this.pages.get(pageInfo)).delete()) {
                                    System.out.println("File Deleted");
                                } else {
                                    System.out.println("File not Found");
                                }

                            } catch (Exception e) {
                                e.getStackTrace();
                            }

                            this.pages.remove(pageInfo);
                        }


                    }

            }
            serializePage(page,pageInfo.getPageNum());
        }
        serializeTable(tableName);
        if(!found)
            throw new DBAppException ("No matching record found.");
    }

    public void update(String tableName, Hashtable<String, Object> columnNameValue, Object clusteringKeyValue,String clusteringKey) throws IOException, DBAppException {

        ArrayList<PageInfo> pagesInfo = new ArrayList<>(this.pages.keySet());
    ArrayList listOfIndices = getIndices(tableName, columnNameValue);
    Collections.sort(pagesInfo);
    ArrayList values = new ArrayList();
    pagesInfo.forEach(info->values.add(info.getMin().getKeyValue()));
    int indexOfPage =Collections.binarySearch(values,clusteringKeyValue);
    indexOfPage = (indexOfPage==-1)?0:(indexOfPage<0)?((indexOfPage+2)*-1):indexOfPage; // [2, 4, 6 , 7]
    Page page = this.deserializePage(this.pages.get(pagesInfo.get(indexOfPage)));
    Hashtable<String,Object> tempHash = new Hashtable<>(columnNameValue);//temporary hashtable to have the clustering key as column name
    tempHash.put(clusteringKey,clusteringKeyValue);
    Row comparisonRow = new Row(clusteringKey,tempHash);
    int indexOfRow =Collections.binarySearch((List)page.rows,comparisonRow);
    if(indexOfRow<0)
        throw new DBAppException("There is no record for this value of the primary key.");
    //indexOfRow = (indexOfRow==-1)?0:(indexOfRow<0)?((indexOfRow+2)*-1):indexOfRow;
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
            in.close();
            fileIn.close();

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
            in.close();
            fileIn.close();
        } catch (FileNotFoundException | ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    public  void updatePageInfoInsert(PageInfo pageInfo, Row row, Page page){
        pageInfo.setNumOfRows(pageInfo.getNumOfRows()+1);
        pageInfo.setMax(page.rows.get(page.rows.size()-1));
        pageInfo.setMin(page.rows.get(0));
    }
    public  void updatePageInfoDelete(PageInfo pageInfo,Page page){
        pageInfo.setNumOfRows(pageInfo.getNumOfRows()-1);
        if (page.rows.size()>0){
            pageInfo.setMax(page.rows.get(page.rows.size()-1));
            pageInfo.setMin(page.rows.get(0));
        }
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


    public static void main(String[] args) throws IOException, DBAppException {

        DBApp zeft = new DBApp();

       //Table t1 = deserializeTable("donia");
       Table t1= new Table("donia"); t1.serializeTable(t1.tableName);
        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", 7);
       // htblColNameValue.put("name", "donia");
     // htblColNameValue.put("gpa", 6.0);
       Row r1 = new Row("id",htblColNameValue);
    //  t1.deleteBinary("donia",htblColNameValue,8,"id");
        // t1.deleteLinear("donia",htblColNameValue);
      // t1.insert(r1,"donia");
        zeft.insertIntoTable("donia",htblColNameValue);
      //  t1.update("donia",htblColNameValue,11,"id");
//        System.out.println("Number of Pages: " + t1.pageNum);
          Page p1= (Page) t1.deserializePage("src/main/resources/data/donia_1.class");
//          Page p2= (Page) t1.deserializePage("src/main/resources/data/donia_2.class");
//          Page p3= (Page) t1.deserializePage("src/main/resources/data/donia_3.class");
//          Page p4= (Page) t1.deserializePage("src/main/resources/data/donia_4.class");
        for(Row row: p1.rows){
            System.out.print(row.values + ", " );
        }
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

//        try{
//            FileOutputStream fileOut =
//                    new FileOutputStream(new File("src/main/resources/data/trial.class"));
//            ObjectOutputStream out = new ObjectOutputStream(fileOut);
//            out.close();
//            fileOut.close();
//        }
//        catch(IOException i){
//
//        }
//
//        try
//        {
//            File f= new File("src/main/resources/data/trial.class");
//            if (f.exists()) {
//                f.delete();
//                System.out.println(f.getName() + " is deleted!");
//            } else {
//                System.out.println("Delete operation is failed.");
//            }
//        }
//        catch(Exception e)
//        {
//            e.printStackTrace();
//        }


    }

}
