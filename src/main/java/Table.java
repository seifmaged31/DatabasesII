import com.opencsv.CSVReader;

import java.io.*;
import java.util.*;



public class Table implements Serializable {

    String tableName;
    Hashtable<PageInfo, String> pages;
    Hashtable <String, PageInfo> opposite;
    int pageNum;
    Vector<String[]> gridIndexNames;

    public Table(String tableName) {
        this.tableName = tableName;
        gridIndexNames = new Vector<>();
        pages = new Hashtable<PageInfo, String>();
        opposite = new Hashtable<String, PageInfo>();
        pageNum = 0;
    }

    public void insert(Row row, String tableName,Hashtable<String, Object> colNameValue) throws IOException, DBAppException {


        String[] colNames = new String[colNameValue.size()];
        ArrayList<String> colNamesArrayList = new ArrayList<>(colNameValue.keySet());
        for(String colName: colNamesArrayList)
            colNames[colNamesArrayList.indexOf(colName)]=colName;
        GridIndex gridIndex = GridIndex.deserializeGrid(tableName,colNames);
        if (this.pages.isEmpty()) {
            createPage(row);
            serializeTable(tableName);
            if(gridIndex!=null){
                String path= "src/main/resources/data/" + this.tableName + "_" + this.pageNum + ".class";
                gridIndex.insertGrid(row,path,gridIndex.indices,0);
                gridIndex.serializeGrid();
            }
            return;
        }
        Set<PageInfo> pagesInfosSet = pages.keySet();
        ArrayList<PageInfo> pagesInfos = new ArrayList<PageInfo>(pagesInfosSet);
        Collections.sort((List) pagesInfos);
        ArrayList values = new ArrayList();
        pagesInfos.forEach(info -> values.add(info.getMin().getKeyValue()));
        int indexOfPage = Collections.binarySearch(values, row.getKeyValue());
        if (indexOfPage >= 0)
            throw new DBAppException("This clustering key already exists.");
        indexOfPage = (indexOfPage == -1) ? 0 : ((indexOfPage + 2) * -1);
        Page page = this.deserializePage(this.pages.get(pagesInfos.get(indexOfPage)));
        PageInfo pageInfo = pagesInfos.get(indexOfPage);
        if (pagesInfos.indexOf(pageInfo) == pagesInfos.size() - 1) {
            if (!pageInfo.isFull()) {
                page = deserializePage(pages.get(pageInfo));
                int indexOfRow = Collections.binarySearch((List) page.rows, row);
                if (indexOfRow >= 0)
                    throw new DBAppException("The clustering key already exists");
                indexOfRow = (indexOfRow == -1) ? 0 : ((indexOfRow + 1) * -1);
                page.insert(row, indexOfRow);
                this.updatePageInfoInsert(pageInfo, row, page);
                serializePage(page, pageInfo.getPageNum());
                serializeTable(tableName);
                if(gridIndex!=null){
                    gridIndex.insertGrid(row,pages.get(pageInfo),gridIndex.indices,indexOfRow);
                    gridIndex.serializeGrid();
                }
                return;
            } else {

                if (checkRange(row, pageInfo.getMin(), pageInfo.getMax()) || row.compareTo(pageInfo.getMin()) < 0) { //the new row is within the page
                    page = deserializePage(pages.get(pageInfo));
                    Row lastElement = page.rows.lastElement();
                    page.rows.removeElementAt(page.rows.size() - 1);
                    pageInfo.setNumOfRows(pageInfo.getNumOfRows() - 1);
                    int indexOfRow = Collections.binarySearch((List) page.rows, row);
                    if (indexOfRow >= 0)
                        throw new DBAppException("The clustering key already exists");
                    indexOfRow = (indexOfRow == -1) ? 0 : ((indexOfRow + 1) * -1);
                    page.insert(row, indexOfRow);
                    this.updatePageInfoInsert(pageInfo, row, page);
                    serializePage(page, pageInfo.getPageNum());
                    createPage(lastElement);
                    if(gridIndex!=null){
                        String path= "src/main/resources/data/" + this.tableName + "_" + this.pageNum + ".class";
                        gridIndex.updatePathInGrid(lastElement,page.rows.size()-1,pages.get(pageInfo),path,0);
                        gridIndex.insertGrid(row,pages.get(pageInfo),gridIndex.indices,indexOfRow);
                        gridIndex.serializeGrid();
                    }
                    serializeTable(tableName);
                    return;

                } else {
                    createPage(row);
                    if(gridIndex!=null){
                        String path= "src/main/resources/data/" + this.tableName + "_" + this.pageNum + ".class";
                        gridIndex.insertGrid(row,path,gridIndex.indices,0);
                        gridIndex.serializeGrid();
                    }
                    serializeTable(tableName);
                    return;
                }
            }


        }
        int currentIndex = ((List<?>) pagesInfos).indexOf(pageInfo);
        PageInfo nextPageInfo = (PageInfo) ((List<?>) pagesInfos).get(currentIndex + 1);
        if (checkRange(row, pageInfo.getMin(), pageInfo.getMax()) || (checkRange(row, pageInfo.getMax(), nextPageInfo.getMin())) || row.compareTo(pageInfo.getMin()) < 0) { // any intermediate page that I need.
          if (!pageInfo.isFull()) { // if the page has space
               page = deserializePage(pages.get(pageInfo));
               int indexOfRow = Collections.binarySearch((List) page.rows, row);
               if (indexOfRow >= 0)
                    throw new DBAppException("The clustering key already exists");
                indexOfRow = (indexOfRow == -1) ? 0 : ((indexOfRow + 1) * -1);
                page.insert(row, indexOfRow);
                this.updatePageInfoInsert(pageInfo, row, page);
                if(gridIndex!=null){
                    gridIndex.insertGrid(row,pages.get(pageInfo),gridIndex.indices,indexOfRow);
                    gridIndex.serializeGrid();
                }
                serializePage(page, pageInfo.getPageNum());
                serializeTable(tableName);
                return;
            } else { if (!nextPageInfo.isFull()) {
                if (checkRange(row, pageInfo.getMax(), nextPageInfo.getMin())) {
                        Page nextPage = deserializePage(pages.get(nextPageInfo));
                        int indexOfRow = Collections.binarySearch((List) nextPage.rows, row);
                        if (indexOfRow >= 0)
                            throw new DBAppException("The clustering key already exists");
                        indexOfRow = (indexOfRow == -1) ? 0 : ((indexOfRow + 1) * -1);
                        nextPage.insert(row, indexOfRow);
                        this.updatePageInfoInsert(nextPageInfo, row, nextPage);
                        if(gridIndex!=null){
                            gridIndex.insertGrid(row,pages.get(nextPageInfo),gridIndex.indices,indexOfRow);
                            gridIndex.serializeGrid();
                        }
                        serializePage(nextPage, nextPageInfo.getPageNum());
                    } else {
                        page = deserializePage(pages.get(pageInfo));
                        Row lastElement = page.rows.lastElement();
                        page.rows.removeElementAt(page.rows.size() - 1);
                        pageInfo.setNumOfRows(pageInfo.getNumOfRows() - 1);
                        int indexOfRow = Collections.binarySearch((List) page.rows, row);
                        if (indexOfRow >= 0)
                            throw new DBAppException("The clustering key already exists");
                        indexOfRow = (indexOfRow == -1) ? 0 : ((indexOfRow + 1) * -1);
                        page.insert(row, indexOfRow);
                        this.updatePageInfoInsert(pageInfo, row, page);
                        serializePage(page, pageInfo.getPageNum());
                        if(gridIndex!=null){
                            gridIndex.insertGrid(row,pages.get(pageInfo),gridIndex.indices,indexOfRow);
                            gridIndex.serializeGrid();
                        }
                        Page nextPage = deserializePage(pages.get(nextPageInfo));
                        indexOfRow = Collections.binarySearch((List) nextPage.rows, row);
                        if (indexOfRow >= 0)
                            throw new DBAppException("The clustering key already exists");
                        indexOfRow = (indexOfRow == -1) ? 0 : ((indexOfRow + 1) * -1);
                        nextPage.insert(lastElement, indexOfRow);
                        this.updatePageInfoInsert(nextPageInfo, lastElement, nextPage);
                        if(gridIndex!=null){
                            String path= pages.get(nextPageInfo);
                            gridIndex.updatePathInGrid(lastElement,page.rows.size()-1,pages.get(pageInfo),path,indexOfRow);
                            gridIndex.serializeGrid();
                        }
                        serializePage(nextPage, nextPageInfo.getPageNum());
                    }
                    serializeTable(tableName);
                    return;
                } else {
                    if (row.compareTo(pageInfo.getMax()) > 0) {
                        createPage(row);
                        if(gridIndex!=null){
                            String path= "src/main/resources/data/" + this.tableName + "_" + this.pageNum + ".class";
                            gridIndex.insertGrid(row,path,gridIndex.indices,0);
                            gridIndex.serializeGrid();
                        }
                        serializeTable(tableName);
                        return;
                    } else {
                        page = deserializePage(pages.get(pageInfo));
                        Row lastElement = page.rows.lastElement();
                        page.rows.removeElementAt(page.rows.size() - 1);
                        pageInfo.setNumOfRows(pageInfo.getNumOfRows() - 1);
                        int indexOfRow = Collections.binarySearch((List) page.rows, row);
                        if (indexOfRow >= 0)
                            throw new DBAppException("The clustering key already exists");
                        indexOfRow = (indexOfRow == -1) ? 0 : ((indexOfRow + 1) * -1);
                        page.insert(row, indexOfRow);
                        this.updatePageInfoInsert(pageInfo, row, page);
                        serializePage(page, pageInfo.getPageNum());
                        createPage(lastElement);
                        serializeTable(this.tableName);
                        if(gridIndex!=null){
                            String path= "src/main/resources/data/" + this.tableName + "_" + this.pageNum + ".class";
                            gridIndex.updatePathInGrid(lastElement,page.rows.size()-1,pages.get(pageInfo),path,0);
                            gridIndex.insertGrid(row,pages.get(pageInfo),gridIndex.indices,indexOfRow);
                            gridIndex.serializeGrid();
                        }
                        return;
                    }
                }
            }

        }

        //}
    }




    public void deleteBinary(String tableName, Hashtable<String, Object> columnNameValue, Object clusteringKeyValue, String clusteringKey) throws DBAppException, IOException {

        ArrayList<PageInfo> pagesInfo = new ArrayList<>(this.pages.keySet());
        ArrayList listOfIndices = getIndices(tableName, columnNameValue);
        Collections.sort(pagesInfo);
        ArrayList values = new ArrayList();
        pagesInfo.forEach(info -> values.add(info.getMin().getKeyValue()));
        int indexOfPage = Collections.binarySearch(values, clusteringKeyValue);
        indexOfPage = (indexOfPage == -1) ? 0 : (indexOfPage < 0) ? ((indexOfPage + 2) * -1) : indexOfPage;
        PageInfo pageInfo = pagesInfo.get(indexOfPage);
        Page page = this.deserializePage(this.pages.get(pageInfo));
        Row comparisonRow = new Row(clusteringKey, columnNameValue);
        int indexOfRow = Collections.binarySearch((List) page.rows, comparisonRow);
        if (indexOfRow < 0)
            throw new DBAppException("No matching record.");
        Row rowToDelete = page.rows.get(indexOfRow);
        if (!rowToDelete.matchRecord(listOfIndices, columnNameValue))
            throw new DBAppException("No matching record.");
        page.delete(rowToDelete);
        updatePageInfoDelete(pageInfo, page);
        serializePage(page, pageInfo.getPageNum());
        if (pageInfo.isEmpty()) {
            try {
                new FileOutputStream(this.pages.get(pageInfo)).close();
                if (new File(this.pages.get(pageInfo)).delete()) {
                    System.out.println("File Deleted");
                } else {
                    System.out.println("File not Found");
                }

            } catch (Exception e) {
                e.getStackTrace();
            }
            this.opposite.remove(this.pages.get(pageInfo));
            this.pages.remove(pageInfo);
            serializeTable(tableName);
            return;
        } else {
            serializePage(page, pageInfo.getPageNum());
            serializeTable(tableName);
            return;

        }

    }

    public  Iterator selectLinear(String tableName, SQLTerm[] sqlTerms, String[] arrayOperators) throws IOException {
        Set<PageInfo> pagesInfosSet = pages.keySet();
        ArrayList<PageInfo> pagesInfos = new ArrayList<PageInfo>(pagesInfosSet);
        Collections.sort((List) pagesInfos);
        ArrayList<Statement> statements= new ArrayList<>();
        Hashtable<String, Object> colNameStatement = new Hashtable<>();
        Statement current;
        for(SQLTerm sqlTerm : sqlTerms){
            current=new Statement(sqlTerm._strTableName,sqlTerm._strColumnName,sqlTerm._strOperator,sqlTerm._objValue);
            statements.add(current);
            colNameStatement.put(current._strColumnName,current);
        }

            ArrayList listOfIndices = getIndices(tableName,colNameStatement);
            for (PageInfo pageInfo : pagesInfos) {
                Page page = deserializePage(this.pages.get(pageInfo));
                for (Row row : page.rows) {
                         row.addRecord(listOfIndices,colNameStatement);
                }
            }

                ArrayList<Statement> resultStatements = new ArrayList(colNameStatement.values());
                ArrayList result = new ArrayList();

                if(arrayOperators.length>0){
                    for(int i=0;i<arrayOperators.length;i++){
                        if(i==0){
                            ArrayList operand1 = (resultStatements.get(0)).results;
                            ArrayList operand2 = (resultStatements.get(1)).results;
                            result= checkOperator(operand1,operand2,arrayOperators[0],false);
                            resultStatements.remove(0);
                            if(resultStatements.size()>0)
                                resultStatements.remove(0);
                        }
                        else {
                            ArrayList operand1 = (resultStatements.get(0)).results;
                            result = checkOperator(operand1, result, arrayOperators[i],false);
                            resultStatements.remove(0);
                        }

                    }
                }
                else{
                    return Arrays.asList(resultStatements.get(0).results).iterator();
                }

                return Arrays.asList(result).iterator();


    }

    public ArrayList<Row> union(ArrayList<Row> operand1, ArrayList<Row> operand2, boolean grid) {

        ArrayList<Row> result = new ArrayList<>(operand1);
        if(!grid) {
            for (Row row : operand2) {
                if (!result.contains(row)) {
                    result.add(row);
                }
            }
        }
        else{
            for (Row row: operand1){
                for (Row innerRow: operand2)
                {
                    if (!row.values.equals(innerRow.values) && !result.contains(row))
                    {
                        result.add(innerRow);
                    }
                }
            }
        }
        return result;
    }

    public ArrayList<Row> intersect(ArrayList<Row> operand1, ArrayList<Row> operand2,boolean grid) {

        ArrayList<Row> result = new ArrayList<>();
        if(!grid){
            result = new ArrayList<>();
            for (Row row : operand1)
                if (operand2.contains(row))
                    result.add(row);
        }
        else {
            for (Row row : operand1) {
                for (Row innerRow : operand2) {
                    if (innerRow.values.equals(row.values))
                        result.add(row);
                }
            }
        }
        return result;

    }

    public ArrayList<Row> unique(ArrayList<Row> operand1, ArrayList<Row> operand2, boolean grid) {

        ArrayList<Row> result = new ArrayList<>();
        if(!grid) {
            for (Row row : operand2) {
                if (!operand1.contains(row)) {
                    result.add(row);
                }
            }
            for (Row row : operand1) {
                if (!operand2.contains(row)) {
                    result.add(row);
                }
            }
        }
        else{
            for (Row row : operand2) {
                for(Row innerRow :operand1){
                    if (!innerRow.values.equals(row.values)) {
                        result.add(row);
                    }
                }

            }
            for (Row row : operand1) {
                for(Row innerRow :operand2){
                    if (!innerRow.values.equals(row.values)) {
                        result.add(row);
                    }
                }
            }

        }

        return result;
    }

    public ArrayList checkOperator(ArrayList operand1,ArrayList operand2 , String operator, boolean grid){

        switch (operator){
            case "AND": return intersect(operand1,operand2, grid);
            case "OR": return union(operand1,operand2,grid);
            case "XOR": return unique(operand1,operand2,grid);
        }
        return null;
    }



    public void deleteLinear(String tableName, Hashtable<String, Object> columnNameValue) throws IOException, DBAppException{

        Boolean found = false;
        Boolean deleted=false;
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
                                new FileOutputStream(this.pages.get(pageInfo)).close();
                           if (new File(this.pages.get(pageInfo)).delete()) {
                                    deleted=true;
                                    System.out.println("File Deleted");
                                }
                                else {
                                    System.out.println("File not Found");
                                }

                            } catch (Exception e) {
                                e.getStackTrace();
                            }
                            this.opposite.remove(this.pages.get(pageInfo));
                            this.pages.remove(pageInfo);
                            serializeTable(tableName);
                        }


                    }

            }
                if(!deleted) {
                    serializePage(page, pageInfo.getPageNum());
                }
                deleted=false;
        }
        serializeTable(tableName);
        if(!found)
            throw new DBAppException ("No matching record found.");
    }

    public void deleteGridIndex(String path, int rowNum){
        Page page = Table.deserializePage(path);
        Row row = page.rows.get(rowNum);
        PageInfo pageInfo=this.opposite.get(path);
        page.delete(row);
        updatePageInfoDelete(pageInfo, page);
        serializePage(page,pageInfo.getPageNum());
        boolean deleted=false;
        if (pageInfo.isEmpty()) {
            try {
                new FileOutputStream(this.pages.get(pageInfo)).close();
         if (new File(this.pages.get(pageInfo)).delete()) {
                    deleted=true;
                    System.out.println("File Deleted");
                }
                else {
                    System.out.println("File not Found");
                }

            } catch (Exception e) {
                e.getStackTrace();
            }
            this.opposite.remove(this.pages.get(pageInfo));
            this.pages.remove(pageInfo);
            serializeTable(tableName);
        }
        if(!deleted) {
            serializePage(page, pageInfo.getPageNum());
        }
        serializeTable(tableName);
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
    Row rowToUpdate = page.rows.get(indexOfRow);
    String[] colNames = new String[columnNameValue.size()];
    ArrayList<String> colNamesArrayList = new ArrayList<>(columnNameValue.keySet());
    for(String colName: colNamesArrayList)
        colNames[colNamesArrayList.indexOf(colName)]=colName;
        GridIndex gridIndex = GridIndex.deserializeGrid(tableName,colNames);
        if(gridIndex!=null) {

            gridIndex.updateGrid(rowToUpdate,listOfIndices,columnNameValue);

        }
        else{
            rowToUpdate.update(listOfIndices,columnNameValue);
        }

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
    public static Page deserializePage(String path){
        Page page = null;
        try{
            FileInputStream fileIn =
                    new FileInputStream(new File(path));
            ObjectInputStream in= new ObjectInputStream(fileIn);
            page = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (FileNotFoundException | ClassNotFoundException e) {}

        catch (IOException e) {}

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
        serializePage(page, this.pageNum);
        String path= "src/main/resources/data/" + this.tableName + "_" + this.pageNum + ".class";
        pages.put(info, path);
        opposite.put(path,info);
    }

}
