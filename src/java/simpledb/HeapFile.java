package simpledb;

import javafx.geometry.HPos;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File file;
    TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // same as table id
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.getPageNumber();
        int pgSize = BufferPool.getPageSize();
        byte data[] = new byte[pgSize];
        
        HeapPage page = null;
        try {
            RandomAccessFile randAccFile = new RandomAccessFile(file, "r");
            randAccFile.seek(pgNo * pgSize);
            randAccFile.read(data);
            page = new HeapPage((HeapPageId) pid, data);
            randAccFile.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int pgNo = pid.getPageNumber();
        int pgSize = BufferPool.getPageSize();

        try {
            RandomAccessFile randAccFile = new RandomAccessFile(file, "rw");
            randAccFile.seek(pgNo * pgSize);
            randAccFile.write(page.getPageData());
            randAccFile.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = null;

        // find a non full page
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0)
                break;
        }

        // new a new page & write to file
        // then read it from file through BuffurPool
        if (page == null || page.getNumEmptySlots() == 0) {
            HeapPageId pid = new HeapPageId(getId(), numPages());
            byte[] data = HeapPage.createEmptyPageData();
            HeapPage heapPage = new HeapPage(pid, data);
            writePage(heapPage);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        }

        page.insertTuple(t);

        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();

        // delete tuple and mark page as dirty
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);

        // return res
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }

    public class ConcreteDbFileIterator extends AbstractDbFileIterator {

        TransactionId tid;
        int tableId;
        int pageIndex;
        int numPages;

        HeapPageId pid;
        HeapPage heapPage;
        HeapPage.HeapPageIterator pageIterator;

        public ConcreteDbFileIterator(TransactionId tid, int numPages) {

            this.tid = tid;
            tableId = getId();
            pageIndex = 0;
            this.numPages = numPages;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {

            // not open() yet
            if (pageIterator == null)
                return null;

            if (pageIterator.hasNext()) {
                return pageIterator.next();
            }

            // move to the next heap page
            if (pageIndex != numPages - 1) {
                pageIndex++;
                pid = new HeapPageId(tableId, pageIndex);
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                pageIterator = (HeapPage.HeapPageIterator) heapPage.iterator();

                if (pageIterator.hasNext()) {
                    return pageIterator.next();
                }
            }
            return null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {

            String path = file.getAbsolutePath();
            pid = new HeapPageId(tableId, pageIndex);
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            pageIterator = (HeapPage.HeapPageIterator) heapPage.iterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {

            pageIndex = 0;
            pid = new HeapPageId(tableId, pageIndex);
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            pageIterator = (HeapPage.HeapPageIterator) heapPage.iterator();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new ConcreteDbFileIterator(tid, numPages());
    }

}

