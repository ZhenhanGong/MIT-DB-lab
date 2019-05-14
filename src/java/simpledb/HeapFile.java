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
            randAccFile.read(data, pgNo * pgSize, pgSize);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
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
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
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
            System.out.println("getId " + getId());
            System.out.println("hash " + file.getAbsoluteFile().hashCode());
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
        System.out.println("num page: " + numPages());
        return new ConcreteDbFileIterator(tid, numPages());
    }

}

