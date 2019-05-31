package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private final int numPages; // actual num of pages

    // use an int to record the time the page is added, from 0, 1, ..., n
    // evict the oldest page
    private static ConcurrentHashMap<PageId, Integer> pageAge;
    private static ConcurrentHashMap<PageId, Page> pages;
    private static int age;
    private PageLockManager lockManager;

    private class Lock {
        TransactionId tid;
        int lockType; // 0 shared lock, 1 exclusive lock

        public Lock(TransactionId tid, int lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }
    }

    private class PageLockManager {

        HashMap<PageId, Vector<Lock>> lockMap;

        public PageLockManager() {
            lockMap = new HashMap<>();
        }

        public synchronized boolean acquireLock(PageId pid, TransactionId tid, int lockType) {
            // if no lock is held on pid
            if (lockMap.get(pid) == null) {
                Lock lock = new Lock(tid, lockType);
                Vector<Lock> locks = new Vector<>();
                locks.add(lock);
                lockMap.put(pid, locks);

                return true;
            }

            // if some Tx holds lock on pid
            // locks.size() won't be 0 because releaseLock will remove 0 size locks from lockMap
            Vector<Lock> locks = lockMap.get(pid);

            // if tid already holds lock on pid
            for (Lock lock : locks) {
                if (lock.tid == tid) {
                    // already hold that lock
                    if (lock.lockType == lockType)
                        return true;
                    // already hold exclusive lock when accquire shared lock
                    if (lock.lockType == 1)
                        return true;
                    // already hold shared lock, upgrade to exclusive lock
                    if (locks.size() == 1) {
                        lock.lockType = 1;
                        return true;
                    } else
                        return false;
                }
            }

            // if the lock is a exclusive lock
            if (locks.get(0).lockType == 1) {
                assert locks.size() == 1 : "exclusive lock can't coexist with other locks";
                return false;
            }

            // if no exclusive lock is held, there could be multiple shared locks
            if (lockType == 0) {

                Lock lock = new Lock(tid, 0);
                locks.add(lock);
                lockMap.put(pid, locks);

                return true;
            }
            // can not acquire a exclusive lock(lockType 1) when there are shard locks on pid
            return false;
        }

        public synchronized boolean releaseLock(PageId pid, TransactionId tid) {

            // if not a single lock is held on pid
            assert lockMap.get(pid) != null : "page not locked!";
            Vector<Lock> locks = lockMap.get(pid);

            for (int i = 0; i < locks.size(); i++) {
                Lock lock = locks.get(i);

                // release lock
                if (lock.tid == tid) {
                    locks.remove(lock);

                    // if the last lock is released
                    // remove 0 size locks from lockMap
                    if (locks.size() == 0)
                        lockMap.remove(pid);
                    return true;
                }
            }
            // not found tid in tids which hold lock on pid
            return false;
        }

        public synchronized boolean holdsLock(PageId pid, TransactionId tid) {

            // if not a single lock is held on pid
            if (lockMap.get(pid) == null)
                return false;

            Vector<Lock> locks = lockMap.get(pid);

            // check if a tid exist in pid's vector of locks
            for (Lock lock : locks)
                if (lock.tid == tid)
                    return true;

            return false;
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pages = new ConcurrentHashMap<>();
        pageAge = new ConcurrentHashMap<>();
        lockManager = new PageLockManager();
        age = 0;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int lockType;
        if (perm == Permissions.READ_ONLY)
            lockType = 0;
        else
            lockType = 1;

        boolean lockAcquired = false;
        while (!lockAcquired)
            lockAcquired = lockManager.acquireLock(pid, tid, lockType);

        if (pages.get(pid) != null)
            return pages.get(pid);
        else {
            int tabId = pid.getTableId();
            DbFile file =  Database.getCatalog().getDatabaseFile(tabId);
            Page page = file.readPage(pid);

            if (numPages == pages.size()) {
                evictPage();
            }
            pages.put(pid, page);
            pageAge.put(pid, age++);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // release all locks
        if (commit) {
            flushPages(tid);
        } else {
            restorePages(tid);
        }

        for (PageId pid: pages.keySet()) {
            if (holdsLock(tid, pid))
                releasePage(tid, pid);
        }
    }

    private void restorePages(TransactionId tid) {

        for (PageId pid : pages.keySet()) {
            Page page = pages.get(pid);

            if (page.isDirty() == tid) {
                int tabId = pid.getTableId();
                DbFile file =  Database.getCatalog().getDatabaseFile(tabId);
                Page pageFromDisk = file.readPage(pid);

                pages.put(pid, pageFromDisk);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // TODO cache dirty page to ensure up-to-date pages
        DbFile file =  Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pageList = file.insertTuple(tid, t);

        // after inserted, tuple will get a record Id, then we can mark page dirty
        for (Page page: pageList)
            page.markDirty(true, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // TODO cache dirty page to ensure up-to-date pages
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        int tableId = pid.getTableId();

        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pageList = file.deleteTuple(tid, t);

        for (Page page: pageList)
            page.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid: pages.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pages.remove(pid);
        pageAge.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = pages.get(pid);

        file.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pages.keySet()) {
            Page page = pages.get(pid);
            if (page.isDirty() == tid) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // TODO what if evict a page which is locked by a Tx
        assert numPages == pages.size() : "Buffor Pool is not full, not need to evict page";

        PageId pageId = null;
        int oldestAge = -1;

        // find the oldest page to evict (which is not dirty)
        for (PageId pid: pageAge.keySet()) {
            Page page = pages.get(pid);
            // skip dirty page
            if (page.isDirty() != null)
                continue;

            if (pageId == null) {
                pageId = pid;
                oldestAge = pageAge.get(pid);
                continue;
            }

            if (pageAge.get(pid) < oldestAge) {
                pageId = pid;
                oldestAge = pageAge.get(pid);
            }
        }

        if (pageId == null)
            throw  new DbException("failed to evict page: all pages are either dirty");
        Page page = pages.get(pageId);

        // evict page
        pages.remove(pageId);
        pageAge.remove(pageId);
    }

}
