package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
    // Jacky Li's personal codes starts from here
    private File f;
    private TupleDesc td;
    private int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.tableId = this.getId();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
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
        return this.f.getAbsoluteFile().hashCode();
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        // throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        int pSize = BufferPool.getPageSize();  // pSize == pageSize
        int pNumber = pid.getPageNumber();
        int offset = pNumber * pSize;

        try {
            RandomAccessFile r = new RandomAccessFile(this.f, "r");
            byte[] d = new byte[pSize];  // d == data
            r.seek(offset);
            r.read(d);
            r.close();
            Page p = new HeapPage((HeapPageId)pid, d);

            return p;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            int pageSize = BufferPool.getPageSize();
            int pageNumber = page.getId().getPageNumber();
            int offset = pageSize * pageNumber;

            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.seek(offset);
            raf.write(page.getPageData());
            raf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) f.length() / BufferPool.getPageSize();   // floor come for free
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> dirtyPages = new ArrayList<>();
        // I need to know which page I am going to insert this tuple
        for (int i = 0; i < this.numPages(); i++) {
            PageId pageId = new HeapPageId(this.tableId, i);
            HeapPage hPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (hPage.getNumEmptySlots() > 0) {
                hPage.insertTuple(t);
                hPage.markDirty(true,tid);
                dirtyPages.add(hPage);
                return dirtyPages;
            }
        }
        // if we can not find the existing pages have empty slots in them, then we need to add a new HeapPage on
        // the disk
        HeapPage newPage = new HeapPage(new HeapPageId(this.tableId, this.numPages()),HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        newPage.markDirty(true, tid);
        writePage(newPage);
        dirtyPages.add(newPage);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // what if the t is not in this heapFile - table??
        if (!t.getTupleDesc().equals(this.td)) {
            throw new DbException("the tuple is not this HeapFile!");
        }

        ArrayList<Page> dirtyPages = new ArrayList<>();
        // first u still need to know which page is the tuple belongs to?
        PageId hPageId = t.getRecordId().getPageId();
//        HeapPage hPage = (HeapPage) this.readPage(hPageId);    // I am not sure if we ccan use this method
        HeapPage hPage = (HeapPage) Database.getBufferPool().getPage(tid, hPageId, Permissions.READ_WRITE);
        hPage.deleteTuple(t);
        hPage.markDirty(true, tid);
        dirtyPages.add(hPage);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    public class HeapFileIterator implements DbFileIterator {

        private HeapPageId pid;
        private HeapPage curPg;
        private int cpn = 0;  // current page Number
        private Iterator<Tuple> itr;
        private TransactionId tid;
        private boolean statusOpen;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.statusOpen = false;
        }

        public void openByPgNum(int pgNum) throws DbException, TransactionAbortedException {
            this.cpn = pgNum;
            this.pid = new HeapPageId(getId(), cpn);
            this.curPg = (HeapPage) Database.getBufferPool().getPage(this.tid,
                    pid, Permissions.READ_ONLY);
            this.itr = curPg.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            openByPgNum(0);
            this.statusOpen = true;
        }

        @Override
        public void close() {
            this.pid = null;
            this.cpn = 0;
            this.curPg = null;
            this.itr = null;
            this.statusOpen = false;
        }

        @Override

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.statusOpen){
                if (this.itr.hasNext())
                    return true;
                while (this.cpn + 1 < numPages()) {
                    openByPgNum(this.cpn + 1);
                    if (this.itr.hasNext())
                        return true;
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()){
                return itr.next();
            } else {
                throw new NoSuchElementException();
            }
        }



        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }


    }

}

