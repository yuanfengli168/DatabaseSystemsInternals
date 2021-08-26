package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    // Jacky Li's fields
    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private List<Tuple> joinTupleList = new ArrayList<>();
    private Iterator<Tuple> itr;


    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;

    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() throws DbException, TransactionAbortedException {
        // some code goes here
        return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() throws DbException, TransactionAbortedException {
        // some code goes here
        return this.child2.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(this.child1.getTupleDesc(), this.child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();  // they can only take next step as long as they opened
        // choose your join method!
        if (this.p.getOperator().equals(Predicate.Op.EQUALS)) {
            hashJoin();
        } else {
            nestedLoopJoin();
        }
    }

    private void nestedLoopJoin() throws DbException, TransactionAbortedException {
        List<Tuple> tuplesFromFirstRelation = new ArrayList<Tuple>();
        List<Tuple> tuplesFromSecondRelation = new ArrayList<Tuple>();
        while(child1.hasNext()) {
            tuplesFromFirstRelation.add(child1.next());
        }
        while(child2.hasNext()) {
            tuplesFromSecondRelation.add(child2.next());
        }
        for(Tuple tuple1 : tuplesFromFirstRelation) {
            for(Tuple tuple2 : tuplesFromSecondRelation) {
                if(p.filter(tuple1, tuple2)) {
                    Tuple mTuple = mergeTuples(tuple1, tuple2);
                    this.joinTupleList.add(mTuple);
                }
            }
        }
        this.itr = joinTupleList.iterator();
    }

    // please push linux first than this, or delete the following part in advance
    private void hashJoin() throws DbException, TransactionAbortedException {
        Map<Integer, ArrayList<Tuple>> hashMap1 = new HashMap<Integer, ArrayList<Tuple>>(); // int is the field, Tuple, is the tuple
        // Map<Integer, ArrayList<Tuple>> hashMap2 = new HashMap<Integer, ArrayList<Tuple>>();
        while(this.child1.hasNext()) {
            Tuple child1Next = this.child1.next();
            int field1 = this.p.getField1();
            Field f1 = child1Next.getField(field1);    // ???
            int hashCode1 = f1.hashCode();
            if (!hashMap1.containsKey(hashCode1)) {
                ArrayList<Tuple> arl1 = new ArrayList<Tuple>();
                arl1.add(child1Next);
                hashMap1.put(hashCode1,arl1);
                // hashMap1.put(hashCode1, new ArrayList<Tuple>(Arrays.asList(child1Next)));  // might cause some problem
            } else {
                ArrayList<Tuple> al1 = hashMap1.get(hashCode1);
                al1.add(child1Next);
                hashMap1.put(hashCode1, al1);
            }
        }

        while(this.child2.hasNext()) {
            Tuple child2Next = this.child2.next();
            int field2 = this.p.getField2();
            Field f2 = child2Next.getField(field2);
            int hashCode2 = f2.hashCode();
            if (hashMap1.containsKey(hashCode2) && hashMap1.get(hashCode2).size() != 0) {
                for (Tuple child1Next : hashMap1.get(hashCode2)) {
                    Tuple hTuple = mergeTuples(child1Next, child2Next);
                    this.joinTupleList.add(hTuple);
                }
            }
        }
        this.itr = joinTupleList.iterator();
    }


    private Tuple mergeTuples(Tuple t1, Tuple t2) {
        TupleDesc mergeTd = this.getTupleDesc();
        Tuple mergeTuple = new Tuple(mergeTd);

        int index = 0;
        for (int i = 0; i < t1.getTupleDesc().numFields(); i++) {
            mergeTuple.setField(index, t1.getField(i));
            index++;
        }
        for (int i = 0; i < t2.getTupleDesc().numFields(); i++) {
            mergeTuple.setField(index, t2.getField(i));
            index++;
        }

        return mergeTuple;
    }

    public void close() {
        // some code goes here
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.itr = joinTupleList.iterator();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (itr.hasNext()) {
            return itr.next();
        }
        return null;
    }


    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child1, this.child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}

