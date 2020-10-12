package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        private Record rightRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = this.getLeftPageIterator();
            this.rightIterator = this.getRightPageIterator();

            this.leftRecordIterator = getBlockIterator(getLeftTableName(), leftIterator, numBuffers -2);
            this.rightRecordIterator = getBlockIterator(getRightTableName(), rightIterator, 1);

            this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
            this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;

            this.leftRecordIterator.markPrev();
            this.rightRecordIterator.markPrev();

            fetchNextRecord();
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            if (leftIterator.hasNext()){
                leftRecordIterator = getBlockIterator(this.getLeftTableName(), leftIterator, numBuffers - 2);
                leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
                leftRecordIterator.markPrev();
            } else {
                throw new NoSuchElementException("left");
            }
        }

        private void fetchNextRightBlock() {
            if (rightIterator.hasNext()){
                rightRecordIterator = getBlockIterator(getRightTableName(), rightIterator, 1);
                rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                rightRecordIterator.markPrev();
            } else {
                throw new NoSuchElementException("right");
            }
        }

        private void resetRight() {
            rightIterator = getRightPageIterator();
            fetchNextRightBlock();
        }

        private void fetchNextRecord() {
            nextRecord = null;
            if (leftRecord == null) {
                throw new DatabaseException("no more records left!");
            }

            do {
                if (rightRecord != null) {
                    DataBox leftJoinValue = leftRecord.getValues().get(getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        this.nextRecord = joinRecords(leftRecord, rightRecord);
                    }
                    rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                } else if (leftRecordIterator.hasNext()) {
                    rightRecordIterator.reset();
                    rightRecord = rightRecordIterator.next();
                    leftRecord = leftRecordIterator.next();
                } else if (rightIterator.hasNext()) {
                    fetchNextRightBlock();
                    leftRecordIterator.reset();
                    leftRecord = this.leftRecordIterator.next();
                } else {
                    fetchNextLeftBlock();
                    resetRight();
                }
            } while(!hasNext());
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
