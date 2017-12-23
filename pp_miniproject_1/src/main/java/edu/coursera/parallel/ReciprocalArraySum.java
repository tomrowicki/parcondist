package edu.coursera.parallel;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */

// For Mini-project 1, it's usually best to choose a threshold so that the number of tasks
// equals the number of processor cores on your machine. (If it equals the number of hardware
// threads, that's okay too.) However, this may not be the best approach for other applications
// where there may be more variation in execution times across tasks, compared to Mini-project
// 1.

public final class ReciprocalArraySum
{

    private static ForkJoinPool pool = new ForkJoinPool( 4 );

    /**
     * Default constructor.
     */
    private ReciprocalArraySum()
    {
        System.setProperty( "java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf( 8 ) );
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum( final double[] input )
    {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for ( int i = 0; i < input.length; i++ )
        {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize( final int nChunks, final int nElements )
    {
        // Integer ceil
        return ( nElements + nChunks - 1 ) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     *         nElements
     */
    private static int getChunkStartInclusive( final int chunk,
                                               final int nChunks, final int nElements )
    {
        final int chunkSize = getChunkSize( nChunks, nElements );
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive( final int chunk, final int nChunks,
                                             final int nElements )
    {
        final int chunkSize = getChunkSize( nChunks, nElements );
        final int end = ( chunk + 1 ) * chunkSize;
        if ( end > nElements )
        {
            return nElements;
        }
        else
        {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask
        extends RecursiveAction
    {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        // private static final int SEQUENTIAL_THRESHOLD = 10000;

        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;

        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;

        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;

        /**
         * Intermediate value produced by this task.
         */
        private double value;

        /**
         * Constructor.
         * 
         * @param setStartIndexInclusive Set the starting index to begin
         *            parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask( final int setStartIndexInclusive,
                                final int setEndIndexExclusive, final double[] setInput )
        {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }

        /**
         * Getter for the value produced by this task.
         * 
         * @return Value produced by this task
         */
        public double getValue()
        {
            return value;
        }

        @Override
        protected void compute()
        {
            // if ( endIndexExclusive - startIndexInclusive <= SEQUENTIAL_THRESHOLD )
            // {
            for ( int i = startIndexInclusive; i < endIndexExclusive; i++ )
            {
                // System.out.println( "Iteration no: " + i );
                value += 1 / input[i];
            }
            // }
            // else
            // {
            // ReciprocalArraySumTask left =
            // new ReciprocalArraySumTask( startIndexInclusive, ( startIndexInclusive +
            // endIndexExclusive ) / 2,
            // input );
            // ReciprocalArraySumTask right =
            // new ReciprocalArraySumTask( ( startIndexInclusive + endIndexExclusive ) / 2,
            // endIndexExclusive,
            // input );
            // left.fork();
            // right.compute();
            // left.join();
            // value = left.getValue() + right.getValue();
            // }
        }
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum( final double[] input )
    {
        assert input.length % 2 == 0;

        // double sum = 0;

        // Compute sum of reciprocals of array elements
        // for ( int i = 0; i < input.length; i++ )
        // {
        // sum += 1 / input[i];
        // }

        // ReciprocalArraySumTask task1 = new ReciprocalArraySumTask( 0, input.length / 2, input );
        // ReciprocalArraySumTask task2 = new ReciprocalArraySumTask( input.length / 2,
        // input.length, input );
        // task1.fork();
        // task2.compute();
        // task1.join();
        // return task1.getValue() + task2.getValue();
        return parManyTaskArraySum( input, 2 );
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum( final double[] input,
                                                 final int numTasks )
    {
        double sum = 0;
        // ForkJoinPool pool = new ForkJoinPool( numTasks );
        //
        // // Compute sum of reciprocals of array elements
        // for ( int i = 0; i < input.length; i++ )
        // {
        // sum += 1 / input[i];
        // }
        ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks];
        System.out.println( "No of tasks: " + numTasks );
        System.out.println( "No of elements: " + input.length );
        for ( int i = 0; i < numTasks; i++ )
        {
            int start = getChunkStartInclusive( i, numTasks, input.length );
            System.out.println( "Starting index: " + start );
            int end = getChunkEndExclusive( i, numTasks, input.length );
            System.out.println( "Ending index:" + end );
            ReciprocalArraySumTask t = new ReciprocalArraySumTask( start, end, input );
            tasks[i] = t;
            // t.fork();
            // pool.submit( t );
            // t.join();
        }
        for ( ReciprocalArraySumTask reciprocalArraySumTask : tasks )
        {
            reciprocalArraySumTask.fork();
        }

        for ( ReciprocalArraySumTask reciprocalArraySumTask : tasks )
        {
            reciprocalArraySumTask.join();
        }
        // TaskStarter taskStarter = new TaskStarter( tasks );
        // pool.invoke( taskStarter );
        // ForkJoinPool.commonPool().invoke( taskStarter );

        for ( ReciprocalArraySumTask reciprocalArraySumTask : tasks )
        {
            sum += reciprocalArraySumTask.getValue();
        }
        return sum;
    }

    private static class TaskStarter
        extends RecursiveAction
    {
        private static final long serialVersionUID = 1L;

        private ReciprocalArraySumTask[] tasks;

        TaskStarter( final ReciprocalArraySumTask[] tasks )
        {
            this.tasks = tasks;
        }

        @Override
        protected void compute()
        {
            invokeAll( tasks );
        }
    }
}
