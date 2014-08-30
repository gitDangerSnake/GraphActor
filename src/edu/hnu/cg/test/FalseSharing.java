package edu.hnu.cg.test;
public final class FalseSharing
    implements Runnable
{
    public final static int NUM_THREADS = 100; // change
    public final static long ITERATIONS = 50L * 1000L * 1000L;
    private final int arrayIndex;
 
    private static long[] longs = new long[NUM_THREADS*15];
    static
    {
        for (int i = 0; i < longs.length; i++)
        {
        }
    }
 
    public FalseSharing(final int arrayIndex)
    {
        this.arrayIndex = arrayIndex*8+7;
    }
 
    public static void main(final String[] args) throws Exception
    {
    	long s = System.currentTimeMillis();
        final long start = System.nanoTime();
        runTest();
        
        System.out.println("duration = " + (System.nanoTime() - start));
        System.out.println(System.currentTimeMillis()-s);
    }
 
    private static void runTest() throws InterruptedException
    {
        Thread[] threads = new Thread[NUM_THREADS];
 
        for (int i = 0; i < threads.length; i++)
        {
            threads[i] = new Thread(new FalseSharing(i));
        }
 
        for (Thread t : threads)
        {
            t.start();
        }
 
        for (Thread t : threads)
        {
            t.join();
        }
    }
 
    public void run()
    {
        long i = ITERATIONS + 1;
        while (0 != --i)
        {
            longs[arrayIndex] = i;
        }
    }
 
    public final static class VolatileLong
    {
        //public long p1, p2, p3, p4, p5, p6,p7; // comment out
        public volatile long value = 0L;
        //public long p1, p2, p3, p4, p5, p6,p7; // comment out
    }
}