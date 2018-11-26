package eu.fbk.dkm.springles.inferencer;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;

import eu.fbk.dkm.springles.ruleset.Rule.StatementHandler;

class Buffer implements Iterable<Statement>
{

    // Constants

    private static final int INITIAL_BUCKETS_SIZE = 65536;

    private static final int INITIAL_BUCKETS_MASK = 0xFFFF;

    private static final int BLOCK_SIZE = 16384;

    private static final int BLOCK_BITS = 14;

    private static final int BLOCK_MASK = 0x3FFF;

    // State

    private final ValueFactory factory;

    private long[] buckets;

    private int bucketsMask;

    private final List<Value[]> blocks;

    private int size;

    // Statistics

    private static long insertions = 0;

    private static long duplicates = 0;

    private static long collisions = 0;

    private static long probes = 0;

    public Buffer(final ValueFactory factory)
    {
        Preconditions.checkNotNull(factory);

        this.factory = factory;
        this.buckets = new long[Buffer.INITIAL_BUCKETS_SIZE];
        this.bucketsMask = Buffer.INITIAL_BUCKETS_MASK;
        this.blocks = Lists.newArrayList();
        this.size = 0;
    }

    private void resize()
    {
        this.bucketsMask = (this.bucketsMask << 1) + 1;
        final int capacity = this.buckets.length * 2;
        final long[] newBuckets = new long[capacity];
        for (final long bucket : this.buckets) {
            if (bucket != 0L) {
                final int hash = (int) (bucket >>> 32);
                newBuckets[hash & this.bucketsMask] = bucket;
            }
        }
        this.buckets = newBuckets;
    }

    private synchronized int append(final Value[] block, final int blockLength)
    {
        if (2 * this.size >= this.buckets.length) {
            this.resize();
        }

        int added = 0;
        int offset = 0;
        while (offset < blockLength) {

            ++Buffer.insertions;

            final Value subj = block[offset++];
            final Value pred = block[offset++];
            final Value obj = block[offset++];
            final Value ctx = block[offset++];
            final int hashCode = 31
                    * (31 * (31 * subj.hashCode() + pred.hashCode()) + obj.hashCode())
                    + (ctx == null ? 0 : ctx.hashCode());

            final long mask = (long) hashCode << 32;
            final int bucketIndex = hashCode & this.bucketsMask;

            for (int j = bucketIndex; j != bucketIndex - 1; j = (j + 1) % this.buckets.length) {
                ++Buffer.probes;
                final long bucket = this.buckets[j];
                if (bucket == 0) {
                    this.buckets[j] = mask | this.size | 0x80000000L;
                    final int blockIndex = this.size >> Buffer.BLOCK_BITS;
                    if (blockIndex >= this.blocks.size()) {
                        this.blocks.add(new Value[4 * Buffer.BLOCK_SIZE]);
                    }
                    final Value[] bufferedBlock = this.blocks.get(blockIndex);
                    int blockOffset = 4 * (this.size & Buffer.BLOCK_MASK);
                    bufferedBlock[blockOffset++] = subj;
                    bufferedBlock[blockOffset++] = pred;
                    bufferedBlock[blockOffset++] = obj;
                    bufferedBlock[blockOffset] = ctx;
                    ++this.size;
                    ++added;
                    break;

                } else if ((bucket & 0xFFFFFFFF00000000L) == mask) {
                    final int statementIndex = (int) bucket & 0x7FFFFFFF;
                    final int blockIndex = statementIndex >> Buffer.BLOCK_BITS;
                    final Value[] bufferedBlock = this.blocks.get(blockIndex);
                    int blockOffset = 4 * (statementIndex & Buffer.BLOCK_MASK);
                    final Value oldSubj = bufferedBlock[blockOffset++];
                    final Value oldPred = bufferedBlock[blockOffset++];
                    final Value oldObj = bufferedBlock[blockOffset++];
                    final Value oldCtx = bufferedBlock[blockOffset];

                    final boolean equal = subj == oldSubj && pred == oldPred && obj == oldObj
                            && ctx == oldCtx
                            || subj.equals(oldSubj) && pred.equals(oldPred) && obj.equals(oldObj)
                                    && (ctx == null && oldCtx == null
                                            || ctx != null && ctx.equals(oldCtx));

                    if (equal) {
                        ++Buffer.duplicates;
                        break;
                    } else {
                        ++Buffer.collisions;
                    }
                }
            }
        }
        return added;
    }

    public synchronized int size()
    {
        return this.size;
    }

    @Override
    public Iterator<Statement> iterator()
    {
        return new UnmodifiableIterator<Statement>() {

            private int statementIndex = 0;

            private int blockOffset = 0;

            private Value[] block = Buffer.this.size > 0 ? Buffer.this.blocks.get(0) : null;

            @Override
            public boolean hasNext()
            {
                return this.statementIndex < Buffer.this.size;
            }

            @Override
            public Statement next()
            {
                try {
                    final Resource subj = (Resource) this.block[this.blockOffset++];
                    final IRI pred = (IRI) this.block[this.blockOffset++];
                    final Value obj = this.block[this.blockOffset++];
                    final Resource ctx = (Resource) this.block[this.blockOffset++];

                    ++this.statementIndex;
                    if (this.blockOffset == this.block.length) {
                        if (this.statementIndex < Buffer.this.size) {
                            this.block = Buffer.this.blocks
                                    .get(this.statementIndex >> Buffer.BLOCK_BITS);
                            this.blockOffset = 0;
                        } else {
                            this.block = null;
                        }
                    }

                    return ctx == null ? Buffer.this.factory.createStatement(subj, pred, obj)
                            : Buffer.this.factory.createStatement(subj, pred, obj, ctx);

                } catch (final NullPointerException ex) {
                    throw new NoSuchElementException("Requested statement " + this.statementIndex
                            + ", buffer contains only " + Buffer.this.size + " statements");
                }
            }

        };
    }

    public Appender newAppender()
    {
        return new Appender();
    }

    public static String getStatistics()
    {
        if (Buffer.insertions == 0) {
            return "buffer unused";
        } else {
            return String.format("%d insertions: %d duplicates, %d probes and %d collisions",
                    Buffer.insertions, Buffer.duplicates, Buffer.probes, Buffer.collisions);
        }
    }

    public final class Appender implements StatementHandler<RuntimeException>
    {

        private final Value[] block;

        private int offset;

        private int added;

        private Appender()
        {
            this.block = new Value[4 * Buffer.BLOCK_SIZE];
            this.offset = 0;
            this.added = 0;
        }

        @Override
        public void handle(final Resource subj, final IRI pred, final Value obj,
                final Resource ctx) throws RuntimeException
        {
            this.block[this.offset++] = subj;
            this.block[this.offset++] = pred;
            this.block[this.offset++] = obj;
            this.block[this.offset++] = ctx;
            if (this.offset == this.block.length) {
                this.added += Buffer.this.append(this.block, this.block.length);
                this.offset = 0;
            }
        }

        public int flush()
        {
            this.added += Buffer.this.append(this.block, this.offset);
            return this.added;
        }

    }

}
