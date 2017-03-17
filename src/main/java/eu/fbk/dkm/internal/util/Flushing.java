package eu.fbk.dkm.internal.util;

import org.openrdf.IsolationLevel;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.UpdateContext;
import org.openrdf.sail.helpers.SailConnectionWrapper;
import org.openrdf.sail.helpers.SailWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;

public final class Flushing {

    private static final Logger LOGGER = LoggerFactory.getLogger(Flushing.class);

    public static SailConnection newFlushingSailConnection(final SailConnection connection) {
        return connection instanceof FlushingConnection ? connection : new FlushingConnection(
                connection);
    }

    public static Sail newFlushingSail(final Sail sail) {
        return sail instanceof FlushingSail ? sail : new FlushingSail(sail);
    }

    private static final class FlushingConnection extends SailConnectionWrapper {

        private boolean dirty;

        private boolean flushed;

        FlushingConnection(final SailConnection delegate) {
            super(delegate);
            this.dirty = false;
            this.flushed = false;
        }

        private void flushIfDirty() throws SailException {
            if (this.dirty) {
                commit();
                this.flushed = true;
                this.dirty = false;
            }
        }

        @Override
        public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
                final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings,
                final boolean includeInferred) throws SailException {
            flushIfDirty();
            return super.evaluate(tupleExpr, dataset, bindings, includeInferred);
        }

        @Override
        public CloseableIteration<? extends Resource, SailException> getContextIDs()
                throws SailException {
            flushIfDirty();
            return super.getContextIDs();
        }

        @Override
        public CloseableIteration<? extends Statement, SailException> getStatements(
                final Resource subj, final URI pred, final Value obj,
                final boolean includeInferred, final Resource... contexts) throws SailException {
            flushIfDirty();
            return super.getStatements(subj, pred, obj, includeInferred, contexts);
        }

        @Override
        public long size(final Resource... contexts) throws SailException {
            flushIfDirty();
            return super.size(contexts);
        }

        @Override
        public long size(final Resource context) throws SailException {
            flushIfDirty();
            return super.size(context);
        }

        @Override
        public void commit() throws SailException {
            super.commit();
            this.dirty = false;
            this.flushed = false;
        }

        @Override
        public void rollback() throws SailException {
            if (this.flushed) {
                // Cannot rollback as there are changes already committed by previous flushes
                throw new SailException(
                        "Cannot rollback changes as they have been already automically committed. "
                                + "Your data is corrupted. Apologies.");
            }
            super.rollback();
            this.dirty = false;
        }

        @Override
        public void addStatement(final Resource subj, final URI pred, final Value obj,
                final Resource... contexts) throws SailException {
            super.addStatement(subj, pred, obj, contexts);
            this.dirty = true;
        }

        @Override
        public void removeStatements(final Resource subj, final URI pred, final Value obj,
                final Resource... contexts) throws SailException {
            super.removeStatements(subj, pred, obj, contexts);
            this.dirty = true;
        }

        @Override
		public void addStatement(UpdateContext modify, Resource subj, URI pred, Value obj, Resource... contexts)
				throws SailException {
			// TODO Auto-generated method stub
			super.addStatement(modify, subj, pred, obj, contexts);
		}

		@Override
		public void begin() throws SailException {
			// TODO Auto-generated method stub
			super.begin();
		}

		@Override
		public void begin(IsolationLevel level) throws SailException {
			// TODO Auto-generated method stub
			super.begin(level);
		}

		@Override
		public void startUpdate(UpdateContext modify) throws SailException {
			// TODO Auto-generated method stub
			super.startUpdate(modify);
		}

		@Override
        public void clear(final Resource... contexts) throws SailException {
            super.clear(contexts);
            this.dirty = true;
        }

        @Override
        public CloseableIteration<? extends Namespace, SailException> getNamespaces()
                throws SailException {
            // TODO Auto-generated method stub
            return super.getNamespaces();
        }

        @Override
        public String getNamespace(final String prefix) throws SailException {
            flushIfDirty();
            return super.getNamespace(prefix);
        }

        @Override
        public void setNamespace(final String prefix, final String name) throws SailException {
            super.setNamespace(prefix, name);
            this.dirty = true;
        }

        @Override
        public void removeNamespace(final String prefix) throws SailException {
            super.removeNamespace(prefix);
            this.dirty = true;
        }

        @Override
        public void clearNamespaces() throws SailException {
            super.clearNamespaces();
            this.dirty = true;
        }

       
      public void executeUpdate(final UpdateExpr updateExpr, final Dataset dataset,
              final BindingSet bindings, final boolean includeInferred) throws SailException {
        flushIfDirty();
           super.startUpdate(new UpdateContext(updateExpr, dataset, bindings, true));
           this.dirty = true;
       }

	
		public void removeStatement(UpdateContext modify, Resource subj, URI pred, Value obj, Resource... contexts)
				throws SailException {
			// TODO Auto-generated method stub
			super.removeStatement(modify, subj, pred, obj, contexts);
		}

    }

    private static final class FlushingSail extends SailWrapper {

        FlushingSail(final Sail sail) {
            super(sail);
            LOGGER.warn("Automatic committing of changes before read operations enabled for {}. "
                    + "Transactions supported only if consisting of write operations.");
        }

        @Override
        public SailConnection getConnection() throws SailException {
            return newFlushingSailConnection(super.getConnection());
        }

    }

}
