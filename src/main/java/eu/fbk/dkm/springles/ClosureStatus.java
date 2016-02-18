package eu.fbk.dkm.springles;

public enum ClosureStatus
{

    STALE,

    CURRENT
    {

        @Override
        public ClosureStatus getStatusAfterStatementsAdded()
        {
            return POSSIBLY_INCOMPLETE;
        }

    },

    POSSIBLY_INCOMPLETE;

    public ClosureStatus getStatusAfterStatementsAdded()
    {
        return this;
    }

    public ClosureStatus getStatusAfterStatementsRemoved()
    {
        return STALE;
    }

}
