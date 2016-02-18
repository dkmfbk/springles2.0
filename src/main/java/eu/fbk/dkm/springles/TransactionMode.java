package eu.fbk.dkm.springles;

public enum TransactionMode
{

    // (auto-commit = true will always launch read-only transactions)
    READ_ONLY,

    // (auto-commit = true will launch read-only transactions if read operation called)
    WRITABLE_MANUAL_CLOSURE,

    // (auto-commit = true will launch read-only transactions if read operation called AND
    // (closure up to date OR closure not accessed))
    WRITABLE_AUTO_CLOSURE;

    public static TransactionMode intersect(final TransactionMode first,
            final TransactionMode second)
    {
        return first.ordinal() < second.ordinal() ? first : second;
    }

}
