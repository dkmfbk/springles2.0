package eu.fbk.dkm.springles;

public enum InferenceMode
{

    NONE(false, false),

    FORWARD(true, false),

    BACKWARD(false, true),

    COMBINED(true, true);

    private final boolean forwardEnabled;

    private final boolean backwardEnabled;

    private InferenceMode(final boolean forwardEnabled, final boolean backwardEnabled)
    {
        this.forwardEnabled = forwardEnabled;
        this.backwardEnabled = backwardEnabled;
    }

    public boolean isForwardEnabled()
    {
        return this.forwardEnabled;
    }

    public boolean isBackwardEnabled()
    {
        return this.backwardEnabled;
    }

    public InferenceMode withForward()
    {
        return this.backwardEnabled ? COMBINED : FORWARD;
    }

    public InferenceMode withoutForward()
    {
        return this.backwardEnabled ? BACKWARD : NONE;
    }

    public InferenceMode withBackward()
    {
        return this.forwardEnabled ? COMBINED : BACKWARD;
    }

    public InferenceMode withoutBackward()
    {
        return this.forwardEnabled ? FORWARD : NONE;
    }

    public static InferenceMode intersect(final InferenceMode first, final InferenceMode second)
    {
        return valueOf(first.isForwardEnabled() && second.isForwardEnabled(),
                first.isBackwardEnabled() && second.isBackwardEnabled());
    }

    public static InferenceMode valueOf(final boolean forwardEnabled, final boolean backwardEnabled)
    {
        return forwardEnabled ? backwardEnabled ? COMBINED : FORWARD : //
                backwardEnabled ? BACKWARD : NONE;
    }

}
