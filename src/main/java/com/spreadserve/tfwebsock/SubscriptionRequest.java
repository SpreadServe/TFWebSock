package com.spreadserve.tfwebsock;

public class SubscriptionRequest {
    private String action;
    private String ecn;
    private String instrument;

    public String getAction( )
    {
        return action;
    }

    public String getECN( )
    {
        return ecn;
    }

    public void setECN( String ecn)
    {
        this.ecn = ecn;
    }

    public String getInstrument( )
    {
        return instrument;
    }

    public void setInstrument( String inst)
    {
        this.instrument = inst;
    }

    @Override
    public boolean equals( Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        SubscriptionRequest that = ( SubscriptionRequest)o;

        if (ecn != null ? !ecn.equals(that.ecn) : that.ecn != null) { return false; }
        if (instrument != null ? !instrument.equals(that.instrument) : that.instrument != null) { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = ecn != null ? ecn.hashCode() : 0;
        result = 31 * result + (instrument != null ? instrument.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "SubscriptionRequest{"  +
              "command='"      + ecn + '\'' +
            ", instrument='" + instrument + '\'' +
            '}';
    }
}
