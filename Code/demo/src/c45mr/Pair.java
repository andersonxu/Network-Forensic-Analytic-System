package c45mr;

public final class Pair<L,R> {
    private L l;
    private R r;
    public Pair(L l, R r){
        this.l = l;
        this.r = r;
    }
    public final L getL(){ return l; }
    public final R getR(){ return r; }
    public final void setL(L l){ this.l = l; }
    public final void setR(R r){ this.r = r; }
}