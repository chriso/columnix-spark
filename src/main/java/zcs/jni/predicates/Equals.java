package zcs.jni.predicates;

public class Equals extends Predicate {

    public Equals(int column, long value) {
        ptr = nativeLongEquals(column, value);
    }

    private native long nativeLongEquals(int column, long value);
}
