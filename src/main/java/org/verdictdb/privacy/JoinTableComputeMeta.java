package org.verdictdb.privacy;

import org.verdictdb.commons.VerdictDBLogger;

import java.math.BigDecimal;

public class JoinTableComputeMeta {
    protected VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());
    private BigDecimal mfBaseCurrent;
    private BigDecimal mfBasePrevious;
    private boolean selfJoin;

    public JoinTableComputeMeta(BigDecimal mfBaseCurrent, BigDecimal mfBasePrevious, boolean selfJoin){
        this.mfBaseCurrent = mfBaseCurrent;
        this.mfBasePrevious = mfBasePrevious;
        this.selfJoin = selfJoin;
    }

    public BigDecimal computeElasticSensitivity(BigDecimal baseSensitivity, BigDecimal cumulatedSensitivity, BigDecimal k){
        BigDecimal result;
        log.trace(this.toString() + "\t cumulatedSensitivity = " +
                cumulatedSensitivity + ", baseSensitivity = " + baseSensitivity + ", k = " + k);
        if (selfJoin){
            result = ((mfBaseCurrent.add(k)).multiply(cumulatedSensitivity))
                    .add((mfBasePrevious.add(k)).multiply(baseSensitivity))
                    .add(baseSensitivity.multiply(cumulatedSensitivity));
        } else{
            result = ((mfBaseCurrent.add(k)).multiply(cumulatedSensitivity))
                    .max((mfBasePrevious.add(k)).multiply(baseSensitivity));
        }
        return result;
    }

    public String toString(){
        return "mfBaseCurrent = " + mfBaseCurrent + ", mfBasePrevious = " + mfBasePrevious + ", isSelfJoin? = " + selfJoin;
    }

}
