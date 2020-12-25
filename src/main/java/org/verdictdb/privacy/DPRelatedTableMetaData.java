package org.verdictdb.privacy;

import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScramblingMethod;
import org.verdictdb.core.scrambling.ScramblingMethodBase;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class DPRelatedTableMetaData {
    private double sampleRate;
    private long blockSize;
    private long blockCount;
    private Map<String, BigDecimal> columnMin = new HashMap<>();
    private Map<String, BigDecimal> columnMax = new HashMap<>();

    private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

    public DPRelatedTableMetaData(ScrambleMeta singleMeta){
        blockCount = singleMeta.getAggregationBlockCount();
        ScramblingMethod method = singleMeta.getScramblingMethod();
        blockSize = ((ScramblingMethodBase) method).getBlockSize();
        sampleRate = 1.0 / blockCount;
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public long getBlockCount() {
        return blockCount;
    }

    public long getTotalCount(){
        return blockCount * blockSize;
    }

    public Map<String, BigDecimal> getColumnMin() {
        return columnMin;
    }

    public Map<String, BigDecimal> getColumnMax() {
        return columnMax;
    }

    public BigDecimal getColumnMin(String columnName) {
        return columnMin.get(columnName);
    }

    public BigDecimal getColumnMax(String columnName) {
        return columnMax.get(columnName);
    }
}
