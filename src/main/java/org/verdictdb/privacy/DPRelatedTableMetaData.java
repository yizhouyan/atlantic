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
    private Map<String, BigDecimal> maxFrequency = new HashMap<>();

    private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

    public DPRelatedTableMetaData(ScrambleMeta singleMeta){
        blockCount = singleMeta.getAggregationBlockCount();
        ScramblingMethod method = singleMeta.getScramblingMethod();
        blockSize = ((ScramblingMethodBase) method).getBlockSize();
        sampleRate = 1.0 / blockCount;
        columnMin = singleMeta.getPrivacyColumnMin();
        columnMax = singleMeta.getPrivacyColumnMax();
        maxFrequency = singleMeta.getPrivacyMetaMaxFreq();
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(double sampleRate){
        this.sampleRate = sampleRate;
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

    public boolean hasColumnMinMax(String columnName) {
        return columnMin.containsKey(columnName) && columnMax.containsKey(columnName);
    }

    public boolean hasColumnMax(String columnName){
        return columnMax.containsKey(columnName);
    }

    public boolean hasColumnMaxFrequency(String columnName){
        return maxFrequency.containsKey(columnName);
    }

    public BigDecimal getColumnMaxFrequency(String columnName){
        return maxFrequency.get(columnName);
    }
}
