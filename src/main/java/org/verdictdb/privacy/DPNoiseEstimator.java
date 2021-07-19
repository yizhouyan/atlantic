package org.verdictdb.privacy;

import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.coordinator.VerdictSingleResultFromListData;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.apache.commons.math3.distribution.LaplaceDistribution;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Process the query, and attach noise to the ResultSet based on differential privacy.
 */
public abstract class DPNoiseEstimator {
    protected VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

    // Used for inferring grouping and aggregate columns.
    protected SelectQuery originalQuery;

    protected boolean isSupported = true;

    protected HashMap<Integer, ColumnOp> aggregationColumns;

    protected HashMap<Pair<String, String>, DPRelatedTableMetaData> tableMetaData = new HashMap<>();

    protected double computedEpsilon = 0.0;

    protected int maxK = 10000;

    // The S = max(k=0...n) e^(-beta*k)S(k)(q,x) will be pre-computed for each aggregation function.
    HashMap<Integer, BigDecimal> preComputedS;

    public DPNoiseEstimator(SelectQuery originalQuery, HashMap<Integer, ColumnOp> aggregationColumns) {
        this.originalQuery = originalQuery;
        this.aggregationColumns = aggregationColumns;
    }

    protected boolean isGroupByQuery(){
        return originalQuery.getGroupby().size() > 0;
    }

    protected double computeBeta(double sampleRate) {
        double delta = VerdictOption.getPrivacyDelta();
        double epsilon = VerdictOption.getPrivacyEpsilon();
        log.trace(String.format("Start setting up DP Noise with delta = %f, " +
                "epsilon = %f, sample rate = %f", delta, epsilon, sampleRate));
        double transEpsilon = Math.log((Math.exp(epsilon) - 1) / sampleRate + 1);
        double transDelta = delta / sampleRate;
        computedEpsilon = transEpsilon;
        if (isGroupByQuery()){
            log.debug("Group by detected in the query, scale the epsilon and delta");
            transEpsilon = transEpsilon/2;
            transDelta = transDelta/2;
        }
        computedEpsilon = transEpsilon;
        double beta = transEpsilon / (2 * Math.log(2.0 / transDelta));
        log.trace(String.format("Computed epsilon = %f, delta = %f, beta = %f", transEpsilon, transDelta, beta));
        return beta;
    }

    protected Pair<String, String> getMetaKey(String schemaName, String tableName) {
        return Pair.of(schemaName, tableName);
    }

    protected String getColumnName(UnnamedColumn column){
        if (column instanceof BaseColumn){
            return ((BaseColumn) column).getColumnName();
        }else{
            log.debug(String.format("Column type %s in sum/avg not supported in privacy computation"), column.getClass());
            isSupported = false;
            return null;
        }
    }

    protected abstract void setUpDPNoiseEstimator(ScrambleMetaSet scrambleMetaSet, DPRelatedTableMetaDataSet dpRelatedTableMetaDataSet);

    private BigDecimal computeLaplaceNoise(BigDecimal s) {
        LaplaceDistribution ld = new LaplaceDistribution(0, 2 * s.doubleValue() / computedEpsilon);
        return new BigDecimal(ld.sample());
    }

    private Number addNoiseToSingleValue(Number originalValue, BigDecimal noise) {
        if (originalValue instanceof BigDecimal){
            return ((BigDecimal) originalValue).add(noise);
        }else if (originalValue instanceof Double){
            return (new BigDecimal((Double)originalValue).add(noise));
        } else {
            log.debug("Original Value " + originalValue.getClass() +
                    " is not of type bigDecimal, return original value");
            return originalValue;
        }
    }

    public VerdictSingleResult addNoiseToSingleResult(VerdictSingleResult result) {
        if (!isSupported) {
            log.info("The query is not supported in differential privacy. Return the original results. ");
            return result;
        }

        List<String> fieldsName = new ArrayList<>();
        // First collect fieldsName
        while (result.next()) {
            for (int i = 0; i < result.getColumnCount(); i++) {
                fieldsName.add(result.getColumnName(i));
            }
        }
        result.rewind();
        if (fieldsName.size() == 0) {
            log.info("No fields found in the results. Return the original results. ");
            return result;
        }
        // Then add noises to aggregation columns (count/sum/avg, etc...)
        List<List<Object>> resultWithNoise = new ArrayList<>();
        while (result.next()) {
            List<Object> curResultWithNoise = new ArrayList<>();
            for (int i = 0; i < result.getColumnCount(); i++) {
                Object originalValue = result.getValue(i);
                if (aggregationColumns.containsKey(i)) {
                    if (originalValue instanceof Number) {
                        BigDecimal noise = computeLaplaceNoise(preComputedS.get(i));
                        Number valueWithNoise = addNoiseToSingleValue((Number) originalValue, noise);
                        log.debug(String.format("Original Value = %f, noise = %f, value with noise = %f",
                                originalValue, noise, valueWithNoise));
                        curResultWithNoise.add(valueWithNoise);
                    } else {
                        curResultWithNoise.add(originalValue);
                    }
                } else {
                    curResultWithNoise.add(originalValue);
                }
            }
            resultWithNoise.add(curResultWithNoise);
        }
        VerdictSingleResult singleResultWithNoise = new VerdictSingleResultFromListData(fieldsName, resultWithNoise);
        return singleResultWithNoise;
    }
}
