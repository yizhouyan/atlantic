package org.verdictdb.privacy;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.coordinator.VerdictSingleResultFromListData;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.scrambling.ScramblingMethod;
import org.verdictdb.core.scrambling.ScramblingMethodBase;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.metastore.VerdictMetaStore;
import org.apache.commons.math3.distribution.LaplaceDistribution;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Process the query, and attach noise to the ResultSet based on differential privacy.
 */
public class DPNoiseEstimator {
    protected VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

    // Used for inferring grouping and aggregate columns.
    private SelectQuery originalQuery;

    private boolean isSupported = true;

    private HashMap<Integer, ColumnOp> aggregationColumns;

    private HashMap<Pair<String, String>, DPRelatedTableMetaData> tableMetaData = new HashMap<>();

    private double computedEpsilon = 0.0;

    private int maxK = 10000;

    // The S = max(k=0...n) e^(-beta*k)S(k)(q,x) will be pre-computed for each aggregation function.
    private HashMap<Integer, BigDecimal> preComputedS;

    public DPNoiseEstimator(SelectQuery originalQuery, VerdictMetaStore metaStore) {
        this.originalQuery = originalQuery;
        this.isSupported = isQuerySupported();
        if (this.isSupported) {
            setUpDPNoiseEstimator(metaStore.retrieve());
        }
        log.debug("Differential Privacy supported = " + isSupported);
    }

    private boolean isQuerySupported() {
        List<AbstractRelation> tableList = originalQuery.getFromList();
        if (tableList.size() != 1)
            return false;
        List<SelectItem> selectItems = originalQuery.getSelectList();
        // Queries with select * are not supported
        for (SelectItem item : selectItems) {
            if (item instanceof AsteriskColumn) {
                return false;
            }
        }
        aggregationColumns = new HashMap<Integer, ColumnOp>();
        for (int index = 0; index < selectItems.size(); index++) {
            SelectItem item = selectItems.get(index);
            if (item instanceof AliasedColumn) {
                item = ((AliasedColumn) item).getColumn();
            }
            if (item instanceof ColumnOp && ((ColumnOp) item).isPrivacySupportedAggregateColumn()) {
                aggregationColumns.put(index, (ColumnOp) item);
            }
        }
        log.debug(String.format("Found %d aggregation columns: %s", aggregationColumns.size(), aggregationColumns));
        if (aggregationColumns.size() == 0)
            return false;
        return true;
    }

    private boolean isGroupByQuery(){
        return originalQuery.getGroupby().size() > 0;
    }

    private double computeBeta(DPRelatedTableMetaData singleTableMeta) {
        double delta = VerdictOption.getPrivacyDelta();
        double epsilon = VerdictOption.getPrivacyEpsilon();
        double sampleRate = singleTableMeta.getSampleRate();
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

    private Pair<String, String> getMetaKey(String schemaName, String tableName) {
        return Pair.of(schemaName, tableName);
    }

    private void setupTableMetaData(ScrambleMetaSet scrambleMetaSet){
        for (AbstractRelation table : originalQuery.getFromList()) {
            if (table instanceof BaseTable) {
                String schemaName = ((BaseTable) table).getSchemaName();
                String tableName = ((BaseTable) table).getTableName();
                if (scrambleMetaSet.isScrambled(schemaName, tableName)) {
                    log.debug(String.format("Scramble table %s:%s detected. ", schemaName, tableName));
                    ScrambleMeta singleMeta = scrambleMetaSet.getSingleMeta(schemaName, tableName);
                    tableMetaData.put(getMetaKey(schemaName, tableName), new DPRelatedTableMetaData(singleMeta));
                }
            }
        }
    }

    private String getColumnName(UnnamedColumn column){
        if (column instanceof BaseColumn){
            return ((BaseColumn) column).getColumnName();
        }else{
            log.debug(String.format("Column type %s in sum/avg not supported in privacy computation"), column.getClass());
            isSupported = false;
            return null;
        }
    }
    private BigDecimal getLocalSensitivity(ColumnOp column, DPRelatedTableMetaData singleTableMeta){
        if(column.getOpType() == "count"){
            log.debug("Count aggregation detected, local sensitivity = 1");
            return new BigDecimal(1);
        } else if(column.getOpType() == "sum"){
            String operandName = getColumnName(column.getOperand(0));
            if (isSupported){
                BigDecimal ls = singleTableMeta.getColumnMax(operandName);
                log.debug(String.format("Sum aggregation detected for column %s, local sensitivity %f ",
                        operandName, ls));
                return ls;
            }
        } else if (column.getOpType() == "avg"){
            String operandName = getColumnName(column.getOperand(0));
            if (isSupported){
                BigDecimal ls =
                        singleTableMeta.getColumnMax(operandName).subtract(singleTableMeta.getColumnMin(operandName));
                log.debug(String.format("Avg aggregation detected for column %s, local sensitivity %f ",
                        operandName, ls));
                return ls;
            }
        } else{
            log.debug("Column type in sum/avg not supported in privacy computation");
        }
        isSupported = false;
        return null;
    }

    private BigDecimal computeS(double beta, BigDecimal ls, long maxK) {
        BigDecimal maxS = new BigDecimal(0);
        for (int k = 0; k <= maxK; k++) {
            BigDecimal curS = ls.multiply(new BigDecimal(Math.exp(-beta * k) * k));
            int res = curS.compareTo(maxS);
            if (res == 1 || res == 0)
                maxS = curS;
            else {
                log.debug("Final K = " + k);
                return maxS;
            }
        }
        return maxS;
    }

    private void setUpDPNoiseEstimator(ScrambleMetaSet scrambleMetaSet) {
        setupTableMetaData(scrambleMetaSet);
        if (tableMetaData.size() == 0){
            log.debug("No scramble/metadata detected in the query. Privacy not supported. ");
            isSupported = false;
        } else if (tableMetaData.size() > 1){
            log.debug("More than one scramble table detected in the query. Privacy not supported. ");
            isSupported = false;
        } else{
            DPRelatedTableMetaData singleTableMeta = tableMetaData.values().iterator().next();
            double beta = computeBeta(singleTableMeta);
            preComputedS = new HashMap<Integer, BigDecimal>();
            for (Map.Entry<Integer, ColumnOp> column : aggregationColumns.entrySet()) {
                BigDecimal columnLS = getLocalSensitivity(column.getValue(), singleTableMeta);
                BigDecimal curColS = computeS(beta, columnLS, singleTableMeta.getTotalCount());
                log.debug(columnLS + " " + curColS);
                preComputedS.put(column.getKey(), curColS);
            }
        }
    }


    private BigDecimal computeLaplaceNoise(BigDecimal s) {
        LaplaceDistribution ld = new LaplaceDistribution(0, 2 * s.doubleValue() / computedEpsilon);
        return new BigDecimal(ld.sample());
    }

    private Number addNoiseToSingleValue(Number originalValue, BigDecimal noise) {
        if (originalValue instanceof BigDecimal){
            return ((BigDecimal) originalValue).add(noise);
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
