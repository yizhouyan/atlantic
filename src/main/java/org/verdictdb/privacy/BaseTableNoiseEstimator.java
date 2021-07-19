package org.verdictdb.privacy;

import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.scrambling.ScrambleMetaSet;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Process the query, and attach noise to the ResultSet based on differential privacy.
 */
public class BaseTableNoiseEstimator extends DPNoiseEstimator{
    protected VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

    public BaseTableNoiseEstimator(SelectQuery originalQuery,
                                   ScrambleMetaSet scrambleMetaSet,
                                   HashMap<Integer, ColumnOp> aggregationColumns,
                                   DPRelatedTableMetaDataSet dpRelatedTableMetaDataSet) {
        super(originalQuery, aggregationColumns);
        setUpDPNoiseEstimator(scrambleMetaSet, dpRelatedTableMetaDataSet);
        log.debug("Differential Privacy supported = " + isSupported);
    }

    private void setupTableMetaData(ScrambleMetaSet scrambleMetaSet,
                                    DPRelatedTableMetaDataSet dpRelatedTableMetaDataSet){
        AbstractRelation table = originalQuery.getFromList().get(0);
        if (table instanceof BaseTable) {
            String schemaName = ((BaseTable) table).getSchemaName();
            String tableName = ((BaseTable) table).getTableName();
            Pair<String, String> metaKey = getMetaKey(schemaName, tableName);
            if(dpRelatedTableMetaDataSet.containsTable(metaKey)){
                boolean isScrambleTable = scrambleMetaSet.isScrambled(schemaName, tableName);
                log.debug(String.format("Meta-data for table %s:%s detected. Scramble table? %b ",
                        schemaName, tableName, isScrambleTable));
                // If the table is not a scramble table, we will need to set the sample rate as 1.0
                DPRelatedTableMetaData dpMeta = dpRelatedTableMetaDataSet.getDPMetaForTable(metaKey);
                if(!isScrambleTable){
                    dpMeta.setSampleRate(1.0);
                }
                tableMetaData.put(metaKey, dpMeta);
            }
        }
    }

    private BigDecimal getLocalSensitivity(ColumnOp column, DPRelatedTableMetaData singleTableMeta){
        if(column.getOpType() == "count"){
            log.debug("Count aggregation detected, local sensitivity = 1");
            return new BigDecimal(1);
        } else if(column.getOpType() == "sum"){
            String operandName = getColumnName(column.getOperand(0));
            if (isSupported && singleTableMeta.hasColumnMax(operandName)){
                BigDecimal ls = singleTableMeta.getColumnMax(operandName);
                log.debug(String.format("Sum aggregation detected for column %s, local sensitivity %f ",
                        operandName, ls));
                return ls;
            }
        } else if (column.getOpType() == "avg"){
            String operandName = getColumnName(column.getOperand(0));
            if (isSupported && singleTableMeta.hasColumnMinMax(operandName)){
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

    private BigDecimal computeSmoothSensitivity(double beta, BigDecimal ls, long maxK, ColumnOp column) {
        BigDecimal maxS = new BigDecimal(0);
        for (int k = 0; k <= maxK; k++) {
            BigDecimal curS = ls.multiply(new BigDecimal(Math.exp(-beta * k) * k));
            if (column.getOpType() == "avg"){
                // TODO: for group by, use the count for the group as N
                curS =curS.divide(new BigDecimal(maxK));
            }
            int res = curS.compareTo(maxS);
            if (res == 1 || res == 0)
                maxS = curS;
            else {
                log.debug("Beta = " + beta + ", Final K = " + k + ", Noise value = " + maxS);
                return maxS;
            }
        }
        return maxS;
    }

    protected void setUpDPNoiseEstimator(ScrambleMetaSet scrambleMetaSet,
                                         DPRelatedTableMetaDataSet dpRelatedTableMetaDataSet) {
        setupTableMetaData(scrambleMetaSet, dpRelatedTableMetaDataSet);
        if (tableMetaData.size() == 0){
            log.debug("No metadata detected in the query. Privacy not supported. ");
            isSupported = false;
        } else if (tableMetaData.size() > 1){
            log.debug("More than one table detected in the query. Privacy not supported. ");
            isSupported = false;
        } else{
            DPRelatedTableMetaData singleTableMeta = tableMetaData.values().iterator().next();
            double beta = computeBeta(singleTableMeta.getSampleRate());
            preComputedS = new HashMap<Integer, BigDecimal>();
            for (Map.Entry<Integer, ColumnOp> column : aggregationColumns.entrySet()) {
                BigDecimal columnLS = getLocalSensitivity(column.getValue(), singleTableMeta);
                if (isSupported && columnLS != null) {
                    BigDecimal curColS = computeSmoothSensitivity(beta, columnLS, singleTableMeta.getTotalCount(), column.getValue());
                    preComputedS.put(column.getKey(), curColS);
                }
            }
        }
        log.debug("Differential Privacy supported = " + isSupported);
    }
}
