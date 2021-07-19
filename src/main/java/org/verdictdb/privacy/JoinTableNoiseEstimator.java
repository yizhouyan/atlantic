package org.verdictdb.privacy;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.mit.dbgroup.atlantic.core.sqlobject.*;
import org.verdictdb.core.sqlobject.*;

import java.math.BigDecimal;
import java.util.*;

/**
 * Process the query, and attach noise to the ResultSet based on differential privacy.
 */
public class JoinTableNoiseEstimator extends DPNoiseEstimator{
    protected VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());
    protected List<BaseTable> joinTables = new ArrayList<>();
    protected Map<String, Pair<String, String>> aliasToTableMapping = new HashMap<>();
    protected List<JoinTableComputeMeta> joinTableComputeMetaList = new ArrayList<>();
    public JoinTableNoiseEstimator(SelectQuery originalQuery,
                                   ScrambleMetaSet scrambleMetaSet,
                                   HashMap<Integer, ColumnOp> aggregationColumns,
                                   DPRelatedTableMetaDataSet dpRelatedTableMetaDataSet) {
        super(originalQuery, aggregationColumns);
        if (!isJoinQuerySupported())
            return;
        setUpDPNoiseEstimator(scrambleMetaSet, dpRelatedTableMetaDataSet);
    }

    private boolean isJoinQuerySupported(){
        for (ColumnOp column: aggregationColumns.values()){
            if (!column.isCountAggregate()){
                log.debug("We currently only support count aggregation in join queries. ");
                return false;
            }
        }
        return true;
    }

    private boolean hasMoreThanOneScrambleTable(){
        int count = 0;
        for(DPRelatedTableMetaData metaData:tableMetaData.values()){
            if(metaData.getSampleRate() <1.0){
                count += 1;
            }
            if(count > 1){
                return true;
            }
        }
        return false;
    }
    private void setupTableMetaData(ScrambleMetaSet scrambleMetaSet, DPRelatedTableMetaDataSet dpRelatedTableMetaDataSet){
        AbstractRelation table = originalQuery.getFromList().get(0);
        if (table instanceof JoinTable) {
            List<AbstractRelation> joinTableList = ((JoinTable) table).getJoinList();
            for (AbstractRelation joinTable: joinTableList) {
                if(joinTable instanceof BaseTable) {
                    String schemaName = ((BaseTable) joinTable).getSchemaName();
                    String tableName = ((BaseTable) joinTable).getTableName();
                    Pair<String, String> metaDataKey = getMetaKey(schemaName, tableName);
                    if (dpRelatedTableMetaDataSet.containsTable(metaDataKey)) {
                        boolean isScrambleTable = scrambleMetaSet.isScrambled(schemaName, tableName);
                        log.debug(String.format("Meta-data for table %s:%s detected. Scramble table? %b ",
                                schemaName, tableName, isScrambleTable));
                        // If the table is not a scramble table, we will need to set the sample rate as 1.0
                        DPRelatedTableMetaData dpMeta = dpRelatedTableMetaDataSet.getDPMetaForTable(metaDataKey);
                        if(!isScrambleTable){
                            dpMeta.setSampleRate(1.0);
                        }
                        tableMetaData.put(metaDataKey, dpMeta);
                        joinTables.add((BaseTable) joinTable);
                        aliasToTableMapping.put(joinTable.getAliasName().get(), metaDataKey);
                    }
                } else {
                    isSupported = false;
                    return;
                }
            }
        }
        if (hasMoreThanOneScrambleTable()){
            log.debug("More than one scramble table detected in the join query, " +
                    "differential privacy not supported. ");
            isSupported = false;
        }
    }

    private Map<String, List<ColumnOp>> getJoinConditionsPerTable(List<UnnamedColumn> allConditions){
        Map<String, List<ColumnOp>> allJoinConditionsPerTable = new HashMap<>();
        for (UnnamedColumn condition: allConditions){
            if (condition instanceof ColumnOp && ((ColumnOp) condition).getOpType() == "equal"){
                List<UnnamedColumn> operands = ((ColumnOp) condition).getOperands();
                for(UnnamedColumn operand: operands){
                    if (operand instanceof BaseColumn){
                        List<ColumnOp> existingColumnOps = allJoinConditionsPerTable.getOrDefault(
                                ((BaseColumn) operand).getTableSourceAlias(),
                                new ArrayList<>());
                        existingColumnOps.add((ColumnOp) condition);
                        allJoinConditionsPerTable.put(((BaseColumn) operand).getTableSourceAlias(), existingColumnOps);
                    }
                }
            }
        }
        if(allJoinConditionsPerTable.keySet().size() != joinTables.size()){
            log.debug(String.format("Found join conditions only for %d (out of %d join tables)",
                    allJoinConditionsPerTable.keySet().size(), joinTables.size()));
            isSupported = false;
        }
        return allJoinConditionsPerTable;
    }

    private Pair<BaseColumn, BaseColumn> getJoinConditionForTable(String curAlias, List<ColumnOp> joinConditions, List<String> processedTableAlias){
        Map<String, Pair<BaseColumn, BaseColumn>> joinConditionsMap = new HashMap<>();
        for(ColumnOp condition: joinConditions){
            List<UnnamedColumn> operands = condition.getOperands();
            if(operands.size() != 2){
                log.error("Join operator requires condition operand size=2, privacy not support");
                isSupported = false;
                return null;
            }
            for(UnnamedColumn operand: operands) {
                if (operand instanceof BaseColumn) {
                    if (!((BaseColumn) operand).getTableSourceAlias().equals(curAlias)) {
                        if(joinConditionsMap.containsKey(((BaseColumn) operand).getTableSourceAlias())){
                            log.error("Found more than one condition on the join table, privacy not support");
                            isSupported=false;
                            return null;
                        }
                        joinConditionsMap.put(((BaseColumn) operand).getTableSourceAlias(),
                                Pair.of((BaseColumn)operands.get(0), (BaseColumn)operands.get(1)));
                    }
                }
            }
        }
        for(String processedTable: processedTableAlias){
            if(joinConditionsMap.containsKey(processedTable)){
                log.trace("Found join condition " + joinConditionsMap.get(processedTable) +
                        " for table " + aliasToTableMapping.get(curAlias));
                return joinConditionsMap.get(processedTable);
            }
        }
        return null;
    }

    private BigDecimal getMaxFrequencyForColumn(BaseColumn column){
        if(aliasToTableMapping.containsKey(column.getTableSourceAlias())) {
            Pair<String, String> schemaTableName = aliasToTableMapping.get(column.getTableSourceAlias());
            if(tableMetaData.containsKey(schemaTableName)) {
                DPRelatedTableMetaData metaData = tableMetaData.get(schemaTableName);
                if (metaData.hasColumnMaxFrequency(column.getColumnName())) {
                    log.trace("Found max frequency " + metaData.getColumnMaxFrequency(column.getColumnName())
                            + " for column " + column);
                    return metaData.getColumnMaxFrequency(column.getColumnName());
                }
            }
        }
        log.error("Cannot find max frequency for column" + column);
        isSupported=false;
        return null;
    }

    private Pair<BigDecimal, BigDecimal> getMaxFrequencyFromMetaData(Pair<BaseColumn, BaseColumn> joinColumns,
                                                                     String currentAlias){
        BaseColumn currentCol, prevCol;
        if(joinColumns.getLeft().getTableSourceAlias() == currentAlias){
            currentCol = joinColumns.getLeft();
            prevCol = joinColumns.getRight();
        } else{
            currentCol = joinColumns.getRight();
            prevCol = joinColumns.getLeft();
        }

        BigDecimal mfBaseCurrent = getMaxFrequencyForColumn(currentCol);
        BigDecimal mfBasePrevious = getMaxFrequencyForColumn(prevCol);
        if(mfBaseCurrent == null || mfBasePrevious == null){
            return null;
        }else {
            return Pair.of(mfBaseCurrent, mfBasePrevious);
        }
    }
    private void setUpJoinComputationMeta(){
        Set<Pair<String, String>> processedTables = new HashSet<>();
        List<String> processedTableAlias = new ArrayList<>();
        AbstractRelation table = originalQuery.getFromList().get(0);
        Map<String, List<ColumnOp>> joinConditionsPerTable = getJoinConditionsPerTable(((JoinTable) table).getCondition());
        if(isSupported) {
            processedTables.add(Pair.of(joinTables.get(0).getSchemaName(), joinTables.get(0).getTableName()));
            processedTableAlias.add(joinTables.get(0).getAliasName().get());
            for (int i = 1; i < joinTables.size(); i++) {
                BaseTable curProcessingTable = joinTables.get(i);
                Pair<String, String> curProcessingTableMetaKey = getMetaKey(curProcessingTable.getSchemaName(),
                        curProcessingTable.getTableName());
                Pair<BaseColumn, BaseColumn> joinColumns = getJoinConditionForTable(
                        curProcessingTable.getAliasName().get(),
                        joinConditionsPerTable.get(curProcessingTable.getAliasName().get()),
                        processedTableAlias);
                if(joinColumns == null){
                    isSupported=false;
                    return;
                }
                Pair<BigDecimal, BigDecimal> currentPreviousMF = getMaxFrequencyFromMetaData(joinColumns,
                        curProcessingTable.getAliasName().get());
                if(currentPreviousMF == null){
                    return;
                }
                boolean isSelfJoin = processedTables.contains(curProcessingTableMetaKey);
                joinTableComputeMetaList.add(new JoinTableComputeMeta(currentPreviousMF.getLeft(),
                        currentPreviousMF.getRight(),isSelfJoin));
                processedTables.add(curProcessingTableMetaKey);
                processedTableAlias.add(curProcessingTable.getAliasName().get());
            }
        }
        log.trace("Join Computation Meta = " + joinTableComputeMetaList);
    }
    private BigDecimal computeElasticSensitivity(int k){
        BigDecimal baseSensitivity = new BigDecimal(1); // We currently support count only, so equals to 1
        BigDecimal cumulatedSensitivity = baseSensitivity;
        for(int i = 0; i< joinTableComputeMetaList.size(); i++){
            cumulatedSensitivity = joinTableComputeMetaList.get(i)
                    .computeElasticSensitivity(baseSensitivity, cumulatedSensitivity, new BigDecimal(k));
            log.trace("CumulatedSensitivity = " + cumulatedSensitivity);
        }
        return cumulatedSensitivity;
    }

    private BigDecimal computeSmoothSensitivity(double beta, long maxK) {
        BigDecimal maxS = new BigDecimal(0);
        for (int k = 0; k <= maxK; k++) {
            BigDecimal es = computeElasticSensitivity(k);
            BigDecimal curS = es.multiply(new BigDecimal( Math.exp(-beta * k) * k));
            int res = curS.compareTo(maxS);
            if (res == 1 || res == 0)
                maxS = curS;
            else {
                log.debug("Beta = " + beta + ", Final K = " + k + ", Noise value = " + maxS);
                return maxS;
            }
        }
        log.debug("Beta = " + beta + ", Final K = " + maxK + ", Noise value = " + maxS);
        return maxS;
    }
    private double getSampleRate(){
        for(DPRelatedTableMetaData metaData:tableMetaData.values()){
            if(metaData.getSampleRate() <1.0){
                return metaData.getSampleRate();
            }
        }
        return 1.0;
    }

    private long getMaxK(){
        for(DPRelatedTableMetaData metaData:tableMetaData.values()){
            if(metaData.getSampleRate() <1.0){
                return metaData.getTotalCount();
            }
        }
        return 10000;
    }
    protected void setUpDPNoiseEstimator(ScrambleMetaSet scrambleMetaSet,
                                         DPRelatedTableMetaDataSet dpRelatedTableMetaDataSet) {
        setupTableMetaData(scrambleMetaSet, dpRelatedTableMetaDataSet);
        setUpJoinComputationMeta();
        if(!isSupported)
            return;
        if (tableMetaData.size() == 0){
            log.debug("No scramble/metadata detected in the query. Privacy not supported. ");
            isSupported = false;
            return;
        }
        double beta = computeBeta(getSampleRate());
        preComputedS = new HashMap<Integer, BigDecimal>();
        for (Map.Entry<Integer, ColumnOp> column : aggregationColumns.entrySet()) {
            BigDecimal curColS = computeSmoothSensitivity(beta, getMaxK());
            preComputedS.put(column.getKey(), curColS);
        }
    }
}
