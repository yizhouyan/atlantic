package org.verdictdb.core.scrambling;

import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import java.math.BigDecimal;
import java.util.*;

public class PrivacyMaxFreqRetrievalNode extends QueryNodeBase {
    private static final long serialVersionUID = -5046082595825056392L;
    private String schemaName;
    private String tableName;
    private String columnName;
    public static final String PRIVACY_MAXFREQ_ALIAS_PREFIX = "verdictdbmaxfreq";
    public static final String PRIVACY_STATS_DUMMPY_COL = "verdictdbprivacydummy";
    private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

    public PrivacyMaxFreqRetrievalNode(String schemaName, String tableName, String columnName) {
        super(-1, null);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    private String getPrivacyColumnName() {
        return PRIVACY_MAXFREQ_ALIAS_PREFIX + columnName;
    }

    public static String getColumnName(String privacyColName) throws VerdictDBException {
        return privacyColName.substring(PRIVACY_MAXFREQ_ALIAS_PREFIX.length());
    }

    @Override
    public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
        if (tokens.size() == 0) {
            // no token information passed
            throw new VerdictDBValueException("No token is passed.");
        }
        String tableSourceAlias = "t";
        List<SelectItem> selectList = new ArrayList<>();
        selectList.add(new BaseColumn(columnName));
        selectList.add(new AliasedColumn(ColumnOp.count(),"tmp_cnt"));
        SelectQuery innerQuery = SelectQuery.create(selectList, new BaseTable(schemaName, tableName, tableSourceAlias));
        innerQuery.addGroupby(new BaseColumn(columnName));
        innerQuery.setAliasName("s");
        selectQuery = SelectQuery.create(
                Arrays.asList(new AliasedColumn(
                        ColumnOp.max(new BaseColumn("tmp_cnt")),
                        getPrivacyColumnName())),
                innerQuery
                );
        return selectQuery;
    }

    @Override
    public ExecutionInfoToken createToken(DbmsQueryResult result) {
        ExecutionInfoToken token = new ExecutionInfoToken();
        token.setKeyValue(getPrivacyColumnName(), result);
        return token;
    }

    public static void extractMaxFreqPrivacyMeta(Map<String, Object> metaData,
                                                Map<String, BigDecimal> privacyMetaMaxFreq) throws VerdictDBException {
        for(Map.Entry<String, Object> metaEntry: metaData.entrySet()) {
            if (metaEntry.getKey().startsWith(PRIVACY_MAXFREQ_ALIAS_PREFIX)) {
                DbmsQueryResult privacyMetaResult =
                        (DbmsQueryResult) metaEntry.getValue();
                while (privacyMetaResult.next()) {
                    String curPrivacyColName = privacyMetaResult.getColumnName(0);
                    BigDecimal curMaxFreqValue = privacyMetaResult.getBigDecimal(0);
                    VerdictDBLogger.getLogger(PrivacyMaxFreqRetrievalNode.class)
                            .debug(String.format("Max Frequency for column %s detected, value = %d.",
                                    PrivacyMaxFreqRetrievalNode.getColumnName(curPrivacyColName),
                                    curMaxFreqValue.intValue()));
                    privacyMetaMaxFreq.put(PrivacyMaxFreqRetrievalNode.getColumnName(curPrivacyColName), curMaxFreqValue);
                }
            }
        }
    }
}
