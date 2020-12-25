package org.verdictdb.core.scrambling;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.*;

public class PrivacyStatisticsRetrievalNode extends QueryNodeBase {
    private static final long serialVersionUID = -6227200690897248322L;
    private String schemaName;
    private String tableName;
    private String columnMetaTokenKey;
    public static final String PRIVACY_STATS_ALIAS_PREFIX = "verdictdbprivacy";
    public static final String PRIVACY_STATS_DUMMPY_COL = "verdictdbprivacydummy";
    private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());
    private static final List<String> supportedColumnTypes =
            new ArrayList<>(Arrays.asList("double", "float", "int", "bigint", "tinyint", "real"));

    public PrivacyStatisticsRetrievalNode(String schemaName, String tableName, String columnMetaTokenKey) {
        super(-1, null);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnMetaTokenKey = columnMetaTokenKey;
    }

    private String getColumnName(String methodName, String colName) {
        return PRIVACY_STATS_ALIAS_PREFIX + methodName + colName;
    }

    public static Pair<String, String> getMethodAndColName(String privacyColName) throws VerdictDBException {
        String subPrivacyColName = privacyColName.substring(PRIVACY_STATS_ALIAS_PREFIX.length());
        if (subPrivacyColName.startsWith("max")) {
            return new ImmutablePair<>("max", subPrivacyColName.substring("max".length()));
        } else if (subPrivacyColName.startsWith("min")) {
            return new ImmutablePair<>("min", subPrivacyColName.substring("min".length()));
        } else {
            throw new VerdictDBException("Privacy statistic method should be either max or min..");
        }
    }

    @Override
    public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
        if (tokens.size() == 0) {
            // no token information passed
            throw new VerdictDBValueException("No token is passed.");
        }
        String tableSourceAlias = "t";
        Map<String, Object> metaData = new HashMap<>();
        for (ExecutionInfoToken token : tokens) {
            for (Map.Entry<String, Object> keyValue : token.entrySet()) {
                String key = keyValue.getKey();
                Object value = keyValue.getValue();
                metaData.put(key, value);
            }
        }
        List<Pair<String, String>> columnNamesAndTypes =
                (List<Pair<String, String>>) metaData.get(ScramblingPlan.COLUMN_METADATA_KEY);
        List<SelectItem> selectList = new ArrayList<>();
        for (Pair<String, String> nameAndType : columnNamesAndTypes) {
            String colName = nameAndType.getLeft();
            String colType = nameAndType.getRight();
            if (supportedColumnTypes.contains(colType)) {
                // add min, max in to select list
                selectList.add(new AliasedColumn(ColumnOp.max(new BaseColumn(colName)),
                        getColumnName("max", colName)));
                selectList.add(new AliasedColumn(ColumnOp.min(new BaseColumn(colName)),
                        getColumnName("min", colName)));
            }
        }
        if (selectList.size() > 0) {
            selectQuery = SelectQuery.create(selectList, new BaseTable(schemaName, tableName, tableSourceAlias));
            return selectQuery;
        } else {
            log.debug("No number columns found. Generate a dummpy query.");
            selectList.add(new AliasedColumn(ConstantColumn.valueOf(1), PRIVACY_STATS_DUMMPY_COL));
            selectQuery = SelectQuery.create(selectList, new BaseTable(schemaName, tableName, tableSourceAlias));
            return selectQuery;
        }
    }

    @Override
    public ExecutionInfoToken createToken(DbmsQueryResult result) {
        ExecutionInfoToken token = new ExecutionInfoToken();
        token.setKeyValue(this.getClass().getSimpleName(), result);
        return token;
    }
}
