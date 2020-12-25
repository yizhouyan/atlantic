package org.verdictdb.core.scrambling;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)

public class PrivacyMeta implements Serializable {
    private static final long serialVersionUID = -5868149128257905562L;
    Map<String, BigDecimal> columnMin = new HashMap<>();
    Map<String, BigDecimal> columnMax = new HashMap<>();

    @JsonIgnore
    private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

    public PrivacyMeta() {
    }

    public List<ExecutableNodeBase> getStatisticsNode(
            String oldSchemaName,
            String oldTableName,
            String columnMetaTokenKey) {
        PrivacyStatisticsRetrievalNode countNode =
                new PrivacyStatisticsRetrievalNode(oldSchemaName, oldTableName, columnMetaTokenKey);
        return Arrays.<ExecutableNodeBase>asList(countNode);
    }

    public void extractPrivacyMeta(Map<String, Object> metaData) throws VerdictDBException {
        DbmsQueryResult privacyMetaResult =
                (DbmsQueryResult) metaData.get(PrivacyStatisticsRetrievalNode.class.getSimpleName());
        privacyMetaResult.next();
        int numColmns = privacyMetaResult.getColumnCount();
        List<String> columnNames = new ArrayList<>();
        for (int i = 0; i < numColmns; i++) {
            columnNames.add(privacyMetaResult.getColumnName(i));
        }
        if (columnNames.contains(PrivacyStatisticsRetrievalNode.PRIVACY_STATS_DUMMPY_COL)) {
            log.debug("Dummy meta detected, no meta data for privacy computation. ");
        } else {
            log.debug(String.format("%d columns detected. %s", numColmns, columnNames));
            for (int i = 0; i < numColmns; i++) {
                String privacyColName = privacyMetaResult.getColumnName(i);
                BigDecimal columnValue = privacyMetaResult.getBigDecimal(i);
                Pair<String, String> columnMethodAndName = PrivacyStatisticsRetrievalNode.getMethodAndColName(privacyColName);
                if (columnMethodAndName.getLeft().equals("min")) {
                    columnMin.put(columnMethodAndName.getRight(), columnValue);
                } else if (columnMethodAndName.getLeft().equals("max")) {
                    columnMax.put(columnMethodAndName.getRight(), columnValue);
                } else {
                    throw new VerdictDBException("Privacy Statistic methods should be either max or min.");
                }
            }
        }
    }

}
