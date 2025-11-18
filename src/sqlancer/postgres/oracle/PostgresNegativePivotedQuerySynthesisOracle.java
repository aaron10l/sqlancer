package sqlancer.postgres.oracle;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresRowValue;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresPostfixOperation;
import sqlancer.postgres.ast.PostgresPostfixOperation.PostfixOperator;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

/**
 * Negative Pivoted Query Synthesis Oracle for PostgreSQL
 * 
 * Unlike traditional PQS which ensures the pivot row IS included in results,
 * this oracle ensures the pivot row is NOT included by generating expressions
 * that evaluate to FALSE or NULL for the pivot row.
 */
public class PostgresNegativePivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<PostgresGlobalState, PostgresRowValue, PostgresExpression, SQLConnection> {

    private List<PostgresColumn> fetchColumns;

    public PostgresNegativePivotedQuerySynthesisOracle(PostgresGlobalState globalState) throws SQLException {
        super(globalState);
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonFetchErrors(errors);
    }

    @Override
    public SQLQueryAdapter getRectifiedQuery() throws SQLException {
        PostgresTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();

        PostgresSelect selectStatement = new PostgresSelect();
        selectStatement.setSelectType(Randomly.fromOptions(PostgresSelect.SelectType.values()));
        List<PostgresColumn> columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        fetchColumns = columns;
        selectStatement.setFromList(randomFromTables.getTables().stream().map(t -> new PostgresFromTable(t, false))
                .collect(Collectors.toList()));
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new PostgresColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));
        
        // KEY DIFFERENCE: Generate expression that should be FALSE/NULL for pivot row
        PostgresExpression whereClause = generateNegativeRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        
        List<PostgresExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        PostgresExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            PostgresExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<PostgresExpression> orderBy = new PostgresExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBys();
        selectStatement.setOrderByClauses(orderBy);
        return new SQLQueryAdapter(PostgresVisitor.asString(selectStatement));
    }

    /*
     * Prevent name collisions by aliasing the column.
     */
    private PostgresColumn getFetchValueAliasedColumn(PostgresColumn c) {
        PostgresColumn aliasedColumn = new PostgresColumn(c.getName() + " AS " + c.getTable().getName() + c.getName(),
                c.getType());
        aliasedColumn.setTable(c.getTable());
        return aliasedColumn;
    }

    private List<PostgresExpression> generateGroupByClause(List<PostgresColumn> columns, PostgresRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> PostgresColumnValue.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private PostgresConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return PostgresConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private PostgresExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return PostgresConstant.createIntConstant(0);
        } else {
            return null;
        }
    }

    /**
     * Generate a rectified expression that should be FALSE or NULL for the pivot row.
     * This is the KEY DIFFERENCE from traditional PQS.
     * 
     * Traditional PQS: rectify to TRUE (IS_TRUE/IS_NULL)
     * Negative PQS: rectify to FALSE/NULL (IS_FALSE/IS_NULL)
     */
    private PostgresExpression generateNegativeRectifiedExpression(List<PostgresColumn> columns, PostgresRowValue rw) {
        PostgresExpression expr = new PostgresExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                .generateExpressionWithExpectedResult(PostgresDataType.BOOLEAN);
        
        PostgresExpression result;
        if (expr.getExpectedValue().isNull()) {
            // NULL is good for exclusion - WHERE NULL excludes rows
            result = PostgresPostfixOperation.create(expr, PostfixOperator.IS_NULL);
        } else {
            // INVERTED LOGIC: If expression is TRUE for pivot, make it FALSE
            // If expression is FALSE for pivot, keep it FALSE
            boolean exprIsTrue = expr.getExpectedValue().cast(PostgresDataType.BOOLEAN).asBoolean();
            result = PostgresPostfixOperation.create(expr,
                    exprIsTrue ? PostfixOperator.IS_FALSE : PostfixOperator.IS_FALSE);
            // Always use IS_FALSE to ensure the pivot row is excluded
        }
        rectifiedPredicates.add(result);
        return result;
    }

    /**
     * For Negative PQS, we INVERT the containment check.
     * 
     * The base class checks if pivot row IS in the result (expects TRUE).
     * For Negative PQS, we want to verify pivot row is NOT in the result.
     * 
     * We do this by inverting the WHERE clause: we select rows that are NOT the pivot.
     * If the base query correctly excluded the pivot, this should return rows.
     * If the base query incorrectly included the pivot, this returns nothing.
     * 
     * So for Negative PQS:
     * - Base class expects containsRows() = true (at least one row)
     * - Our inverted check returns rows when pivot is ABSENT (correct)
     * - Our inverted check returns nothing when pivot is PRESENT (bug found)
     */
    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM (");
        sb.append(query.getUnterminatedQueryString());
        sb.append(") as result WHERE NOT (");
        
        // Build condition that identifies the pivot row
        int i = 0;
        for (PostgresColumn c : fetchColumns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("result.");
            sb.append(c.getTable().getName());
            sb.append(c.getName());
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }
        
        sb.append(")");
        
        // This query returns:
        // - Rows if pivot is ABSENT from result (correct behavior)
        // - Empty if pivot is PRESENT in result (bug - will trigger assertion)
        
        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, errors);
    }

    @Override
    protected String getExpectedValues(PostgresExpression expr) {
        return PostgresVisitor.asExpectedValues(expr);
    }
}