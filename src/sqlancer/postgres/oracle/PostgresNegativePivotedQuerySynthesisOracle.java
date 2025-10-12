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
        selectStatement.setFromList(randomFromTables.getTables().stream()
                .map(t -> new PostgresFromTable(t, false))
                .collect(Collectors.toList()));
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new PostgresColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));

        //we generate a predicate that EXCLUDES the pivot row
        PostgresExpression whereClause = generateNegatedExpression(columns, pivotRow);
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

    private PostgresColumn getFetchValueAliasedColumn(PostgresColumn c) {
        PostgresColumn aliasedColumn = new PostgresColumn(
                c.getName() + " AS " + c.getTable().getName() + c.getName(), c.getType());
        aliasedColumn.setTable(c.getTable());
        return aliasedColumn;
    }

    private PostgresExpression generateNegatedExpression(List<PostgresColumn> columns, PostgresRowValue rw) {
        // generate a random expression
        PostgresExpression expr = new PostgresExpressionGenerator(globalState)
                .setColumns(columns)
                .setRowValue(rw)
                .generateExpressionWithExpectedResult(PostgresDataType.BOOLEAN);

        PostgresExpression result;
        if (expr.getExpectedValue().isNull()) {
            // Already NULL — keep it as IS NULL
            result = PostgresPostfixOperation.create(expr, PostfixOperator.IS_NULL);
        } else {
            boolean expected = expr.getExpectedValue().cast(PostgresDataType.BOOLEAN).asBoolean();
            // we want to negate the expression
            // if the expression is expected to be true, we want it to be false, and vice versa
            result = PostgresPostfixOperation.create(expr,
                    expected ? PostfixOperator.IS_FALSE : PostfixOperator.IS_TRUE);
        }

        rectifiedPredicates.add(result);
        return result;
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

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM (");
        sb.append(query.getUnterminatedQueryString());
        sb.append(") AS result WHERE ");
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
        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, errors);
    }

    @Override
    protected String getExpectedValues(PostgresExpression expr) {
        return PostgresVisitor.asExpectedValues(expr);
    }

}
