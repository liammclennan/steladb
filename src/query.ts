import * as Aggregation from "./aggregation";
import * as Database from "./database";

export class Query {
    private _table: string;
    private _select: string[] = [];

    private _aggregations: Aggregation.AggregationInstruction[] = [];
    private _filters: { columnName: string, value: unknown }[] = [];
    private _grouping: string | undefined;

    constructor(table: string) {
        this._table = table;
    }

    select(columnName: string, aggregation?: Aggregation.AggregationName): Query {
        if (typeof aggregation === "undefined") {
            if (this._aggregations.length > 0) {
                throw new Error("A query must be a projection or an aggregation - never both");
            }
            this._select.push(columnName);
        } else {   
            if (this._select.length > 0) {
                throw new Error("A query must be a projection or an aggregation - never both");
            }
            this._aggregations.push({ columnName, aggregation });
        }
        return this;
    }

    where(columnName: string, value: unknown) {
        this._filters.push({ columnName, value });
        return this;
    }

    groupBy(columnName: string) {
        this._grouping = columnName;
        return this;
    }

    evaluate(database: Database.StelaDB): Row[] {
        const restriction = this._filters.length === 0 
            ? new Set([...Array(database.length(this._table)).keys()])
            : this._filters.reduce((p, c, ix) => {
                const col = database.column(this._table, c.columnName);
                const filtered = col.filter(c.value);
                return ix === 0 ? filtered : p.intersection(filtered);
            }, new Set() as Set<number>);

        if (this._select.length > 0) {
            return this.evaluateSelect(database, restriction);
        } else {
            return this.evaluateAggregation(database, restriction);
        }
    }

    evaluateSelect(database: Database.StelaDB, restriction: Set<number>): Row[] {
        const schema = database.schema();
        const tableSchema = schema[this._table];
        const isSelectAll = this._select[0] === "*";
        const columns = (isSelectAll ? tableSchema : this._select)
            .map(columnName => [columnName, database.column(this._table, columnName)] as [string, Database.Column]);
        
        const heading = columns.map(([columnName, column]) => columnName);
        const results: Row[] = [heading];
        const row_ixs = [...restriction];
        
        for (const row_ix of row_ixs) {
            const row = columns.map(([columnName, column]) => column.get(row_ix));
            results.push(row);
        }

        return results;
    }

    evaluateAggregation(database: Database.StelaDB, restriction: Set<number>): Row[] {
        const groups = typeof this._grouping !== "undefined" 
            ? database.column(this._table, this._grouping).group(restriction)
            : new Map([
                [null, restriction]
            ]);

        const results = [];
        const heading = [];

        if (typeof this._grouping !== "undefined") {
            heading.push(this._grouping);
        }

        for (const select of this._aggregations) {
            heading.push(Aggregation.formatAggregationInstruction(select));
        }
        results.push(heading);

        for (const [value, row_ixs] of groups) {
            const row = [];
            
            if (typeof this._grouping !== "undefined") {
                row.push(value);
            }
            
            for (const select of this._aggregations) {
                const aggregator = Aggregation.getAggregator(select.aggregation);
                const column = database.column(this._table, select.columnName);
                const reduction = column.aggregate(aggregator, row_ixs);
                row.push(reduction);
            }
            results.push(row);
        }
        return results;
    }

    toString(): string {
        const select = this._select.join(", ") + this._aggregations.map(Aggregation.formatAggregationInstruction).join(", ");
        let result = 
`
SELECT ${select}
FROM ${this._table}`;

        if (this._filters.length > 0) {
            result += 
`
WHERE ${this._filters.map(f => `${f.columnName} = ${f.value}`)}`;
        }

        if (this._grouping) {
            result += 
`
GROUP BY ${this._grouping}`;
        }

        return result;

    }
}

type Row = unknown[];