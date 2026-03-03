import * as Aggregation from "./aggregation";
import * as Database from "./database";

export class Query {
    private _table: string;
    private _select: string[] = [];

    private _aggregations: { columnName: string, aggregation: Aggregation.AggregationName }[] = [];
    private _filters: { columnName: string, value: unknown }[] = [];
    private _grouping: string | undefined;

    constructor(table: string) {
        this._table = table;
    }

    select(columnName: string, aggregation: Aggregation.AggregationName | undefined): Query {
        if (typeof aggregation === "undefined") {
            if (columnName === "*") {
                this._select.push(columnName);
            } else {
                throw new Error("`aggregation` is required");
            }
        } else {   
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

    evaluate(database: Database.StelaDB): unknown[][] {
        const restriction = this._filters.reduce((p, c, ix) => {
            const col = database.column(this._table, c.columnName);
            const filtered = col.filter(c.value);
            return ix === 0 ? filtered : p.intersection(filtered);
        }, new Set() as Set<number>);

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
            heading.push(`${select.aggregation}(${select.columnName})`);
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
}