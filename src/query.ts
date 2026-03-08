import * as Aggregation from "./evaluation/aggregation";

export class Query {
    _table: string;
    _select: string[] = [];

    _aggregations: Aggregation.AggregationInstruction[] = [];
    _filters: { columnName: string, value: unknown }[] = [];
    _grouping: string | undefined;

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

