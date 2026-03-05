import * as Aggregation from "./aggregation";
import { Query } from "./query";
import * as Io from "./io";

export type Row = unknown[];

export function evaluate(path: string, query: Query): Row[] {
    const db = new Db(path);
    return db.evaluate(query);
}

class Db {
    _path: string;

    constructor(path: string) {
        this._path = path;
    }

    column(table: string, columnName: string): Column {
        const data = Io.fileToTypedArray(this._path, table, columnName);
        return new LiteralColumn(data);
    }

    length(table: string): number {
        const schema = Io.schema(this._path);
        return this.column(table, schema[table][0]).length();
    }

    evaluate(query: Query): Row[] {
        const restriction = query._filters.length === 0 
            ? new Set([...Array(this.length(query._table)).keys()])
            : query._filters.reduce((p, c, ix) => {
                const col = this.column(query._table, c.columnName);
                const filtered = col.filter(c.value);
                return ix === 0 ? filtered : p.intersection(filtered);
            }, new Set() as Set<number>);

        if (query._select.length > 0) {
            return this.evaluateSelect(query, restriction);
        } else {
            return this.evaluateAggregation(query, restriction);
        }
    }

    evaluateSelect(query: Query, restriction: Set<number>): Row[] {
        const schema = Io.schema(this._path);
        const tableSchema = schema[query._table];
        const isSelectAll = query._select[0] === "*";
        const columns = (isSelectAll ? tableSchema : query._select)
            .map(columnName => [columnName, this.column(query._table, columnName)] as [string, Column]);
        
        const heading = columns.map(([columnName, column]) => columnName);
        const results: Row[] = [heading];
        const row_ixs = [...restriction];
        
        for (const row_ix of row_ixs) {
            const row = columns.map(([_, column]) => column.get(row_ix));
            results.push(row);
        }

        return results;
    }

    evaluateAggregation(query: Query, restriction: Set<number>): Row[] {
        const groups = typeof query._grouping !== "undefined" 
            ? this.column(query._table, query._grouping).group(restriction)
            : new Map([
                [null, restriction]
            ]);

        const results = [];
        const heading = [];

        if (typeof query._grouping !== "undefined") {
            heading.push(query._grouping);
        }

        for (const select of query._aggregations) {
            heading.push(Aggregation.formatAggregationInstruction(select));
        }
        results.push(heading);

        for (const [value, row_ixs] of groups) {
            const row = [];
            
            if (typeof query._grouping !== "undefined") {
                row.push(value);
            }
            
            for (const select of query._aggregations) {
                const aggregator = Aggregation.getAggregator(select.aggregation);
                const column = this.column(query._table, select.columnName);
                const reduction = column.aggregate(aggregator, row_ixs);
                row.push(reduction);
            }
            results.push(row);
        }
        return results;
    }
}

interface Column {
    filter(value: unknown):  Set<number>;
    group(mask: Set<number>): Map<unknown, Set<number>>;
    aggregate(aggregator: Aggregation.Aggregator, mask: Set<number>): number;
    length(): number;
    get(row_ix: number): unknown;
}

class LiteralColumn<T> implements Column {
    private _data: unknown[];

    constructor(data: T[]) {
        this._data = data;
    }

    filter(value: T): Set<number> {
        return new Set(this._data.flatMap((v, ix) => v === value ? [ix] : []));
    }

    group(mask: Set<number> = new Set([...Array(this._data.length).keys()])): Map<unknown, Set<number>> {
        const groups = new Map<unknown, Set<number>>();
        
        for (let row_ix = 0; row_ix < this._data.length; row_ix++) {
            if (!mask.has(row_ix)) { 
                continue;
            }

            const v = this._data[row_ix];
            if (!groups.has(v)) {
                groups.set(v, new Set());
            }
            const group = groups.get(v)!;
            groups.set(v, group.union(new Set([row_ix])));
        }

        return groups;
    }

    aggregate(aggregator: Aggregation.Aggregator, mask: Set<number>): number {
        const masked = [...mask].map((row_ix) => this._data[row_ix]);
        return aggregator.aggregate(masked);
    }

    get(row_ix: number): unknown {
        return this._data[row_ix];
    }

    length(): number {
        return this._data.length;
    }

}