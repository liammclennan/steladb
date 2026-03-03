type AggregationName = "min" | "max" | "sum";

export class StelaDB {
    private _path: string;

    constructor(path: string) {
        this._path = path;
    }

    query(table: string) {
        return new Query(table, this);
    }

    column(table: string, columnName: string): Column {
        const makes = columnFrom(['Honda', 'Tesla', 'Honda','Ford','BMW','Ford','Volkswagen','Tesla', 'Ford', 'Kia']);
        
        switch (columnName) {
            case "make": return columnFrom(['Honda',    'Tesla',    'Honda',    'Ford',     'BMW',  'Ford', 'Volkswagen',   'Tesla',    'Ford', 'Kia']);
            case "year": return columnFrom(['1999',     '1999',     '1995',     '1999',     '1998', '1999', '2000',         '1999',     '1999', '2000']);
            case "price": return columnFrom([10_000,    12_000,     8_000,      10_000,     10_000, 12_000, 8_000,          14_000,     10_000, 16_000]);
        }

        throw new Error("can't find " + columnName);

        return new LiteralColumn([]);
    }
}

export default class Query {
    private _database: Database;
    private _table: string;

    private _aggregations: { columnName: string, aggregation: AggregationName }[] = [];
    private _filters: { columnName: string, value: unknown }[] = [];
    private _grouping: string | undefined;

    constructor(table: string, database: Database) {
        this._table = table;
        this._database = database;
    }

    select(columnName: string, aggregation: AggregationName): Query {
        this._aggregations.push({ columnName, aggregation });
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

    evaluate(): unknown[][] {
        const restriction = this._filters.reduce((p, c, ix) => {
            const col = this._database.column(this._table, c.columnName);
            const filtered = col.filter(c.value);
            return ix === 0 ? filtered : p.intersection(filtered);
        }, new Set() as Set<number>);

        const groups = typeof this._grouping !== "undefined" 
            ? this._database.column(this._table, this._grouping).group(restriction)
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
            row.push(value);
            
            for (const select of this._aggregations) {
                const aggregator = getAggregator(select.aggregation);
                const column = this._database.column(this._table, select.columnName);
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
    aggregate(aggregator: Aggregator, mask: Set<number>): number;
    length(): number;
}

interface Table {
    columns: Map<string, Column>;
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

    aggregate(aggregator: Aggregator, mask: Set<number>): number {
        const masked = [...mask].map((row_ix) => this._data[row_ix]);
        return aggregator.aggregate(masked);
    }

    length(): number {
        return this._data.length;
    }

}

interface Aggregator {
    aggregate(data: unknown[]): number;    
}

class Min implements Aggregator {
    aggregate(data: unknown[]): number {
        return Math.min(...(data as number[]));
    }
}

class Max implements Aggregator {
    aggregate(data: unknown[]): number {
        return Math.max(...(data as number[]));
    }
}

class Sum implements Aggregator {
    aggregate(data: unknown[]): number {
        return (data as number[]).reduce((p,c) => p + c, 0);
    }
}

export function columnFrom(data: unknown[]): Column {
    return new LiteralColumn(data);
}

function getAggregator(name: AggregationName): Aggregator {
    switch (name) {
        case "min": return new Min();
        case "max": return new Max();
        case "sum": return new Sum();
    }
}