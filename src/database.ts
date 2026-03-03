import * as Aggregation from "./aggregation";
import fs from "node:fs";
import path from 'node:path';

export class StelaDB {
    private _path: string;

    constructor(path: string) {
        this._path = path;
    }

    column(table: string, columnName: string): Column {
        const tablePath = path.join(this._path, table, `${columnName}.txt`);
        const data = this.fileToTypedArray(tablePath);
        return new LiteralColumn(data);
    }

    private fileToTypedArray(path: string): unknown[] {
        const strings = fs.readFileSync(path, 'utf8').split('\n');
        const sample = strings.slice(0, 10);
        const asNumbers = strings.map(parseFloat);

        return asNumbers.every(n => !isNaN(n)) 
            ? asNumbers
            : strings;
    }
}

interface Column {
    filter(value: unknown):  Set<number>;
    group(mask: Set<number>): Map<unknown, Set<number>>;
    aggregate(aggregator: Aggregation.Aggregator, mask: Set<number>): number;
    length(): number;
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

    length(): number {
        return this._data.length;
    }

}