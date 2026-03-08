import {Column} from "./columns";
import * as Aggregation from "../aggregation";

/**
 * A column that is stored as raw, newline-delimeted values. 
 */
export class LiteralColumn<T> implements Column {
    static format: string = "literal";
    private _data: unknown[];

    constructor(content: string) {
        const lines = content.split("\n").filter(l => l.length > 0);
        const sample = lines.slice(0, 10);

        this._data = sample.map(parseFloat).every(n => !isNaN(n)) 
            ? lines.map(parseFloat)
            : lines;
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