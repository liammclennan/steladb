import * as Aggregation from "../aggregation";

/**
 * The operations required from all column encodings.
 */
export interface Column {
    get(row_ix: number): unknown;
    filter(value: unknown):  Set<number>;
    group(mask: Set<number>): Map<unknown, Set<number>>;
    aggregate(aggregator: Aggregation.Aggregator, mask: Set<number>): number;
    length(): number;
}

export { LiteralColumn } from "./literal-column";
export { DictionaryColumn } from "./dictionary-column";