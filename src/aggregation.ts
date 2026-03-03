export type AggregationName = "min" | "max" | "sum";

export interface Aggregator {
    aggregate(data: unknown[]): number;    
}

export class Min implements Aggregator {
    aggregate(data: unknown[]): number {
        return Math.min(...(data as number[]));
    }
}

export class Max implements Aggregator {
    aggregate(data: unknown[]): number {
        return Math.max(...(data as number[]));
    }
}

export class Sum implements Aggregator {
    aggregate(data: unknown[]): number {
        return (data as number[]).reduce((p,c) => p + c, 0);
    }
}

export function getAggregator(name: AggregationName): Aggregator {
    switch (name) {
        case "min": return new Min();
        case "max": return new Max();
        case "sum": return new Sum();
    }
}