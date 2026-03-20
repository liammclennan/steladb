export type AggregationName = "min" | "max" | "sum" | "mean";

export type AggregationInstruction = { columnName: string, aggregation: AggregationName };

export interface Aggregator {
    aggregate(data: unknown[]): number;    
}

export class Min implements Aggregator {
    aggregate(data: unknown[]): number {
        let minSoFar = Number.MAX_SAFE_INTEGER;

        for (const v of data) {
            let n = v as number;
            if (n < minSoFar) {
                minSoFar = n;
            }
        }

        return minSoFar;
    }
}

export class Max implements Aggregator {
    aggregate(data: unknown[]): number {
        let maxSoFar = Number.MIN_SAFE_INTEGER;

        for (const v of data) {
            let n = v as number;
            if (n > maxSoFar) {
                maxSoFar = n;
            }
        }

        return maxSoFar;
    }
}

export class Sum implements Aggregator {
    aggregate(data: unknown[]): number {
        const checkpoints: number[] = [];
        return (data as number[]).reduce((p,c) => p + c, 0);
    }
}

export class Mean implements Aggregator {
    aggregate(data: unknown[]): number {
        return (new Sum()).aggregate(data) / data.length;
    }
}

export function getAggregator(name: AggregationName): Aggregator {
    switch (name) {
        case "min": return new Min();
        case "max": return new Max();
        case "sum": return new Sum();
        case "mean": return new Mean();
    }
}

export function formatAggregationInstruction(instruction: AggregationInstruction): string {
    return `${instruction.aggregation}(${instruction.columnName})`;
}