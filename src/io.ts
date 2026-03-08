import path from 'node:path';
import fs from "node:fs";

export function readString(dbPath: string, tableName: string, columnName: string): string {
    const tablePath = path.join(dbPath, tableName, `${columnName}.txt`);
    return fs.readFileSync(tablePath, 'utf8');
}

export function fileToTypedArray(dbPath: string, tableName: string, columnName: string): {format: string, data: unknown[]} {
    const tablePath = path.join(dbPath, tableName, `${columnName}.txt`);

    const strings = fs.readFileSync(tablePath, 'utf8').split('\n');
    const format = strings.splice(0,1)[0];
    const sample = strings.slice(1, 10);
    const asNumbers = strings.map(parseFloat);

    return { 
        format, 
        data: sample.map(parseFloat).every(n => !isNaN(n)) 
            ? asNumbers
            : strings
    };
}

export function schema(dbPath: string): { [table: string]: string[] } {
    const schema: { [table: string]: string[] } = {};
    const tableNames = fs.readdirSync(dbPath, { withFileTypes: true })
        .filter(dirent => dirent.isDirectory())
        .map(dirent => dirent.name);

    for (const tableName of tableNames) {
        schema[tableName] = fs.readdirSync(path.join(dbPath, tableName), { withFileTypes: false })
            .map(fileName => (fileName as string).split(".")[0]);
    }

    return schema;
}