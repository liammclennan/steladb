import path from 'node:path';
import fs from "node:fs";

export function fileToTypedArray(dbPath: string, tableName: string, columnName: string): unknown[] {
    const tablePath = path.join(dbPath, tableName, `${columnName}.txt`);

    const strings = fs.readFileSync(tablePath, 'utf8').split('\n');
    const sample = strings.slice(0, 10);
    const asNumbers = strings.map(parseFloat);

    return asNumbers.every(n => !isNaN(n)) 
        ? asNumbers
        : strings;
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