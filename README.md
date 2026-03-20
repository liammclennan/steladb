# StelaDB

The simplest possible, read-only, column oriented database. Intended as a reference for those interested in column store internals, not for any kind of usage. 

> Requires a recent version of node. 

## Demo

To create a demo database:

```shell
$ npm run write
```

or, to include a dictionary encoded column:

```shell
$ npm run write-dictionary
```

To run demo queries:

```shell
$ npm start
```

## Usage (see demo.ts):

1. Build a database from a CSV file: 

    ```shell
    $ node scripts/write-dictionary.js <csv file> <db name>
    ```

1. Create a database object:

    ```js
    const db = new StelaDB("./data");
    ```

1. Create a query object:

    ```js
    const query = new Query("cars")
        .where("year", 2018)
        .groupBy("make")
        .select("price", "min")
        .select("price", "max")
        .select("mileage", "mean");
    ```

1. Evaluate the query:

    ```js
    db.evaluate(query)
    ```

## Database Storage Format

A StelaDB database is a directory. Each subdirectory of the database directory is a table. Each text file in each table directory is a column. The first line of each column file identifies that column's encoding. Each column encoding maps to an implementation of the `Column` interface. 

E.g.

```
/mydb
    /cars
        make.txt
        mileage.txt
        price.txt
        year.txt
```
