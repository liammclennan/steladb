import { StelaDB } from "./database";

const db = new StelaDB("/home/liam/code/colstore");
const result = db.query("cars")
    .where("year", "1999")
    .groupBy("make")
    .select("price", "min")
    .select("price", "max")
    .select("price", "sum")
    .evaluate();

console.log(result);