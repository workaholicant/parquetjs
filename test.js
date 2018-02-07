const parquet = require('./parquet');


const opts = {}

const schema = new parquet.ParquetSchema({
    name:       { type: 'UTF8'},
    quantity:   { type: 'INT32'},
    price:      { type: 'DOUBLE', compression: opts.compression },
    date:       { type: 'TIMESTAMP_MICROS', compression: opts.compression },
    finger:     { type: 'FIXED_LEN_BYTE_ARRAY', compression: opts.compression, typeLength: 5 },
    inter:      { type: 'INTERVAL', compression: opts.compression },
  });


const rows = [
{name: 'abc', quantity: 234, price: 3.4, date: new Date(), finger:'abcde', inter: { months: 42, days: 23, milliseconds: 777 }},
{name: 'abc', quantity: 2, price: 3.4, date: new Date(), finger:'abcde', inter: { months: 42, days: 23, milliseconds: 777 }},
{name: 'abc', quantity: 6, price: 3.4, date: new Date(), finger:'abcde', inter: { months: 42, days: 23, milliseconds: 777 }},
{name: 'abc', quantity: 7, price: 3.4, date: new Date(), finger:'abcde', inter: { months: 42, days: 23, milliseconds: 777 }},
{name: 'abc', quantity: 8, price: 3.4, date: new Date(), finger:'abcde', inter: { months: 42, days: 23, milliseconds: 777 }},


]

async function main() {
    let writer = await parquet.ParquetWriter.openFile(schema, 'test.parquet',{pageSize:1});
     for (let row of rows) {
    await writer.appendRow(row);
  }

  await writer.close();
}

main().then(console.log,console.log)