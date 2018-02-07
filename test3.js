const parquet = require('./parquet');


const opts = {}

const schema = new parquet.ParquetSchema({
    name: { repeated: true, type: 'UTF8'}
  });


const rows = [
{name: ['abc']},
{name: ['abcd']}


]

async function main() {
    let writer = await parquet.ParquetWriter.openFile(schema, 'test3.parquet',{pageSize:1,useDataPageV2: true});
     for (let row of rows) {
    await writer.appendRow(row);
  }

  await writer.close();
}

main().then(console.log,console.log)