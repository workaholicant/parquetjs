const parquet = require('./parquet');


const opts = {}

const schema = new parquet.ParquetSchema({
     quantity:   { type: 'INT64', optional: true, compression: opts.compression },
   
  });


const rows = [
{name: 'abc', score: 3, date: new Date(), quantity: 9},
{},
{},
{},
{},
{},
{},
{},
{quantity:3},

/*{name: 'abc', score: 3, date: new Date()},
{name: 'abc', score: 3, date: new Date()},
{name: 'abc', score: 3, date: new Date()},
{name: 'abc', score: 3, date: new Date()},
{name: 'abc', score: 3, date: new Date()},*/
//{name: 'abcd', score: 9},
//{name: 'ebde', score: 10}


]

async function main() {
    let writer = await parquet.ParquetWriter.openFile(schema, 'test2.parquet',{pageSize:2,useDataPageV2: false});
     for (let row of rows) {
    await writer.appendRow(row);
  }

  await writer.close();
  console.log('done writing')

 let reader = await parquet.ParquetReader.openFile('test2.parquet');
  console.log(JSON.stringify(reader.schema))


  let cursor = reader.getCursor();
  let record = null; let i = 0;

  while (record = await cursor.next()) {
    i++;
    console.log(record);
    process.stdout.write('.')
   // if (i ==30000) break;
  }
  await reader.close();



}

main().then(console.log,console.log)