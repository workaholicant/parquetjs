'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');
const child_process = require('child_process');

// helper function that runs parquet-tools dump inside a docker container and returns the stdout
async function readParquetMr(file) {
  return new Promise( (resolve, reject) => {
    const dockerCmd = `docker run -v \${PWD}:/home nathanhowell/parquet-tools dump --debug /home/${file}`;
    child_process.exec(dockerCmd, (err, stdout, stderr) => {
      if (err || stderr) {
        reject(err || stderr);
      } else {
        resolve(stdout);
      }
    });
  });
}

describe('Parquet-mr', function() {
  it('should read a simple parquetjs file', async function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64' },
      price: { type: 'DOUBLE' },
    });

    const rows = [
      { name: 'apples', quantity: 10, price: 2.6 },
      { name: 'oranges', quantity: 20, price: 2.7},
      { name: 'kiwi', price: 4.2, quantity: 4},
    ];

    let writer = await parquet.ParquetWriter.openFile(schema, 'test-mr.parquet');
    
    for (let row of rows) {
      await writer.appendRow(row);
    }

    await writer.close();

    const result = await readParquetMr('test-mr.parquet');
    assert.equal(result,'row group 0 \n--------------------------------------------------------------------------------\nname:      BINARY UNCOMPRESSED DO:0 FPO:4 SZ:51/51/1.00 VC:3 ENC:PLAIN,RLE\nquantity:  INT64 UNCOMPRESSED DO:0 FPO:79 SZ:46/46/1.00 VC:3 ENC:PLAIN,RLE\nprice:     DOUBLE UNCOMPRESSED DO:0 FPO:154 SZ:46/46/1.00 VC:3 ENC:PLAIN,RLE\n\n    name TV=3 RL=0 DL=0\n    ----------------------------------------------------------------------------\n    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:29 VC:3\n\n    quantity TV=3 RL=0 DL=0\n    ----------------------------------------------------------------------------\n    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:24 VC:3\n\n    price TV=3 RL=0 DL=0\n    ----------------------------------------------------------------------------\n    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:24 VC:3\n\nBINARY name \n--------------------------------------------------------------------------------\n*** row group 1 of 1, values 1 to 3 *** \nvalue 1: R:0 D:0 V:apples\nvalue 2: R:0 D:0 V:oranges\nvalue 3: R:0 D:0 V:kiwi\n\nINT64 quantity \n--------------------------------------------------------------------------------\n*** row group 1 of 1, values 1 to 3 *** \nvalue 1: R:0 D:0 V:10\nvalue 2: R:0 D:0 V:20\nvalue 3: R:0 D:0 V:4\n\nDOUBLE price \n--------------------------------------------------------------------------------\n*** row group 1 of 1, values 1 to 3 *** \nvalue 1: R:0 D:0 V:2.6\nvalue 2: R:0 D:0 V:2.7\nvalue 3: R:0 D:0 V:4.2\n');
  });
});