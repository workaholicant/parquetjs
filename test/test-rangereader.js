'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');
const path = require('path');
const fs = require('fs');
const EventEmitter = require('events');

describe('rangereader', async function() {
  // Mock the request module to read test-files
  function request(options, cb) {
    if (!options.headers) {
      const emitter =  new EventEmitter();
      emitter.abort = () => null;
      setTimeout(() => emitter.emit('response',{headers: {'content-length' : 4128461}}));
      return emitter;
    }
    const filename = options.headers.range.slice(0,40);
    fs.readFile(path.resolve(__dirname, 'test-files','rangereader',filename), (err, body) => {
      if (err) {
        cb(err);
      } else {
        cb(null,null,body);
      }
    });
    
  }

  it('parses byteranges',async function(){
    let reader = await parquet.ParquetReader.openUrl(request,{url: 'fruits'},{rangeSize: 100000});
    let cursor = reader.getCursor();
    const records = [];
    let record = null; let i = 0;
    while (record = await cursor.next()) {
      if (i++ === 2) break;
      records.push(record);
    }
    assert.equal(records[0].name,'apples');
    assert.equal(records[1].name,'oranges');
    assert.equal(records[0].quantity,10);
    assert.equal(records[1].quantity,20);
    
    await reader.close();
  });
    
});