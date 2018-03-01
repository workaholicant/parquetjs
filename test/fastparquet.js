const { spawn } = require('child_process');
const streamz = require('streamz');
const parquet_codec_rle = require('../lib/codec/rle');
const chai = require('chai');
const assert = chai.assert;

const script = (data) => `
import pandas as pd
import numpy as np
import json
import fastparquet

encoding = fastparquet.encoding
writer = fastparquet.writer

values = np.array(${JSON.stringify(data)})
o = encoding.Numpy8(np.zeros(100, dtype=np.uint8))
width = encoding.width_from_max_int(values.max())
writer.encode_rle_bp(values, width, o)
buffer = ''.join('{:02x}'.format(x) for x in o.data)
print (json.dumps({'buffer': buffer, 'width': width}))
`;

const runPython = async (script) => {
  const python = spawn('docker',['run','-i','heartysoft/docker-pandas-fastparquet','python']);
  python.stdin.end(script);
  let response = await python.stdout.pipe(streamz()).promise();
  try {
    response = JSON.parse(response);
  } catch(e) {
    throw `Could not parse: ${response}`;
  }
  return response;
};


describe('fastparquet', function() {
  it('should match RLE encoding/decoding', async () => {   
    const data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    const response = await runPython(script(data));

    const expected = new Buffer(response.buffer,'hex').slice(0,8);
    const actual = parquet_codec_rle.encodeValues('INT32',data,{bitWidth: response.width}).slice(4).slice(0,8); 

    assert.deepEqual(actual, expected);
  });
});