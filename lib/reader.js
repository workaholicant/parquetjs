'use strict';

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

const fs = require('fs');
const thrift = require('thrift');
const parquet_thrift = require('../gen-nodejs/parquet_types');
const parquet_shredder = require('./shred');
const parquet_util = require('./util');
const parquet_schema = require('./schema');
const parquet_codec = require('./codec');
const parquet_compression = require('./compression');
const BufferReader = require('./bufferReader');
const streamz = require('streamz');

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1';

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * Internal type used for repetition/definition levels
 */
const PARQUET_RDLVL_TYPE = 'INT32';
const PARQUET_RDLVL_ENCODING = 'RLE';

/**
 * A parquet cursor is used to retrieve rows from a parquet file in order
 */
class ParquetCursor {

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is usually not recommended to call this constructor directly except for
   * advanced and internal use cases. Consider using getCursor() on the
   * ParquetReader instead
   */
  constructor(metadata, envelopeReader, schema, columnList) {
    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
    this.schema = schema;
    this.columnList = columnList;
    this.rowGroup = [];
    this.rowGroupIndex = 0;
  }

  /**
   * Retrieve the next row from the cursor. Returns a row or NULL if the end
   * of the file was reached
   */
  next() {
    var _this = this;

    return _asyncToGenerator(function* () {
      if (_this.rowGroup.length === 0) {
        if (_this.rowGroupIndex >= _this.metadata.row_groups.length) {
          return null;
        }

        let rowBuffer = yield _this.envelopeReader.readRowGroup(_this.schema, _this.metadata.row_groups[_this.rowGroupIndex], _this.columnList);

        _this.rowGroup = parquet_shredder.materializeRecords(_this.schema, rowBuffer);
        _this.rowGroupIndex++;
      }

      return _this.rowGroup.shift();
    })();
  }

  /**
   * Rewind the cursor the the beginning of the file
   */
  rewind() {
    this.rowGroup = [];
    this.rowGroupIndex = 0;
  }

};

/**
 * A parquet reader allows retrieving the rows from a parquet file in order.
 * The basic usage is to create a reader and then retrieve a cursor/iterator
 * which allows you to consume row after row until all rows have been read. It is
 * important that you call close() after you are finished reading the file to
 * avoid leaking file descriptors.
 */
class ParquetReader {

  /**
   * Open the parquet file pointed to by the specified path and return a new
   * parquet reader
   */
  static openFile(filePath, options) {
    var _this2 = this;

    return _asyncToGenerator(function* () {
      let envelopeReader = yield ParquetEnvelopeReader.openFile(filePath, options);
      return _this2.openEnvelopeReader(envelopeReader);
    })();
  }

  static openBuffer(buffer, options) {
    var _this3 = this;

    return _asyncToGenerator(function* () {
      let envelopeReader = yield ParquetEnvelopeReader.openBuffer(buffer, options);
      return _this3.openEnvelopeReader(envelopeReader);
    })();
  }

  /**
   * Open the parquet file from S3 using the supplied aws client and params
   * The params have to include `Bucket` and `Key` to the file requested
   * This function returns a new parquet reader
   */
  static openS3(client, params, options) {
    var _this4 = this;

    return _asyncToGenerator(function* () {
      let envelopeReader = yield ParquetEnvelopeReader.openS3(client, params, options);
      return _this4.openEnvelopeReader(envelopeReader);
    })();
  }

  /**
   * Open the parquet file from a url using the supplied request module
   * params should either be a string (url) or an object that includes 
   * a `url` property.  
   * This function returns a new parquet reader
   */
  static openUrl(request, params, options) {
    var _this5 = this;

    return _asyncToGenerator(function* () {
      let envelopeReader = yield ParquetEnvelopeReader.openUrl(request, params, options);
      return _this5.openEnvelopeReader(envelopeReader);
    })();
  }

  static openEnvelopeReader(envelopeReader) {
    return _asyncToGenerator(function* () {
      try {
        let header = envelopeReader.readHeader();
        let metadataPromise = envelopeReader.readFooter();
        yield header;
        let metadata = yield metadataPromise;
        return new ParquetReader(metadata, envelopeReader);
      } catch (err) {
        yield envelopeReader.close();
        throw err;
      }
    })();
  }

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is not recommended to call this constructor directly except for advanced
   * and internal use cases. Consider using one of the open{File,Buffer} methods
   * instead
   */
  constructor(metadata, envelopeReader) {
    if (metadata.version != PARQUET_VERSION) {
      throw 'invalid parquet version';
    }

    this.metadata = envelopeReader.metadata = metadata;
    this.envelopeReader = envelopeReader;
    this.schema = envelopeReader.schema = new parquet_schema.ParquetSchema(decodeSchema(this.metadata.schema.splice(1)));
  }

  /**
   * Return a cursor to the file. You may open more than one cursor and use
   * them concurrently. All cursors become invalid once close() is called on
   * the reader object.
   *
   * The required_columns parameter controls which columns are actually read
   * from disk. An empty array or no value implies all columns. A list of column
   * names means that only those columns should be loaded from disk.
   */
  getCursor(columnList) {
    if (!columnList) {
      columnList = [];
    }

    columnList = columnList.map(x => x.constructor === Array ? x : [x]);

    return new ParquetCursor(this.metadata, this.envelopeReader, this.schema, columnList);
  }

  /**
   * Return the number of rows in this file. Note that the number of rows is
   * not neccessarily equal to the number of rows in each column.
   */
  getRowCount() {
    return this.metadata.num_rows;
  }

  /**
   * Returns the ParquetSchema for this file
   */
  getSchema() {
    return this.schema;
  }

  /**
   * Returns the user (key/value) metadata for this file
   */
  getMetadata() {
    let md = {};
    for (let kv of this.metadata.key_value_metadata) {
      md[kv.key] = kv.value;
    }

    return md;
  }

  /**
   * Close this parquet reader. You MUST call this method once you're finished
   * reading rows
   */
  close() {
    var _this6 = this;

    return _asyncToGenerator(function* () {
      yield _this6.envelopeReader.close();
      _this6.envelopeReader = null;
      _this6.metadata = null;
    })();
  }

}

/**
 * The parquet envelope reader allows direct, unbuffered access to the individual
 * sections of the parquet file, namely the header, footer and the row groups.
 * This class is intended for advanced/internal users; if you just want to retrieve
 * rows from a parquet file use the ParquetReader instead
 */
class ParquetEnvelopeReader {

  static openFile(filePath, options) {
    return _asyncToGenerator(function* () {
      let fileStat = yield parquet_util.fstat(filePath);
      let fileDescriptor = yield parquet_util.fopen(filePath);

      let readFn = parquet_util.fread.bind(undefined, fileDescriptor);
      let closeFn = parquet_util.fclose.bind(undefined, fileDescriptor);

      return new ParquetEnvelopeReader(readFn, closeFn, fileStat.size, options);
    })();
  }

  static openBuffer(buffer, options) {
    return _asyncToGenerator(function* () {
      let readFn = function (offset, length) {
        return buffer.slice(offset, offset + length);
      };
      let closeFn = function () {
        return {};
      };
      return new ParquetEnvelopeReader(readFn, closeFn, buffer.length, options);
    })();
  }

  static openS3(client, params, options) {
    return _asyncToGenerator(function* () {
      let fileStat = (() => {
        var _ref = _asyncToGenerator(function* () {
          return client.headObject(params).promise().then(function (d) {
            return d.ContentLength;
          });
        });

        return function fileStat() {
          return _ref.apply(this, arguments);
        };
      })();

      let readFn = (() => {
        var _ref2 = _asyncToGenerator(function* (offset, length) {
          let Range = `bytes=${offset}-${offset + length - 1}`;
          let res = yield client.getObject(Object.assign({ Range }, params)).promise();
          return res.Body;
        });

        return function readFn(_x, _x2) {
          return _ref2.apply(this, arguments);
        };
      })();

      let closeFn = function () {
        return {};
      };

      return new ParquetEnvelopeReader(readFn, closeFn, fileStat, options);
    })();
  }

  static openUrl(request, params, options) {
    return _asyncToGenerator(function* () {
      if (typeof params === 'string') params = { url: params };
      if (!params.url) throw new Error('URL missing');

      params.encoding = params.encoding || null;

      let defaultHeaders = params.headers || {};

      let filesize = (() => {
        var _ref3 = _asyncToGenerator(function* () {
          return new Promise(function (resolve, reject) {
            let req = request(params);
            req.on('response', function (res) {
              req.abort();
              resolve(res.headers['content-length']);
            });
            req.on('error', reject);
          });
        });

        return function filesize() {
          return _ref3.apply(this, arguments);
        };
      })();

      let readFn = function (offset, length) {
        let range = `bytes=${offset}-${offset + length - 1}`;
        let headers = Object.assign({}, defaultHeaders, { range });
        let req = Object.assign({}, params, { headers });
        return new Promise(function (resolve, reject) {
          request(req, function (err, res) {
            if (err) {
              reject(err);
            } else {
              resolve(res.body);
            }
          });
        });
      };

      let closeFn = function () {
        return {};
      };

      return new ParquetEnvelopeReader(readFn, closeFn, filesize, options);
    })();
  }

  constructor(readFn, closeFn, fileSize, options) {
    options = options || {};
    this.readFn = readFn;
    this.close = closeFn;
    this.fileSize = fileSize;
    if (options.maxLength || options.maxSpan || options.queueWait) {
      const bufferReader = new BufferReader(this, options);
      this.read = (offset, length) => bufferReader.read(offset, length);
    }
  }

  read(offset, length) {
    var _this7 = this;

    return _asyncToGenerator(function* () {
      return yield _this7.readFn(offset, length);
    })();
  }

  readHeader() {
    var _this8 = this;

    return _asyncToGenerator(function* () {
      let buf = yield _this8.read(0, PARQUET_MAGIC.length);

      if (buf.toString() != PARQUET_MAGIC) {
        throw 'not valid parquet file';
      }
    })();
  }

  search(paths, options) {

    // Pushes all pages that have data inside the min/max from a rowgroup
    let pages = (() => {
      var _ref4 = _asyncToGenerator(function* (rowGroup) {
        let excludedPages = new Set();

        // start retreiving the offset indices for selected columns
        let offsetsPromise = Promise.all(paths.map((() => {
          var _ref5 = _asyncToGenerator(function* (path, i) {
            const column = rowGroup.columns[columnNumbers[i]];
            return reader.readOffsetIndex(column);
          });

          return function (_x4, _x5) {
            return _ref5.apply(this, arguments);
          };
        })()));

        // Get column indices and compare the min/max to exclude pages
        let cols = yield Promise.all(paths.map((() => {
          var _ref6 = _asyncToGenerator(function* (path, i) {
            if (path.min !== undefined || path.max !== undefined) {
              const column = rowGroup.columns[columnNumbers[i]];
              let colIndex = yield reader.readColumnIndex(column);

              colIndex.max_values.forEach(function (max, pageNo) {
                let min = colIndex.min_values[pageNo];
                if (path.min !== undefined && path.min > max || path.max !== undefined && path.max < min || path.value !== undefined && (max < path.value || min > path.value)) {
                  excludedPages.add(pageNo);
                }
              });
              return colIndex;
            }
          });

          return function (_x6, _x7) {
            return _ref6.apply(this, arguments);
          };
        })()));

        // wait for offset indices to finish
        let offsets = yield offsetsPromise;

        // push forward any pages that haven't been excluded
        for (let i = 0; i < offsets[0].page_locations.length; i++) {
          if (!excludedPages.has(i)) {
            this.push({ offsets, cols, pageNo: i, rowGroupNo: rowGroup.no });
          }
        }
      });

      return function pages(_x3) {
        return _ref4.apply(this, arguments);
      };
    })();

    let results = (() => {
      var _ref7 = _asyncToGenerator(function* (page) {
        var _this9 = this;

        let pageRecords = [];

        if (options.emitPages) {
          this.emit('page', paths.reduce(function (p, path, i) {
            p[path.path] = {
              offset: page.offsets[i].page_locations[page.pageNo],
              max_value: page.cols[i] && page.cols[i].max_values[page.pageNo],
              min_value: page.cols[i] && page.cols[i].min_values[page.pageNo]
            };
            return p;
          }, {
            pageNo: page.pageNo,
            rowGroupNo: page.rowGroupNo
          }));
        }

        yield Promise.all(paths.map((() => {
          var _ref8 = _asyncToGenerator(function* (path, i) {
            if (!path.index) {
              pageRecords = yield reader.readPage(page.offsets[i], page.pageNo, pageRecords);
              if (path.source) {
                sourceIncluded = true;
                pageRecords.forEach(function (pageRecord) {
                  Object.assign(pageRecord, JSON.parse(pageRecord[path.path]));
                  delete pageRecord[path.path];
                });
              }
            }
          });

          return function (_x9, _x10) {
            return _ref8.apply(this, arguments);
          };
        })()));

        pageRecords.forEach(function (rec) {
          if (paths.every(function (path) {
            return path.index && !sourceIncluded || (path.min === undefined || rec[path.path] >= path.min) && (path.max === undefined || rec[path.path] <= path.max) && (path.value === undefined || rec[path.path] == path.value) && (!options.filter || options.filter(rec));
          })) {
            _this9.push(rec);
          }
        });
      });

      return function results(_x8) {
        return _ref7.apply(this, arguments);
      };
    })();

    let sourceIncluded;
    options = options || {};
    let concurrency = options.concurrency || 50;
    let columnNumbers = paths.map(p => this.metadata.row_groups[0].columns.findIndex(d => d.meta_data.path_in_schema.join(',') === p.path));
    let reader = this; // we have to explicitly define reader as the streams redefine `this` context 


    let rowGroups = reader.metadata.row_groups.filter((rowGroup, i) => {
      rowGroup.no = i;
      return paths.every((path, i) => {
        const column = rowGroup.columns[columnNumbers[i]];
        const stats = column.meta_data.statistics;
        return (path.min === undefined || stats.max_value >= path.min) && (path.max === undefined || stats.min_value <= path.max) && (path.value === undefined || stats.min_value <= path.value && stats.max_value >= path.value);
      });
    });

    // Stream all rowgroups that have data inside the min/max
    function streamRowGroups() {
      let out = streamz();
      rowGroups.forEach(rowGroup => out.write(rowGroup));
      out.end();
      return out;
    }

    let res = {
      rowGroups,
      pages: () => streamRowGroups().pipe(streamz(concurrency, pages)),
      results: () => res.pages().pipe(streamz(concurrency, results)),
      first: (() => {
        var _ref9 = _asyncToGenerator(function* () {
          return new Promise(function (resolve, reject) {
            const results = res.results();
            results.on('error', reject).pipe(streamz(function (d) {
              resolve(d);
              results.destroy();
            })).on('finish', function () {
              reject('not_found');
            });
          });
        });

        return function first() {
          return _ref9.apply(this, arguments);
        };
      })(),
      sort: field => {
        let init = (() => {
          var _ref10 = _asyncToGenerator(function* () {
            pages = yield res.pages().promise();

            pages.forEach(function (page) {
              page.max_value = page.cols[columnNumber].max_values[page.pageNo];
              page.min_value = page.cols[columnNumber].min_values[page.pageNo];
            });
          });

          return function init() {
            return _ref10.apply(this, arguments);
          };
        })();

        let next = (() => {
          var _ref11 = _asyncToGenerator(function* () {
            // 1 Find the lowest maximum_value of all remaining pages
            // 2 Get data from all pages with minimum_value < lowest maximum_value
            // 3 Add data to buffer and push all values < lowest maximum_value
            // 4 Values > lowest maximum_value go into buffer
            // Repeat until we have read all the pages
            let lowestMax = pages.reduce(function (p, d) {
              return Math.min(p, d.max_value);
            }, Infinity);
            let candidates = [];
            pages = pages.reduce(function (p, page) {
              if (page.min_value <= lowestMax) {
                candidates.push(page);
              } else {
                p.push(page);
              }
              return p;
            }, []);

            const candidateResults = streamz(concurrency, results);
            candidates.forEach(function (candidate) {
              return candidateResults.write(candidate);
            });
            candidateResults.end();

            const candidateData = yield candidateResults.promise();
            candidateData.forEach(function (d) {
              return buffer.push(d);
            });

            buffer.sort(function (a, b) {
              return a[field] - b[field];
            });

            buffer = buffer.reduce(function (p, d) {
              if (d[field] <= lowestMax) {
                out.push(d);
              } else {
                p.push(d);
              }
              return p;
            }, []);

            if (pages.length) {
              return next();
            } else {
              out.end();
            }
          });

          return function next() {
            return _ref11.apply(this, arguments);
          };
        })();

        let columnNumber = paths.findIndex(d => d.path === field);
        let pages;
        let out = streamz();
        let buffer = [];

        init().then(next).catch(e => out.emit('error', e));

        return out;
      }
    };

    return res;
  }

  // Helper function to get the column object for a particular path and row_group
  getColumn(path, row_group) {
    let column;
    if (!isNaN(row_group)) {
      row_group = this.metadata.row_groups[row_group];
    }

    if (typeof path === 'string') {
      if (!row_group) {
        throw `Missing RowGroup ${row_group}`;
      }
      column = row_group.columns.find(d => d.meta_data.path_in_schema.join(',') === path);
      if (!column) {
        throw `Column ${path} Not Found`;
      }
    } else {
      column = path;
    }
    return column;
  }

  readOffsetIndex(path, row_group) {
    var _this10 = this;

    return _asyncToGenerator(function* () {
      let column = _this10.getColumn(path, row_group);
      let offset_index = new parquet_thrift.OffsetIndex();
      parquet_util.decodeThrift(offset_index, (yield _this10.read(+column.offset_index_offset, column.offset_index_length)));
      Object.defineProperty(offset_index, 'column', { value: column, enumerable: false });
      return offset_index;
    })();
  }

  readColumnIndex(path, row_group) {
    var _this11 = this;

    return _asyncToGenerator(function* () {
      let column = _this11.getColumn(path, row_group);
      let column_index = new parquet_thrift.ColumnIndex();
      parquet_util.decodeThrift(column_index, (yield _this11.read(+column.column_index_offset, column.column_index_length)));
      column_index.column = column;
      return column_index;
    })();
  }

  readPage(offsetIndex, pageNumber, records) {
    var _this12 = this;

    return _asyncToGenerator(function* () {
      let column = Object.assign({}, offsetIndex.column);
      column.metadata = Object.assign({}, column.metadata);
      column.meta_data.data_page_offset = offsetIndex.page_locations[pageNumber].offset;
      column.meta_data.total_compressed_size = offsetIndex.page_locations[pageNumber].compressed_page_size;
      const chunk = yield _this12.readColumnChunk(_this12.schema, column);
      Object.defineProperty(chunk, 'column', { value: column });
      let data = {
        columnData: { [chunk.column.meta_data.path_in_schema.join(',')]: chunk },
        rowCount: offsetIndex.page_locations.length
      };

      return parquet_shredder.materializeRecords(_this12.schema, data, records);
    })();
  }

  // Read the complete index (column and offset) for all pages in a column
  readIndex(path) {
    var _this13 = this;

    return _asyncToGenerator(function* () {
      const columnNum = _this13.metadata.row_groups[0].columns.find(function (d) {
        return d.meta_data.path_in_schema.join(',') === path;
      });
      if (columnNum === -1) {
        throw `Column ${path} Not Found`;
      }
      // We begin by finding the range of the Indices
      const range = {
        first: Infinity,
        last: 0
      };

      _this13.metadata.row_groups.forEach(function (row) {
        const column = row.columns[columnNum];
        range.first = Math.min(range.first, column.column_index_offset, column.offset_index_offset);
        range.last = Math.max(range.last, column.column_index_offset + column.column_index_length, column.offset_index_offset + column.offset_index_length);
      });

      const buffer = yield _this13.read(range.first, range.last - range.first);

      let columnIndices = [];
      let offsetIndices = [];

      _this13.metadata.row_groups.forEach(function (row, i) {
        let column = row.columns[columnNum];
        let readCol = buffer.slice(+column.column_index_offset - range.first, column.column_index_offset + column.column_index_length - range.first);
        let column_index = new parquet_thrift.ColumnIndex();
        parquet_util.decodeThrift(column_index, readCol);
        column_index.row_group = i;
        columnIndices.push(column_index);

        let readOff = buffer.slice(+column.offset_index_offset - range.first, +column.offset_index_offset + column.offset_index_length - range.first);
        let offset_index = new parquet_thrift.OffsetIndex();
        parquet_util.decodeThrift(offset_index, readOff);
        offset_index.row_group = i;
        offsetIndices.push(offset_index);
      });

      return offsetIndices.reduce(function (p, d, i) {
        d.page_locations.forEach(function (l, ii) {
          l.max_value = columnIndices[i].max_values[ii];
          l.min_value = columnIndices[i].min_values[ii];
          l.row_group_no = d.row_group;
          l.page_no = ii;
          p.push(l);
        });
        return p;
      }, []);
    })();
  }

  readRowGroup(schema, rowGroup, columnList) {
    var _this14 = this;

    return _asyncToGenerator(function* () {
      var buffer = {
        rowCount: +rowGroup.num_rows,
        columnData: {}
      };

      let requests = [];

      for (let colChunk of rowGroup.columns) {
        const colMetadata = colChunk.meta_data;
        const colKey = colMetadata.path_in_schema;

        if (columnList.length > 0 && parquet_util.fieldIndexOf(columnList, colKey) < 0) {
          continue;
        }

        requests.push(_this14.readColumnChunk(schema, colChunk).then(function (payload) {
          return {
            colKey, payload
          };
        }));
      }

      let data = yield Promise.all(requests);

      data.forEach(function (d) {
        buffer.columnData[d.colKey] = d.payload;
      });

      return buffer;
    })();
  }

  readColumnChunk(schema, colChunk) {
    var _this15 = this;

    return _asyncToGenerator(function* () {
      if (colChunk.file_path !== null) {
        throw 'external references are not supported';
      }

      let field = schema.findField(colChunk.meta_data.path_in_schema);
      let type = parquet_util.getThriftEnum(parquet_thrift.Type, colChunk.meta_data.type);

      let compression = parquet_util.getThriftEnum(parquet_thrift.CompressionCodec, colChunk.meta_data.codec);

      let pagesOffset = +colChunk.meta_data.data_page_offset;
      let pagesSize = +colChunk.meta_data.total_compressed_size;
      let pagesBuf = yield _this15.read(pagesOffset, pagesSize);

      return decodeDataPages(pagesBuf, {
        type: type,
        rLevelMax: field.rLevelMax,
        dLevelMax: field.dLevelMax,
        compression: compression,
        column: field
      });
    })();
  }

  readFooter() {
    var _this16 = this;

    return _asyncToGenerator(function* () {
      if (typeof _this16.fileSize === 'function') {
        _this16.fileSize = yield _this16.fileSize();
      }
      let trailerLen = PARQUET_MAGIC.length + 4;
      let trailerBuf = yield _this16.read(_this16.fileSize - trailerLen, trailerLen);

      if (trailerBuf.slice(4).toString() != PARQUET_MAGIC) {
        throw 'not a valid parquet file';
      }

      let metadataSize = trailerBuf.readUInt32LE(0);
      let metadataOffset = _this16.fileSize - metadataSize - trailerLen;
      if (metadataOffset < PARQUET_MAGIC.length) {
        throw 'invalid metadata size';
      }

      let metadataBuf = yield _this16.read(metadataOffset, metadataSize);
      let metadata = new parquet_thrift.FileMetaData();
      parquet_util.decodeThrift(metadata, metadataBuf);
      return metadata;
    })();
  }

}

/**
 * Decode a consecutive array of data using one of the parquet encodings
 */
function decodeValues(type, encoding, cursor, count, opts) {
  if (!(encoding in parquet_codec)) {
    throw 'invalid encoding: ' + encoding;
  }

  return parquet_codec[encoding].decodeValues(type, cursor, count, opts);
}

function decodeDataPages(buffer, opts) {
  let cursor = {
    buffer: buffer,
    offset: 0,
    size: buffer.length
  };

  let data = {
    rlevels: [],
    dlevels: [],
    values: [],
    count: 0
  };

  while (cursor.offset < cursor.size) {
    const pageHeader = new parquet_thrift.PageHeader();
    cursor.offset += parquet_util.decodeThrift(pageHeader, cursor.buffer.slice(cursor.offset));

    const pageType = parquet_util.getThriftEnum(parquet_thrift.PageType, pageHeader.type);

    let pageData = null;
    switch (pageType) {
      case 'DATA_PAGE':
        pageData = decodeDataPage(cursor, pageHeader, opts);
        break;
      case 'DATA_PAGE_V2':
        pageData = decodeDataPageV2(cursor, pageHeader, opts);
        break;
      default:
        throw "invalid page type: " + pageType;
    }

    for (let i = 0; i < pageData.rlevels.length; i++) {
      data.rlevels.push(pageData.rlevels[i]);
      data.dlevels.push(pageData.dlevels[i]);
      if (pageData.values[i] !== undefined) {
        data.values.push(pageData.values[i]);
      }
    }
    data.count += pageData.count;
  }

  return data;
}

function decodeDataPage(cursor, header, opts) {
  let valueCount = header.data_page_header.num_values;
  let valueEncoding = parquet_util.getThriftEnum(parquet_thrift.Encoding, header.data_page_header.encoding);

  /* read repetition levels */
  let rLevelEncoding = parquet_util.getThriftEnum(parquet_thrift.Encoding, header.data_page_header.repetition_level_encoding);

  let rLevels = new Array(valueCount);
  if (opts.rLevelMax > 0) {
    rLevels = decodeValues(PARQUET_RDLVL_TYPE, rLevelEncoding, cursor, valueCount, { bitWidth: parquet_util.getBitWidth(opts.rLevelMax) });
  } else {
    rLevels.fill(0);
  }

  /* read definition levels */
  let dLevelEncoding = parquet_util.getThriftEnum(parquet_thrift.Encoding, header.data_page_header.definition_level_encoding);

  let dLevels = new Array(valueCount);
  if (opts.dLevelMax > 0) {
    dLevels = decodeValues(PARQUET_RDLVL_TYPE, dLevelEncoding, cursor, valueCount, { bitWidth: parquet_util.getBitWidth(opts.dLevelMax) });
  } else {
    dLevels.fill(0);
  }

  /* read values */
  let valueCountNonNull = 0;
  for (let dlvl of dLevels) {
    if (dlvl === opts.dLevelMax) {
      ++valueCountNonNull;
    }
  }

  let values = decodeValues(opts.type, valueEncoding, cursor, valueCountNonNull, {
    typeLength: opts.column.typeLength,
    bitWidth: opts.column.typeLength
  });

  return {
    dlevels: dLevels,
    rlevels: rLevels,
    values: values,
    count: valueCount
  };
}

function decodeDataPageV2(cursor, header, opts) {
  const cursorEnd = cursor.offset + header.compressed_page_size;

  const valueCount = header.data_page_header_v2.num_values;
  const valueCountNonNull = valueCount - header.data_page_header_v2.num_nulls;
  const valueEncoding = parquet_util.getThriftEnum(parquet_thrift.Encoding, header.data_page_header_v2.encoding);

  /* read repetition levels */
  let rLevels = new Array(valueCount);
  if (opts.rLevelMax > 0) {
    rLevels = decodeValues(PARQUET_RDLVL_TYPE, PARQUET_RDLVL_ENCODING, cursor, valueCount, {
      bitWidth: parquet_util.getBitWidth(opts.rLevelMax),
      disableEnvelope: true
    });
  } else {
    rLevels.fill(0);
  }

  /* read definition levels */
  let dLevels = new Array(valueCount);
  if (opts.dLevelMax > 0) {
    dLevels = decodeValues(PARQUET_RDLVL_TYPE, PARQUET_RDLVL_ENCODING, cursor, valueCount, {
      bitWidth: parquet_util.getBitWidth(opts.dLevelMax),
      disableEnvelope: true
    });
  } else {
    dLevels.fill(0);
  }

  /* read values */
  let valuesBufCursor = cursor;

  if (header.data_page_header_v2.is_compressed) {
    let valuesBuf = parquet_compression.inflate(opts.compression, cursor.buffer.slice(cursor.offset, cursorEnd));

    valuesBufCursor = {
      buffer: valuesBuf,
      offset: 0,
      size: valuesBuf.length
    };

    cursor.offset = cursorEnd;
  }

  let values = decodeValues(opts.type, valueEncoding, valuesBufCursor, valueCountNonNull, {
    typeLength: opts.column.typeLength,
    bitWidth: opts.column.typeLength
  });

  return {
    dlevels: dLevels,
    rlevels: rLevels,
    values: values,
    count: valueCount
  };
}

function decodeSchema(schemaElements) {
  let schema = {};
  schemaElements.forEach(schemaElement => {

    let repetitionType = parquet_util.getThriftEnum(parquet_thrift.FieldRepetitionType, schemaElement.repetition_type);

    let optional = false;
    let repeated = false;
    switch (repetitionType) {
      case 'REQUIRED':
        break;
      case 'OPTIONAL':
        optional = true;
        break;
      case 'REPEATED':
        repeated = true;
        break;
    };

    if (schemaElement.num_children > 0) {
      schema[schemaElement.name] = {
        optional: optional,
        repeated: repeated,
        fields: Object.create({}, {
          /* define parent and num_children as non-enumerable */
          parent: {
            value: schema,
            enumerable: false
          },
          num_children: {
            value: schemaElement.num_children,
            enumerable: false
          }
        })
      };
      /* move the schema pointer to the children */
      schema = schema[schemaElement.name].fields;
    } else {
      let logicalType = parquet_util.getThriftEnum(parquet_thrift.Type, schemaElement.type);

      if (schemaElement.converted_type != null) {
        logicalType = parquet_util.getThriftEnum(parquet_thrift.ConvertedType, schemaElement.converted_type);
      }

      schema[schemaElement.name] = {
        type: logicalType,
        typeLength: schemaElement.type_length,
        optional: optional,
        repeated: repeated
      };
    }

    /* if we have processed all children we move schema pointer to parent again */
    while (schema.parent && Object.keys(schema).length === schema.num_children) {
      schema = schema.parent;
    }
  });
  return schema;
}

module.exports = {
  ParquetEnvelopeReader,
  ParquetReader
};