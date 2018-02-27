function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

module.exports = class BufferReader {
  constructor(envelopeReader, options) {
    options = options || {};
    this.envelopeReader = envelopeReader;
    this.maxSpan = options.maxSpan || 100000; // 100k
    this.maxLength = options.maxLength || 10000000; // 10mb
    this.queueWait = options.queueWait || 5;
    this.scheduled = undefined;
    this.queue = [];
  }

  read(offset, length) {
    var _this = this;

    return _asyncToGenerator(function* () {
      if (!_this.scheduled) {
        _this.scheduled = true;
        setTimeout(function () {
          _this.scheduled = false;
          _this.processQueue();
        }, _this.queueWait);
      }

      return new Promise(function (resolve, reject) {
        _this.queue.push({ offset, length, resolve, reject });
      });
    })();
  }

  processQueue() {
    var _this2 = this;

    return _asyncToGenerator(function* () {
      const queue = _this2.queue;
      if (!queue.length) return;
      _this2.queue = [];
      queue.sort(function (a, b) {
        return a.offset - b.offset;
      });

      var subqueue = [];

      const readSubqueue = (() => {
        var _ref = _asyncToGenerator(function* () {
          if (!subqueue.length) {
            return;
          }

          const processQueue = subqueue;
          subqueue = [];

          const lastElement = processQueue[processQueue.length - 1];
          const start = processQueue[0].offset;
          const finish = lastElement.offset + lastElement.length;
          const buffer = yield _this2.envelopeReader.readFn(start, finish - start);

          processQueue.forEach((() => {
            var _ref2 = _asyncToGenerator(function* (d) {
              d.resolve(buffer.slice(d.offset - start, d.offset + d.length - start));
            });

            return function (_x) {
              return _ref2.apply(this, arguments);
            };
          })());
        });

        return function readSubqueue() {
          return _ref.apply(this, arguments);
        };
      })();

      queue.forEach(function (d, i) {
        const prev = queue[i - 1];
        if (!prev || d.offset - (prev.offset + prev.length) < _this2.maxSpan) {
          subqueue.push(d);
          if (d.offset + d.length - subqueue[0].offset > _this2.maxLength) {
            readSubqueue();
          }
        } else {
          readSubqueue();
          subqueue = [d];
        }
      });
      readSubqueue(subqueue);
    })();
  }
};