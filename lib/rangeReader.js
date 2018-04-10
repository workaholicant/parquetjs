module.exports = class BufferReader {
  constructor(request, url, params, options) {
    options = options || {};
    this.request = request;
    this.params = params;
    this.options = options;
    this.url = url;
    this.maxRange = options.maxRange;
    this.scheduled = undefined;
    this.queue = {};
    this.queueLength = 0;
  }

  async read(offset, length) {
    this.queueLength += length;

    if (!this.scheduled) {
      this.scheduled = true;
      setTimeout( () => {
        this.scheduled = false;
        this.processQueue();
      },this.queueWait || 100);
    }

    return new Promise( (resolve, reject) => {
      this.queue[`${offset}-${offset+length-1}`] = {resolve, reject};

      if (this.queueLength > (this.maxRange || 100000000)) {
       this.processQueue();
     }
    });
  }

  async processQueue() {
    const queue = this.queue;
    if (!Object.keys(queue).length) return;
    this.queue = {};

    let headers = this.params.headers || {};
    headers.range = 'bytes='+Object.keys(queue).join(', ');

    const options = Object.assign({},this.params,{url: this.url, headers, encoding: null});
    
    this.request(options, (err, res, body) => {
      if (Object.keys(queue).length == 1) {
        return queue[Object.keys(queue)[0]].resolve(body);
      }

      let offset = 0;

      function next() {
        const pos = body.indexOf('\n\r\n', offset);
        if (pos < 0) return;
        const header = body.slice(offset, pos);
        offset = pos + 3;
        const bytes = /Content-Range: bytes (\d+)-(\d+)/.exec(header);
        const start = +bytes[1];
        const finish = +bytes[2];
        const len = finish-start;
        const buffer = body.slice(offset, offset+ len + 1);
        queue[`${start}-${finish}`].resolve(buffer);
        offset += len;
        return next();
      }
      next();
    });
  }
};