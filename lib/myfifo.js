// Copyright Danny Graham and other contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.


module.exports = myfifo;

/** log facility */
var log = require('debug')('myfifo');

/** core deps */
var fs = require('fs');
var util = require('util');
var events = require('events');
var stream = require('stream');
var exec = require('child_process').exec;
var spawn = require('child_process').spawn;

util.inherits( myfifo, events.EventEmitter );
util.inherits( myfifo, stream.Transform );
function myfifo( options ) {

	if( !( this instanceof myfifo ) ) {
		return new myfifo( options );
	}

	events.EventEmitter.call(this);
	stream.Transform.call(this);

	if (!options) options = {};

	var self = this;
	var basepath = options.basepath || '/tmp/';
	var table = options.table || 'test_table';
	var database = options.database || 'testdatabase';
	var username = options.username || 'test';
	var password = options.password || 'test';

	var fifo = null;
	var mysql = null;

	var _fifo = function(target, callback) {
		log('mkfifo %s', basepath+target);
		exec('mkfifo '+basepath+target, function(error, stdout, stderr){
			if (error !== null) {
				log('mkfifo error: ', error);
				return callback(error);
			}
			return	callback(null);
		});
	};

	var _unfifo = function(target) {
		log('close and rm fifo')
		fs.closeSync(fifo);
		fs.unlinkSync(basepath+target);
	};

	var _stream = function(target) {
		fifo = fs.openSync('/tmp/test_table','a');
	};

	var _mysql = function(target) {
		var out = fs.openSync('/var/log/myfifo.log', 'a');
		var mysql = spawn('mysqlimport', [
				'-v',
				'--debug-info',
				// '--lines-terminated-by="\n"',
				'--lock-tables',
				'--local',
				'-u'+username,
				'-p'+password,
				database,
				basepath+target
			], {
				detached: true,
				stdio: [ 'ignore', out, out ]
			}
		);

		mysql.unref();
	}

	this._transform = function(chunk, encoding, callback) {
		fs.writeSync(fifo, chunk.toString());
		if (this._readableState.pipesCount > 0) this.push(chunk);
		return callback();
	};

	this._flush = function(callback) {
		_unfifo(table);
		return callback();
	};

	_fifo(table, function(err){
		if (err) {
			console.trace(err);
			return;
		}
		_mysql(table);
		_stream();
		setTimeout(function(){
			self.emit('ready');
		},1000);
	});
}