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
	var host = options.host || 'localhost';
	var table = options.table || 'test_table';
	var database = options.database || 'testdatabase';
	var username = options.username || 'test';
	var password = options.password || 'test';

	var fifo = null;
	var mysql = null;

	var _cleanfifo = function() {
		if (fs.existsSync(basepath+table)) {
			log('cleanup junk fifo');

			/** 
			 * Bad joojoo - we should make sure it's a fifo so we
			 * don't delete real files;
			 */
			fs.unlinkSync(basepath+table);
		}
	};

	var _createfifo = function(callback) {
		_cleanfifo();

		log('mkfifo %s', basepath+table);
		exec('mkfifo '+basepath+table, function(error, stdout, stderr){
			if (error !== null) {
				log('mkfifo error: ', error);
				return callback(error);
			}
			return	callback(null);
		});
	};

	var _unfifo = function() {
		log('close fifo')
		fs.closeSync(fifo);
		log('unlink fifo');
		fs.unlinkSync(basepath+table);
	};

	var _stream = function(callback) {
		log('create fifo stream to'+basepath+table)
		fs.open(basepath+table,'a',function(e,fd){
			if (e) {
				console.trace(e);
				throw e;
				return;
			}

			log('stream opened');
			fifo = fd;
			callback();
		});
	};

	var _mysql = function() {
		log('spawn mysqlimport')
		var mysql = spawn('mysqlimport', [
				'-v',
				'--debug-info',
				// '--lines-terminated-by="\n"',
				// '--lock-tables',
				'--local',
				'-h'+host,
				'-u'+username,
				'-p'+password,
				database,
				basepath+table
			], {
				detached: false,
				stdio: ['ignore', 'pipe', 'pipe']
			}
		);

		/**
		 * LAME we can't get all of STDOUT; we might adopt maria
		 * just to fire off a query for LOAD LOCAL INSTEAD;
		 */

		mysql.stdout.on('data',function(e){
			log('mysqlimport stdout', e.toString());
		});

		mysql.stderr.on('data',function(e){
			log('mysqlimport stderr', e.toString());
		});

		mysql.on('message', function(e){
			log('msyqlimport message',e);
		});

		mysql.on('error', function(e){
			log('msyqlimport error',e);

		});

		mysql.on('exit', function(e){
			log('msyqlimport exit',e);

		});

		mysql.on('close', function(e,d){
			log('msyqlimport close',e,d);

		});

		mysql.on('disconnect', function(e){
			log('msyqlimport disconnect',e);

		});

		// mysql.unref();
		log('mysqlimport spawned');
	};

	this._transform = function(chunk, encoding, callback) {
		// console.log(chunk.toString());
		fs.writeSync(fifo, chunk.toString());
		// if (this._readableState.pipesCount > 0) this.push(chunk);
		return callback();
	};

	this._flush = function(callback) {
		_unfifo(table);
		return callback();
	};

	_createfifo(function(err){
		if (err) {
			console.trace(err);
			return;
		}
		
		process.nextTick(function(){
			_mysql();
			setTimeout(function(){
			_stream(function(){
				log('Setup complete; fifo & mysqlimport ready and waiting');
				self.emit('ready');
			});
			},1000);
		});

	});
}