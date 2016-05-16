/**
 * Copyright 2013, 2015 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original and good work by IBM
 * "Big Nodes" mods by Jacques W
 *
 * /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
 *
 *  Big Nodes principles:
 *
 *  #1 can handle big data
 *  #2 send start/end messages
 *  #3 tell what they are doing
 *
 *  Any issues? https://github.com/Jacques44
 *
 * /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
 *
 **/

module.exports = function(RED) {

    "use strict";

    var biglib = require('node-red-biglib');
    var xlsx = require('node-xlsx');
    var stream = require('stream');
    var fs = require('fs');

    // Definition of which options are known for spawning a command (ie node configutation to BigExec.spawn function)
    var xlsx_options = {
      
    }    

    function BigXLSX(config) {

      RED.nodes.createNode(this, config);

        // Custom progress function
      var progress = function(running) { return running ? "running for " + this.duration() : ("done with rc " + (this._runtime_control.rc != undefined ? this._runtime_control.rc : "?")) }

      // new instance of biglib for this configuration
      // probably the most tuned biglib I've asked ever...
      var bignode = new biglib({ 
        config: config, node: this,   // biglib needs to know the node configuration and the node itself (for statuses and sends)
        status: progress             // define the kind of informations displayed while running
      });

      this.on('input', function(msg) {

        // if no configuration available from the incoming message, a new one is returned, cloned from default
        msg.config = bignode.new_config(msg.config);  

        if (!config.nopayload) msg.filename = msg.filename || msg.payload;
        bignode.set_generator_property(msg);

        if (!msg.config.filename) throw new Error("No filename given");

        msg.config.sheets = config.sheet || msg.config.sheets;

        var input_stream = function(config) {

          // Create a Readable Stream from data
          var r = new stream.Readable( { objectMode: true });

          var node = this;

          var sheets = [];

          try {

            var data = xlsx.parse(fs.readFileSync(config.filename));

            var s = config.sheets || [];
            if (!Array.isArray(s)) s = [ s ];

            var wanted_sheets = {};
            (Array.isArray(s)?s:[s]).map(function(i) { wanted_sheets[i] = 1 });

            var wanted = function(s) { return 1 };
            if (s.length) {
              wanted = function(s) { return wanted_sheets.hasOwnProperty(s) }
            }

            for (var i in data) {
              if (wanted(data[i].name)) {
                //node.send([ null, { control: { state: 'running' }, sheet: data[i].name }]);
                sheets.push(data[i]);
              } else {
                delete data[i]; // save memory ;-)
              }
            }

          } catch (err) {

            throw err; 
          }

          var i_sheet = -1;
          var i_doc = -1;
          var first_is_header = true;
          var heads;

          var next_sheet = function() {

            i_sheet++;
            if (i_sheet = sheets.length) return false;

            i_doc = -1;
            if (first_is_header) heads = sheets[i_sheet][0];
            i_doc++;

          }

          var next_line = function() {

            // init?
            if (i_doc == -1) {
              if (!next_sheet()) return false;
            }

            i_doc++;

            // Last document for the sheet?
            if (i_doc == sheets[i_sheet].length) {
              if (!next_sheet()) return false;
            }

            return true;

          }

          // Avoid Error: not implemented error message
          r._read = function() {

            try {

              console.log("read call");

              if (!next_line()) return this.push(null);

              if (first_is_header) {
                var ret = {};
                data[i_sheet][i_doc].forEach(function(v, i) {
                  if (i < heads.length) {
                    ret[heads[i]] = v;
                  } else {
                    ret["_" + i] = v;
                  }
                });
              } else {
                this.push(data[i_sheet][i_doc]);
              }

            } catch (err) {
              console.log(err);
              throw(err);
            }

          }   

          return r;
        }

        bignode._enqueue(msg, input_stream);
      })
    }  

    RED.nodes.registerType("bigxlsx", BigXLSX);

}
