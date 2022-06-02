(function() {
  var Readable, _, async, collectModules, defaultLoader, exportModule, firstChunk, fs, mergeOptionsFile, path, rjs, through, trace, util, vinylFs;

  _ = require("lodash");

  fs = require("fs");

  path = require("path");

  vinylFs = require("vinyl-fs");

  async = require("async");

  through = require("through2");

  Readable = require("stream").Readable;

  trace = require("./trace");

  exportModule = require("./export");

  util = require("./util");

  firstChunk = function(stream, callback) {
    var settled;
    settled = false;
    stream.on("data", function(data) {
      if (!settled) {
        settled = true;
        callback(null, data);
      }
    }).on("end", function() {
      if (!settled) {
        callback();
      }
    }).on("error", function(err) {
      if (!settled) {
        settled = true;
        callback(err);
      }
    });
  };

  collectModules = function(module, omitInline = true) {
    var collector, outputBuffer;
    // Depth-first search over the module dependency tree
    outputBuffer = [];
    collector = function(currentModule) {
      currentModule.deps.forEach(function(depModule) {
        return collector(depModule);
      });
      if (!(omitInline && currentModule.isInline) && !_.some(outputBuffer, {
        name: currentModule.name
      })) {
        return outputBuffer.push(currentModule);
      }
    };
    collector(module);
    return outputBuffer;
  };

  mergeOptionsFile = function(file, options = {}) {
    return _.merge({}, Function(`var output,
  requirejs = require = function() {},
  define = function () {};
require.config = function (options) { output = options; };
${file.contents.toString("utf8")};
return output;`)(), options);
  };

  defaultLoader = function(fileBuffer, options) {
    return function(name, callback, asPlainFile) {
      var addJs, file;
      addJs = (!asPlainFile) && '.js' || '';
      if (options.baseUrl && (file = _.find(fileBuffer, {
        path: path.resolve(options.baseUrl, name + addJs)
      }))) {
        return callback(null, file);
      } else if (file = _.find(fileBuffer, {
        relative: path.join(options.baseUrl, name + addJs)
      })) {
        return callback(null, file);
      } else if (options.loader) {
        return options.loader(name, callback);
      } else {
        return module.exports.loader()(path.join(options.baseUrl, name + addJs), callback);
      }
    };
  };

  module.exports = rjs = function(entryModuleName, options = {}) {
    var configFileStream, fileBuffer, mainStream;
    // Default options
    options = _.defaults(options, {
      baseUrl: "",
      configFile: null,
      exclude: [],
      excludeShallow: [],
      // include : []
      findNestedDependencies: false,
      // wrapShim : true
      loader: null,
      preserveComments: false,
      preserveFiles: false
    });
    // Fix sloppy options
    if (_.isString(options.exclude)) {
      options.exclude = [options.exclude];
    }
    if (_.isString(options.excludeShallow)) {
      options.excludeShallow = [options.excludeShallow];
    }
    // Prepare config file stream
    if (_.isString(options.configFile) || _.isArray(options.configFile)) {
      configFileStream = vinylFs.src(options.configFile);
    } else if (_.isObject(options.configFile)) {
      configFileStream = options.configFile;
    }
    fileBuffer = [];
    // Go!
    // transform
    mainStream = through.obj(function(file, enc, done) {
      fileBuffer.push(file);
      return done();
    // flush
    }, function(done) {
      return async.waterfall([
        function(callback) {
          // Read and merge external options
          if (configFileStream) {
            return configFileStream.pipe(through.obj(function(file,
        enc,
        done) {
              options = mergeOptionsFile(file,
        options);
              return done();
            },
        function() {
              return callback();
            }));
          } else {
            return callback();
          }
        },
        function(callback) {
          // Trace entry module
          return trace(entryModuleName,
        options,
        null,
        defaultLoader(fileBuffer,
        options),
        callback);
        },
        function(module,
        callback) {
          // Flatten modules list
          return callback(null,
        collectModules(module));
        },
        function(modules,
        callback) {
          // Find excluded modules
          if (_.isArray(options.exclude)) {
            return async.map(options.exclude,
        function(moduleName,
        callback) {
              return trace(moduleName,
        options,
        null,
        defaultLoader(fileBuffer,
        options),
        callback);
            },
        function(err,
        excludedModules) {
              if (err) {
                return callback(err);
              } else {
                return callback(null,
        modules,
        _(excludedModules).map(function(module) {
                  return collectModules(module);
                }).flatten().pluck("name").unique().value());
              }
            });
          } else {
            return callback(null,
        modules,
        []);
          }
        },
        function(modules,
        excludedModuleNames,
        callback) {
          var exportStream;
          // printTree(module)

          // Remove excluded modules
          modules = _.reject(modules,
        function(module) {
            return _.includes(excludedModuleNames,
        module.name) || _.includes(options.excludeShallow,
        module.name);
          });
          // Fix and export all the files in correct order
          exportStream = exportModule(options);
          exportStream.on("data",
        function(file) {
            return mainStream.push(file);
          }).on("end",
        function() {
            return callback();
          }).on("error",
        callback);
          modules.forEach(exportStream.write.bind(exportStream));
          return exportStream.end();
        }
      // Done!
      ], done);
    });
    return mainStream;
  };

  module.exports.src = function(moduleName, options) {
    var source;
    source = rjs(moduleName, options);
    process.nextTick(function() {
      return source.end();
    });
    return source;
  };

  module.exports.loader = function(filenameResolver, pipe) {
    return function(moduleName, callback) {
      var filename, source;
      // console.log(filenameResolver(moduleName))
      if (filenameResolver) {
        filename = filenameResolver(moduleName);
      } else {
        filename = moduleName;
      }
      source = vinylFs.src(filename).pipe(through.obj());
      if (pipe) {
        source = source.pipe(pipe());
      }
      firstChunk(source, callback);
    };
  };

}).call(this);
