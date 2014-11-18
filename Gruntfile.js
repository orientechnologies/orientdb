'use strict';
var lrSnippet = require('grunt-contrib-livereload/lib/utils').livereloadSnippet;
var proxySnippet = require('grunt-connect-proxy/lib/utils').proxyRequest;
var red,reset;
var fs = require('fs');
red   = '\u001b[31m';
reset = '\u001b[0m';


var mountFolder = function (connect, dir) {
  return connect.static(require('path').resolve(dir));
};

module.exports = function (grunt) {
  // load all grunt tasks
  require('matchdep').filterDev('grunt-*').forEach(grunt.loadNpmTasks);

  var dist =  process.env['ORIENTDB_HOME'];
  if(dist)
    dist = ((dist.lastIndexOf('/') +1 == dist.length) ? dist  : dist + "/");
  // configurable paths
  var yeomanConfig = {
    app: 'app',
    dist: 'dist/studio/www'
    //dist: dist  + 'www/studio/www'
  };

  try {
    yeomanConfig.app = require('./component.json').appPath || yeomanConfig.app;
  } catch (e) {}

  grunt.initConfig({
    yeoman: yeomanConfig,
    watch: {
      coffee: {
        files: ['<%= yeoman.app %>/scripts/{,*/}*.coffee'],
        tasks: ['coffee:dist']
      },
      coffeeTest: {
        files: ['test/spec/{,*/}*.coffee'],
        tasks: ['coffee:test']
      },
      compass: {
        files: ['<%= yeoman.app %>/styles/{,*/}*.{scss,sass}'],
        tasks: ['compass']
      },
      livereload: {
        files: [
          '<%= yeoman.app %>/{,*/}*.html',
          '{.tmp,<%= yeoman.app %>}/styles/{,*/}*.css',
          '{.tmp,<%= yeoman.app %>}/scripts/{,*/}*.js',
          '<%= yeoman.app %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}'
        ],
        tasks: ['livereload']
      }
    },
    connect: {
      options: {
        port: 9000,
        // Change this to '0.0.0.0' to access the server from outside.
        hostname: '0.0.0.0'
      },
      proxies: [
          {
              context: '/api',
              host: 'localhost',
              port: 2480,
              https: false,
              changeOrigin: false,
              rewrite: {
                '^/api': ''
              }
          }
      ],
      livereload: {
        options: {
          middleware: function (connect) {
            return [
              proxySnippet,
              mountFolder(connect, '.tmp'),
              mountFolder(connect, yeomanConfig.app)
            ];
          }
        }
      },
      test: {
        options: {
          middleware: function (connect) {
            return [
              mountFolder(connect, '.tmp'),
              mountFolder(connect, 'test')
            ];
          }
        }
      }
    },
    open: {
      server: {
        url: 'http://localhost:<%= connect.options.port %>'
      }
    },
    clean: {
      dist: {
        options: { force: true },
        files: [{
          dot: true,
          src: [
            '.tmp',
            '<%= yeoman.dist %>/*',
            '!<%= yeoman.dist %>/.git*'
          ]
        }]
      },
      server: '.tmp'
    },
      jshint: {
          options: {
              jshintrc: '.jshintrc'
          },
          all: [
              'Gruntfile.js',
              '<%= yeoman.app %>/scripts/{,*/}*.js'
          ]
      },
      karma: {
          unit: {
              configFile: 'karma.conf.js',
              singleRun: true
          }
      },
      coffee: {
          dist: {
              files: [{
                  expand: true,
                  cwd: '<%= yeoman.app %>/scripts',
                  src: '{,*/}*.coffee',
                  dest: '.tmp/scripts',
                  ext: '.js'
              }]
          },
          test: {
              files: [{
                  expand: true,
                  cwd: 'test/spec',
                  src: '{,*/}*.coffee',
                  dest: '.tmp/spec',
                  ext: '.js'
              }]
          }
      },
      compass: {
          options: {
              sassDir: '<%= yeoman.app %>/styles',
              cssDir: '.tmp/styles',
              imagesDir: '<%= yeoman.app %>/img',
              javascriptsDir: '<%= yeoman.app %>/scripts',
              fontsDir: '<%= yeoman.app %>/styles/fonts',
              importPath: '<%= yeoman.app %>/components',
              relativeAssets: true
          },
          dist: {},
          server: {
              options: {
                  debugInfo: true
              }
          }
      },
      concat: {
          options : {
            sourceMap : true
          },
          dist: {
              files: {
                  '<%= yeoman.dist %>/scripts/scripts.js': [
                      '.tmp/scripts/{,*/}*.js',
                      '<%= yeoman.app %>/scripts/{,*/}*.js',
                      '!<%= yeoman.app %>/scripts/dev.js',
                      '!<%= yeoman.app %>/scripts/testdata.js'
                  ]
              }
          }
      },
      useminPrepare: {
          html: '<%= yeoman.app %>/index.html',
          options: {
              dest: '<%= yeoman.dist %>'
          }
      },
      usemin: {
          html: ['<%= yeoman.dist %>/{,*/}*.html'],
          css: ['<%= yeoman.dist %>/styles/{,*/}*.css'],
          options: {
              dirs: ['<%= yeoman.dist %>']
          }
      },
      imagemin: {
          dist: {
              files: [{
                  expand: true,
                  cwd: '<%= yeoman.app %>/img',
                  src: '{,*/}*.{png,jpg,jpeg}',
                  dest: '<%= yeoman.dist %>/img'
              }]
          }
      },
      cssmin: {
          dist: {
              files: {
                  '<%= yeoman.dist %>/styles/main.css': [
                      '.tmp/styles/{,*/}*.css',
                      '<%= yeoman.app %>/styles/{,*/}*.css'
                  ]
              }
          }
      },
      htmlmin: {
          dist: {
              options: {
                  /*removeCommentsFromCDATA: true,
                   // https://github.com/yeoman/grunt-usemin/issues/44
                   //collapseWhitespace: true,
                   collapseBooleanAttributes: true,
                   removeAttributeQuotes: true,
                   removeRedundantAttributes: true,
                   useShortDoctype: true,
                   removeEmptyAttributes: true,
                   removeOptionalTags: true*/
              },
              files: [{
                  expand: true,
                  cwd: '<%= yeoman.app %>',
                  src: ['*.html', '*.htm','views/{,*/,*/*/}*.html'],
                  dest: '<%= yeoman.dist %>'
              }]
          }
      },
      cdnify: {
          dist: {
              html: ['<%= yeoman.dist %>/*.html']
          }
      },
      ngmin: {
          dist: {
              files: [{
                  expand: true,
                  cwd: '<%= yeoman.dist %>/scripts',
                  src: '*.js',
                  dest: '<%= yeoman.dist %>/scripts'
              }]
          }
      },
      uglify: {
          dist: {
              files: {
                  '<%= yeoman.dist %>/scripts/scripts.js': [
                      '<%= yeoman.dist %>/scripts/scripts.js'
                  ]
              }
          }
      },
      rev: {
          dist: {
              files: {
                  src: [
                      '<%= yeoman.dist %>/scripts/{,*/}*.js',
                      '<%= yeoman.dist %>/styles/{,*/}*.css',
                      '<%= yeoman.dist %>/img/{,*/}*.{png,jpg,jpeg,gif,webp,svg}',
                      '<%= yeoman.dist %>/styles/fonts/*'
                  ]
              }
          }
      },
      copy: {
          dist: {
              files: [{
                  expand: true,
                  dot: true,
                  cwd: '<%= yeoman.app %>',
                  dest: '<%= yeoman.dist %>',
                  src: [
                      '*.{ico,txt}',
                      '.htaccess',
                      '../plugin.json',
                      // 'components/**/*',
                      'img/{,*/}*.{gif,webp,png,jpg}',
                      'styles/{,*/}*.{png,ttf,woff,eot}',
                      'fonts/*',
                      'data/**/*',
                      'translations/**/*',
                      'config/**/*'
                  ]
              }]
          }
      }
  });

    grunt.renameTask('regarde', 'watch');

    grunt.registerTask('server', [
        'clean:server',
        'coffee:dist',
        'configureProxies',
        'livereload-start',
        'connect:livereload',
        'open',
        'watch'
    ]);

    grunt.registerTask('test', [
        'clean:server',
        'coffee',
        'compass',
        'connect:test',
        'karma'
    ]);

    grunt.registerTask('build', [
        //'studiodist',
        'clean:dist',
        // 'jshint',
        // 'test',
        'coffee',
        //'compass:dist',
        'useminPrepare',
        //'imagemin',
        'cssmin',
        'htmlmin',
        'concat',
        'ngmin',
        'copy',
        'cdnify',
//      'uglify',
//        'rev',
        'usemin'
    ]);

    grunt.registerTask('default', ['build']);
    grunt.registerTask('studiodist', [],function(){
        if(!process.env['ORIENTDB_HOME']){
            console.log(red + 'No ORIENTDB_HOME found.' + reset) ;
            process.exit(1);
        }
    });
};
