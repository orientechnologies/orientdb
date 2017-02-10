const HtmlWebpackPlugin = require("html-webpack-plugin");
const webpack = require("webpack");
const path = require('path');
const helpers = require('./helpers');
var babelPresets = ["es2015"];


var oldPath = path.resolve(__dirname, './src/views');

var newPath = path.resolve(__dirname, './src/app');

const METADATA = {
  title: 'OrientDB Studio',
  baseUrl: '/',
  isDevServer: true
};

module.exports = function (options) {

  var isProd = false;
  return {

    entry: {
      'main': './src/main.browser.ts'

    },
    resolve: {

      /*
       * An array of extensions that should be used to resolve modules.
       *
       * See: http://webpack.github.io/docs/configuration.html#resolve-extensions
       */
      extensions: ['.ts', '.js', '.json'],

      // An array of directory names to be resolved to the current directory

    },
    output: {
      path: path.join(__dirname, 'dist/www'),
      filename: "[name].js"
    },
    plugins: [
      new HtmlWebpackPlugin({
        template: "src/index.html",
        title: METADATA.title,
        chunksSortMode: 'none',
        metadata: METADATA,
        inject: 'head'
      }),
      new webpack.ProvidePlugin({
        $: "jquery",
        jQuery: "jquery",
        c3: "c3",
        "window.jQuery": "jquery",
        _: "underscore",
        S: "string",
        moment: "moment",
        "window.CodeMirror": "codemirror",
        CodeMirror: "codemirror"

      })
    ],
    module: {

      loaders: [
        {
          test: /\.ts$/,
          loaders: ['awesome-typescript-loader', 'angular2-template-loader', '@angularclass/hmr-loader'],
          exclude: [ /\.(spec|e2e)\.ts$/, /node_modules\/(?!(ng2-.+))/]
        },
        {
          test: /\.js[x]?$/,
          exclude: /(node_modules|src\/vendor)/,
          loaders: ["babel-loader?" + babelPresets.map((preset) => `presets[]=${preset}`).join("&")]
        },
        {
          test: /.css$/,
          loader: "style-loader!css-loader"
        },
        {
          test: /\.(eot|svg|ttf|woff(2)?)(\?v=\d+\.\d+\.\d+)?/,
          loader: 'url-loader'
        },
        {
          test: /\.json$/,
          loader: 'json-loader'
        },
        {
          test: /\.(jpg|svg|gif|png)$/,
          loader: "file-loader"
        },
        {
          test: /\.html$/,
          loader: 'raw-loader',
          exclude: [/src\/views/]
        },
        {
          test: /\.html$/,
          loader: 'ngtemplate-loader?relativeTo=' + (path.resolve(__dirname, './src')) + '/!html-loader',
          exclude: [/src\/app/, /src\/components/]
        },
      ]
    },
    devtool: "source-map",
    devServer: {
      proxy: {
        '/api/*': {
          target: 'http://localhost:2480',
          pathRewrite: {"^/api": ""}
        }
      }
    }
  }
};
