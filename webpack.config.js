const HtmlWebpackPlugin = require("html-webpack-plugin");
const webpack = require("webpack");
const path = require('path');
var babelPresets = ["es2015"];


var oldPath = path.resolve(__dirname, './src/views');

var newPath = path.resolve(__dirname, './src/app');


module.exports = function (options) {

  var isProd = false;
  return {

    entry: {
      "lagacy": "./src/app.js",
      'main': './src/main.browser.ts'

    },
    output: {
      path: path.join(__dirname, 'dist/www'),
      filename: "bundle.js"
    },
    plugins: [
      new HtmlWebpackPlugin({
        template: "src/index.html"
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
          use: [
            '@angularclass/hmr-loader?pretty=' + !isProd + '&prod=' + isProd,
            'awesome-typescript-loader',
            'angular2-template-loader',
            'angular2-router-loader'
          ],
          exclude: [/\.(spec|e2e)\.ts$/]
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
