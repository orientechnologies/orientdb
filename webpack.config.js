const HtmlWebpackPlugin = require("html-webpack-plugin");
const webpack = require("webpack");
const path = require('path');
var babelPresets = ["es2015"];


var oldPath = path.resolve(__dirname, './src/views');

var newPath = path.resolve(__dirname, './src/app');


module.exports = {
  entry: "./src/app.js",
  output: {
    path: path.join(__dirname, 'dist'),
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
        test: /\.js[x]?$/,
        exclude: /(node_modules|src\/vendor)/,
        loaders: ["babel-loader?" + babelPresets.map((preset) => `presets[]=${preset}`).join("&")]
      },
      {
        test: /.css$/,
        loader: "style-loader!css-loader"
      },
      {
        test: /\.(eot|svg|ttf|gif|woff(2)?)(\?v=\d+\.\d+\.\d+)?/,
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
        exclude: [/src\/app/]
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
};
