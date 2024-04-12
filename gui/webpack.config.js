const path = require('path')
const autoprefixer = require('autoprefixer')
const MiniCssExtractPlugin = require("mini-css-extract-plugin");

module.exports = {
    mode: 'development',
    entry: './assets_raw/index.js',
    output: {
        filename: 'index.js',
        path: path.resolve(__dirname, 'assets')
    },
    plugins: [new MiniCssExtractPlugin({
        filename: 'index.css'
    })],
    module: {
        rules: [
            {
                test: /\.css$/i,
                use: [
                    {
                        // Adds CSS to the DOM by injecting a `<style>` tag
                        loader: 'style-loader',
                        // Extracts CSS for each JS file that includes CSS
                        loader: MiniCssExtractPlugin.loader,
                    },
                    // Translates CSS into CommonJS
                    'css-loader',
                ],
            },
            {
                test: /\.(s[ac]ss)$/i,
                use: [
                    {
                        // Adds CSS to the DOM by injecting a `<style>` tag
                        loader: 'style-loader',
                        // Extracts CSS for each JS file that includes CSS
                        loader: MiniCssExtractPlugin.loader,
                    },
                    // Translates CSS into CommonJS
                    'css-loader',
                    {
                        // Loader for webpack to process CSS with PostCSS
                        loader: 'postcss-loader',
                        options: {
                            postcssOptions: {
                                plugins: [
                                    autoprefixer
                                ]
                            } 
                        }
                    },            
                    // Compiles Sass to CSS
                    {
                        loader: 'sass-loader',
                        options: {
                          // Prefer `dart-sass`
                            implementation: require('sass'),
                        },
                    }
                ],
            },
        ],
    }
}