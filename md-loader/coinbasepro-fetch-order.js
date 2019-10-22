"use strict";

const ccxt = require ('ccxt');
const asTable = require ('as-table');
const log = require ('ololog');
const redis = require('redis');

require ('ansicolor').nice;

// const exchange = new ccxt.coinbasepro({
//     apiKey: "wHBBzo8ScSmLtwC1",
//     secret: "eCV7RUnWszLmy3nHKSPzLuhfPH4gfwSA",
//     enableRateLimit: true,
// });



async function fetchOrderBook(exchangeId, symbol, bookDepth) {

    const depth = bookDepth | 20;

    if (ccxt.exchanges.indexOf(exchangeId) > -1) {

        const exchange = new ccxt[exchangeId];

        //load markets
        await exchange.loadMarkets();
        log(exchangeId.green, 'has', exchange.symbols.length, ' symbols:', exchange.symbols.join(', ').yellow);


        if (symbol in exchange.markets) {

            const market = exchange.markets[symbol];
            const pricePrecision = market.precision ? market.precision.price : 8;
            const amountPrecision = market.precision ? market.precision.amount : 8;

            // Object.values (exchange.markets).forEach (market => log (market));
            //
            // // make a table of all markets
            // const table = asTable.configure ({ delimiter: ' | ' }) (Object.values (exchange.markets));
            // log (table);

            const priceVolumeHelper = color => ([price, amount]) => ({
                price: price.toFixed (pricePrecision)[color],
                amount: amount.toFixed (amountPrecision)[color],
                '  ': '  ',
            });

            const cursorUp = '\u001b[1A';
            const tableHeight = depth * 2 + 4; // bids + asks + headers

            log (' '); // empty line

            while (true) {

                const orderbook = await exchange.fetchOrderBook (symbol);

                log (symbol.green, exchange.iso8601 (exchange.milliseconds ()));

                log (asTable.configure ({ delimiter: ' | '.dim, right: true }) ([
                    ... orderbook.asks.slice (0, depth).reverse ().map (priceVolumeHelper ('red')),
                    // { price: '--------'.dim, amount: '--------'.dim },
                    ... orderbook.bids.slice (0, depth).map (priceVolumeHelper ('green')),
                ]));

                log (cursorUp.repeat (tableHeight))
            }

        } else {

            log.error ('Symbol', symbol.bright, 'not found')
        }

    }
}

async function storeOrderBooksToRedis(exchangeId, symbol, periodMins, intervalSec, depth) {

    let pollingStart = Date.now();
    let pollingPeriod = (periodMins || 10) * 60000; // default polling period is 10min

    let pollingInterval = (intervalSec || 60) * 1000; /// default polling interval is 1min

    const dbClient = await connectToRedis();


    if (ccxt.exchanges.indexOf(exchangeId) > -1) {

        const exchange = new ccxt[exchangeId];

        //load markets
        await exchange.loadMarkets();

        if (symbol in exchange.markets) {

            const market = exchange.markets[symbol];
            const pricePrecision = market.precision ? market.precision.price : 8;
            const amountPrecision = market.precision ? market.precision.amount : 8;
            const formatPrice = ([price, amount]) => ( price.toFixed (pricePrecision) + ":" + amount.toFixed (amountPrecision));


             while ( pollingPeriod > Date.now() - pollingStart ) {

                 const orderbook = await exchange.fetchOrderBook(symbol);

                 log (symbol.green, exchange.iso8601 (exchange.milliseconds ()));
                 let exchangeTimestamp = exchange.iso8601(exchange.milliseconds ());



                 let asks = orderbook.asks.slice(0, depth).map(formatPrice);
                 let bids = orderbook.bids.slice(0, depth).map(formatPrice);

                 const store = (pairsType, pairs) => {

                     // store each price pint as a different key-value

                     // for (const pair of pairs) {
                     //     let key = `{${exchangeId}}:{${symbol}}:{${pairsType}}:{${exchangeTimestamp}}:{${pair[0]}}`;
                     //     let message = `{${pair[0]}}:{${pair[1]}}`;
                     //     dbClient.set(key, message, redis.print);
                     // }

                     // store all pricePoints under one key
                     let key = `{${exchangeId}}:{${symbol}}:{${pairsType}}:{${exchangeTimestamp}}`;
                     // let message = JSON.stringify(pairs);
                     let message = pairs.join("|");
                     dbClient.set(key, message, redis.print);

                 };

                 store("ask", asks);
                 store("bid", bids);

                 await new Promise((resolve, reject) => {
                     setTimeout( () => {
                         resolve();
                     }, pollingInterval);
                 });

            }
        } else {
            log.error ('Symbol', symbol.bright, 'not found')
        }

    }

}

async function connectToRedis(){
    let client = redis.createClient();
     return new Promise((resolve, reject) => {
         client.on('connect', () => {
             console.log('Redis client connected');
             resolve(client);
         });
         client.on('error', (err) => {
             console.error('Redis client connection failed: ' + err);
             reject();
         });
     });
}


storeOrderBooksToRedis('coinbasepro','BTC/USD', 1, 1, 50).then( () => {
    console.log("DONE");
});
