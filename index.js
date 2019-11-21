var amqp = require('amqplib/callback_api');
var Crawler = require("crawler");

const queryString = require('query-string');

let urlItems = [];


var c_items = new Crawler({
    maxConnections: 19,

    // This will be called for each crawled page
    preRequest: function (options, done) {
        done();

    },
});






amqp.connect(process.env.AMQP?process.env.AMQP:'amqp://kyqjanjv:6djuPiJWnpZnIMT1jZ-SvIULv8IOLw2P@hedgehog.rmq.cloudamqp.com/kyqjanjv', function (error0, connection) {
    if (error0) {
        throw error0;
    }
    connection.createChannel(function (error1, channel) {
        if (error1) {
            throw error1;
        }
        var queue = 'olx-link-items-single';


        channel.prefetch(1);

        console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);
        channel.consume(queue, function (msg) {
            // throw msg;
            //   var secs = msg.content.toString().split('.').length - 1;
            let obj = msg.content.toString();
            console.log(obj)
            c_items.queue({
                uri: obj,
                forceUTF8: false,
                headers: {
                    "Content-Type": "application/json",
                    "sec-fetch-site": "same-origin",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-user": "?1",
                    "upgrade-insecure-requests": 1,
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3"
                },
                callback: async function (error, res, done) {
                    if (error) {
                        console.log(error);
                    } else {
                        var $ = res.$;
                        // $ is Cheerio by default
                        //a lean implementation of core jQuery designed specifically for the server
                        // console.log($.html())
                        //     console.log($('a[data-cy="page-link-next"]'));
                        let newItem = {}
                        newItem.host = 'https://www.olx.pl'
                        newItem.partner_code = 'olx_pl'
                        newItem.app_group = 'olx'
                        newItem.language = 'pl-PL'
                        try { newItem.title = $('div.offer-titlebox>h1').text().trim() } catch (ex) { }
                        //  try { newItem.price = $('div#offeractions>div>strong').text().trim() } catch (ex) { }
                        try { newItem.description = $('div#textContent').text().trim() } catch (ex) { }
                        try { newItem.createdDate = $('div.offer-titlebox__details>em').text().split(',')[1].trim() } catch (ex) { }
                        //   try { newItem.externalID = $('div.offer-titlebox__details>em>small').text().split(':')[1].trim() } catch (ex) { }
                        try { newItem.latitude = $('div#mapcontainer').attr('data-lat') } catch (ex) { }
                        try { newItem.longitude = $('div#mapcontainer').attr('data-lon') } catch (ex) { }
                        try { newItem.externalLink = $('link[rel="canonical"]').attr('href') } catch (ex) { }
                        try { newItem.externalUserName = $('div.offer-user__details>h4>a').text().trim() } catch (ex) { }
                        try { newItem.externalUserLink = $('div.offer-user__details>h4>a').attr('href').trim() } catch (ex) { }
                        let details = $('td.value>strong>a');
                        let findScript = $('script:not([src])')
                        Object.keys(findScript).filter(item => {
                            return isNaN(item) == false
                        }).map(item => {
                            if (findScript[item].children[0].data.includes('$config') && findScript[item].children[0].data.includes('trackingData ')) {
                                eval(findScript[item].children[0].data);
                                // console.log(trackingData);
                                //   console.log(trackingData.pageView);
                                newItem.region_name = trackingData.pageView.region_name;
                                newItem.city_name = trackingData.pageView.city_name;
                                newItem.ad_price = trackingData.pageView.ad_price;
                                newItem.external_id = trackingData.pageView.ad_id;
                                newItem.price_currency = trackingData.pageView.price_currency;
                                newItem.category_id = trackingData.pageView.category_id
                            }
                        });;

                        urlItems = urlItems.filter(item => {
                            return item != newItem.externalLink
                        })

                        newItem.details = Object.keys(details).filter(item => {
                            return isNaN(item) == false
                        }).map(item => {
                            let obj = queryString.parse(details[item].attribs.href.split('?')[1])
                            obj = Object.keys(obj).map(item => {
                                let enumType = item.split('[')[1].split(']')[0];
                                let result = JSON.parse(`{"${enumType}":"${obj[item]}"}`)
                                return result
                            })[0]
                            return obj;
                            // console.log(obj)
                        });
                        //  $('div.show-map-link').click((el) => {
                        //     console.log(el);
                        //  })
                        let imgs = $('div.img-item>div.photo-glow>img');
                        newItem.images = Object.keys(imgs).filter(item => {
                            return isNaN(item) == false
                        }).map(item => {
                            return imgs[item].attribs.src
                        });
                        // console.log(urlItems.length)
                        // console.log(newItem)
                        const CONN_URL = 'amqp://kyqjanjv:6djuPiJWnpZnIMT1jZ-SvIULv8IOLw2P@hedgehog.rmq.cloudamqp.com/kyqjanjv';
                        let ch = null;
                        await new Promise((res, rej) => {
                            amqp.connect(CONN_URL, function (err, conn) {
                                if (err) {
                                    throw err;
                                }
                                conn.createChannel(async function (err2, channel2) {
                                    if (err2) {
                                        throw err2;
                                    } ch = channel2;
                                    channel2.assertQueue('integration-items', {
                                        durable: true
                                    });


                                    //  let queue = await addToQueue();
                                    //   console.log(queue);
                                    //queue.forEach(item => {
                                    ch.sendToQueue('integration-items', new Buffer(JSON.stringify(newItem)), { persistent: true });
                                    //  });
                                    //  console.log(test);
                                    res();
                                    channel.ack(msg);

                                    //  ch.close();
                                    done();
                                });

                                setTimeout(() => {
                                    conn.close();

                                }, 2000)
                            });
                        });


                    }

                }

            })


            // console.log(" [x] Received %s", msg.content.toString());

        }, {
            noAck: false
        });
    });
});