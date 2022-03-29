import * as redis from "redis";




import async from "async";

// redis callback Ready check failed bypass trick
function rediscreateClient() {
    return redis.createClient();
}

/**
 * Sort object properties (only own properties will be sorted).
 * @param {object} obj object to sort properties
 * @param {string|int} sortedBy 1 - sort object properties by specific value.
 * @param {bool} isNumericSort true - sort object properties as numeric value, false - sort as string value.
 * @param {bool} reverse false - reverse sorting.
 * @returns {Array} array of items in [[key,value],[key,value],...] format.
 */
function sortProperties(obj, sortedBy, isNumericSort, reverse) {
    sortedBy = sortedBy || 1; // by default first key
    isNumericSort = isNumericSort || false; // by default text sort
    reverse = reverse || false; // by default no reverse

    var reversed = (reverse) ? -1 : 1;

    var sortable = [];
    for (var key in obj) {
        if (obj.hasOwnProperty(key)) {
            sortable.push([key, obj[key]]);
        }
    }
    if (isNumericSort)
        sortable.sort(function (a, b) {
            return reversed * (a[1][sortedBy] - b[1][sortedBy]);
        });
    else
        sortable.sort(function (a, b) {
            var x = a[1][sortedBy].toLowerCase(),
                y = b[1][sortedBy].toLowerCase();
            return x < y ? reversed * -1 : x > y ? reversed : 0;
        });
    return sortable; // array in format [ [ key1, val1 ], [ key2, val2 ], ... ]
}

export default  function(logger){
    var workers = {};
    var _this = this;

    var logSystem = 'Stats';

    var redisClients = [];
    var redisStats = {};

    this.statHistory = [];
    this.statPoolHistory = [];

    this.stats = {};
    this.statsString = '';


    gatherStatHistory().then();





        var coin = 'ergo';
        var redisConfig = {
            port: 6379,
            host: '127.0.0.1',
            password: ''
        }

        redisClients.push({
            coins: [coin],
            client: rediscreateClient({
                host: '127.0.0.1',
                port: 6379,
                enableOfflineQueue: false
            })
        });


    async function setupStatsRedis() {
        return redisStats = await redis.createClient({
            host: '127.0.0.1',
            port: 6379,
            enableOfflineQueue: false
        })
    }



    async function gatherStatHistory() {
        var redisStats = await setupStatsRedis();
        await redisStats.connect()
        var retentionTime = Number(((Date.now() / 1000) - 43200) | 0);
        try {


           await redisStats.zRangeByScore(['statHistory', retentionTime, '+inf', 'withscores'], function (err, replies) {
                if (err) {
                    logger.error(logSystem, 'Historics', 'Error when trying to grab historical stats ' + JSON.stringify(err));
                    return;
                }
                for (var i = 0; i < replies.length; i++) {
                    _this.statHistory.push(JSON.parse(replies[i]));
                }
                _this.statHistory = _this.statHistory.sort(function (a, b) {
                    return a.time - b.time;
                });
                _this.statHistory.forEach(function (stats) {
                    addStatPoolHistory(stats);
                });
            })
        } catch (e) {
            console.log(e)
        }
    }

    function addStatPoolHistory(stats){
        var data = {
            time: stats.time,
            pools: {}
        };
        for (var pool in stats.pools){
            data.pools[pool] = {
                hashrate: stats.pools[pool].hashrate,
                workerCount: stats.pools[pool].workerCount,
                blocks: stats.pools[pool].blocks
            }
        }
        _this.statPoolHistory.push(data);
    }

    this.getBlocks = function (cback) {
        var allBlocks = {};
        async.each(_this.stats.pools, function(pool, pcb) {

            if (_this.stats.pools[pool.name].pending && _this.stats.pools[pool.name].pending.blocks) {
                for (var i=0; i<_this.stats.pools[pool.name].pending.blocks.length; i++) {
                    allBlocks[pool.name+"-"+_this.stats.pools[pool.name].pending.blocks[i].split(':')[2]] = _this.stats.pools[pool.name].pending.blocks[i];
                }
            }

            if (_this.stats.pools[pool.name].confirmed && _this.stats.pools[pool.name].confirmed.blocks) {
                for (var i=0; i<_this.stats.pools[pool.name].confirmed.blocks.length; i++) {
                    allBlocks[pool.name+"-"+_this.stats.pools[pool.name].confirmed.blocks[i].split(':')[2]] = _this.stats.pools[pool.name].confirmed.blocks[i];
                }
            }

            pcb();
        }, function(err) {
            cback(allBlocks);
        });
    };
    this.getWorkerStats = (address) => {
             var history = [];
            for (var h in _this.statHistory) {
                for (var pool in _this.statHistory[h].pools) {

                    _this.statHistory[h].pools[pool].workers.sort(sortWorkersByHashrate);

                    for (var w in _this.statHistory[h].pools[pool].workers) {
                        if (w.startsWith(address)) {
                            if (history[w] == null) {
                                history[w] = [];
                            }
                            if (workers[w] == null && stats.pools[pool].workers[w] != null) {
                                workers[w] = stats.pools[pool].workers[w];
                            }
                            if (_this.statHistory[h].pools[pool].workers[w].hashrate) {
                                history[w].push({
                                    time: _this.statHistory[h].time,
                                    hashrate: _this.statHistory[h].pools[pool].workers[w].hashrate
                                });
                            }
                        }
                    }
                }
            }
            return JSON.stringify({"workers": workers, "history": history});


    }


    var magnitude = 100000000;
    var coinPrecision = magnitude.toString().length - 1;

    function roundTo(n, digits) {
        if (digits === undefined) {
            digits = 0;
        }
        var multiplicator = Math.pow(10, digits);
        n = parseFloat((n * multiplicator).toFixed(11));
        var test =(Math.round(n) / multiplicator);
        return +(test.toFixed(digits));
    }

    var satoshisToCoins = function(satoshis){
        return roundTo((satoshis / magnitude), coinPrecision);
    };


    function coinsRound(number) {
        return roundTo(number, coinPrecision);
    }

    function readableSeconds(t) {
        var seconds = Math.round(t);
        var minutes = Math.floor(seconds/60);
        var hours = Math.floor(minutes/60);
        var days = Math.floor(hours/24);
        hours = hours-(days*24);
        minutes = minutes-(days*24*60)-(hours*60);
        seconds = seconds-(days*24*60*60)-(hours*60*60)-(minutes*60);
        if (days > 0) { return (days + "d " + hours + "h " + minutes + "m " + seconds + "s"); }
        if (hours > 0) { return (hours + "h " + minutes + "m " + seconds + "s"); }
        if (minutes > 0) {return (minutes + "m " + seconds + "s"); }
        return (seconds + "s");
    }


    this.getPayout = function(address, cback){
        async.waterfall([
            function(callback){
                _this.getBalanceByAddress(address, function(){
                    callback(null, 'test');
                });
            }
        ], function(err, total){
            cback(coinsRound(total).toFixed(8));
        });
    };

    this.getTotalSharesByAddress = function(address, cback) {
        var a = address.split(".")[0];
        var client = redisClients[0].client,
            coins = redisClients[0].coins,
            shares = [];

        var pindex = parseInt(0);
        var totalShares = parseFloat(0);
        async.each(_this.stats.pools, function(pool, pcb) {
            pindex++;
            var coin = String(_this.stats.pools[pool.name].name);
            client.hscan(coin + ':shares:roundCurrent', 0, "match", a+"*", "count", 1000, function(error, result) {
                if (error) {
                    pcb(error);
                    return;
                }
                var workerName="";
                var shares = 0;
                for (var i in result[1]) {
                    if (Math.abs(i % 2) != 1) {
                        workerName = String(result[1][i]);
                    } else {
                        shares += parseFloat(result[1][i]);
                    }
                }
                if (shares>0) {
                    totalShares = shares;
                }
                pcb();
            });
        }, function(err) {
            if (err) {
                cback(0);
                return;
            }
            if (totalShares > 0 || (pindex >= Object.keys(_this.stats.pools).length)) {
                cback(totalShares);
                return;
            }
        });
    };

    this.getBalanceByAddress = function(address, cback){

        var a = address.split(".")[0];

        var client = redisClients[0].client,
            coins = redisClients[0].coins,
            balances = [];

        var totalHeld = parseFloat(0);
        var totalPaid = parseFloat(0);
        var totalImmature = parseFloat(0);

        async.each(_this.stats.pools, function(pool, pcb) {
            var coin = String(_this.stats.pools[pool.name].name);
            // get all immature balances from address
            client.hscan(coin + ':immature', 0, "match", a+"*", "count", 10000, function(error, pends) {
                // get all balances from address
                client.hscan(coin + ':balances', 0, "match", a+"*", "count", 10000, function(error, bals) {
                    // get all payouts from address
                    client.hscan(coin + ':payouts', 0, "match", a+"*", "count", 10000, function(error, pays) {

                        var workerName = "";
                        var balAmount = 0;
                        var paidAmount = 0;
                        var pendingAmount = 0;



                        for (var i in pays[1]) {
                            if (Math.abs(i % 2) != 1) {
                                workerName = String(pays[1][i]);
                                workers[workerName] = (workers[workerName] || {});
                            } else {
                                paidAmount = parseFloat(pays[1][i]);
                                // workers[workerName].paid = coinsRound(paidAmount);
                                workers[workerName].paid = paidAmount.toFixed(8);
                                totalPaid += paidAmount;
                            }
                        }
                        for (var b in bals[1]) {
                            if (Math.abs(b % 2) != 1) {
                                workerName = String(bals[1][b]);
                                workers[workerName] = (workers[workerName] || {});
                            } else {
                                balAmount = parseFloat(bals[1][b]);
                                // workers[workerName].balance = coinsRound(balAmount);
                                workers[workerName].balance = balAmount.toFixed(8);
                                totalHeld += balAmount;
                            }
                        }
                        for (var b in pends[1]) {
                            if (Math.abs(b % 2) != 1) {
                                workerName = String(pends[1][b]);
                                workers[workerName] = (workers[workerName] || {});
                            } else {
                                pendingAmount = parseFloat(pends[1][b]);
                                // workers[workerName].immature = coinsRound(pendingAmount);
                                workers[workerName].immature = pendingAmount.toFixed(8);
                                totalImmature += pendingAmount;
                            }
                        }

                        for (var w in workers) {
                            balances.push({
                                worker:String(w),
                                balance:workers[w].balance,
                                paid:workers[w].paid,
                                immature:workers[w].immature
                            });
                        }

                        pcb();
                    });
                });
            });
        }, function(err) {
            if (err) {
                callback("There was an error getting balances");
                return;
            }

            _this.stats.balances = balances;
            _this.stats.address = address;

            cback({totalHeld:coinsRound(totalHeld), totalPaid:coinsRound(totalPaid), totalImmature:satoshisToCoins(totalImmature), balances});
        });
    };

    this.getGlobalStats = function(callback){

        var statGatherTime = Date.now() / 1000 | 0;

        var allCoinStats = {};

        var client = rediscreateClient()
            var windowTime = (((Date.now() / 1000) - 300 ) | 0).toString();
            var redisCommands = [];

            var redisCommandTemplates = [
                ['zremrangebyscore', ':hashrate', '-inf', '(' + windowTime],
                ['zrangebyscore', ':hashrate', windowTime, '+inf'],
                ['hgetall', ':stats'],
                ['scard', ':blocksPending'],
                ['scard', ':blocksConfirmed'],
                ['scard', ':blocksKicked'],
                ['smembers', ':blocksPending'],
                ['smembers', ':blocksConfirmed'],
                ['hgetall', ':shares:roundCurrent'],
                ['hgetall', ':blocksPendingConfirms'],
                ['zrange', ':payments', -100, -1],
                ['hgetall', ':shares:timesCurrent']
            ];

            var commandsPerCoin = redisCommandTemplates.length;


                redisCommandTemplates.map(function(t){
                    var clonedTemplates = t.slice(0);
                    clonedTemplates[1] = coin + clonedTemplates[1];
                    redisCommands.push(clonedTemplates);
                });


            client.multi(redisCommands).exec(function(err, replies){
                if (err){
                    logger.error(logSystem, 'Global', 'error with getting global stats ' + JSON.stringify(err));
                    callback(err);
                }
                else{
                    for(var i = 0; i < replies.length; i += commandsPerCoin){
                        var coinName = client.coins[i / commandsPerCoin | 0];


                        var coinStats = {
                            name: coinName,
                            symbol: 'erg'.toUpperCase(),
                            algorithm: 'blake',
                            hashrates: replies[i + 1],
                            poolStats: {
                                validShares: replies[i + 2] ? (replies[i + 2].validShares || 0) : 0,
                                validBlocks: replies[i + 2] ? (replies[i + 2].validBlocks || 0) : 0,
                                invalidShares: replies[i + 2] ? (replies[i + 2].invalidShares || 0) : 0,
                                totalPaid: replies[i + 2] ? (replies[i + 2].totalPaid || 0) : 0,
                                networkBlocks: replies[i + 2] ? (replies[i + 2].networkBlocks || 0) : 0,
                                networkSols: replies[i + 2] ? (replies[i + 2].networkSols || 0) : 0,
                                networkSolsString: getReadableNetworkHashRateString(replies[i + 2] ? (replies[i + 2].networkSols || 0) : 0),
                                networkDiff: replies[i + 2] ? (replies[i + 2].networkDiff || 0) : 0,
                                networkConnections: replies[i + 2] ? (replies[i + 2].networkConnections || 0) : 0,
                                networkVersion: replies[i + 2] ? (replies[i + 2].networkSubVersion || 0) : 0,
                                networkProtocolVersion: replies[i + 2] ? (replies[i + 2].networkProtocolVersion || 0) : 0
                            },
                            marketStats: {},
                            /* block stat counts */
                            blocks: {
                                pending: replies[i + 3],
                                confirmed: replies[i + 4],
                                orphaned: replies[i + 5]
                            },
                            /* show all pending blocks */
                            pending: {
                                blocks: replies[i + 6].sort(sortBlocks),
                                confirms: (replies[i + 9] || {})
                            },
                            /* show last 50 found blocks */
                            confirmed: {
                                blocks: replies[i + 7].sort(sortBlocks).slice(0,50)
                            },
                            payments: [],
                            currentRoundShares: (replies[i + 8] || {}),
                            currentRoundTimes: (replies[i + 11] || {}),
                            maxRoundTime: 0,
                            shareCount: 0,
                            shareSort: replies[i + 2] ? (replies[i + 2].validShares || 0) : 0
                        };
                        for(var j = replies[i + 10].length; j > 0; j--){
                            var jsonObj;
                            try {
                                jsonObj = JSON.parse(replies[i + 10][j-1]);
                            } catch(e) {
                                jsonObj = null;
                            }
                            if (jsonObj !== null) {
                                coinStats.payments.push(jsonObj);
                            }
                        }
                        allCoinStats[coinStats.name] = (coinStats);
                    }
                    // sort pools alphabetically
                    // allCoinStats = sortPoolsByName(allCoinStats);
                    allCoinStats = sortPoolsByShares(allCoinStats);
                    callback();
                }
            });
        }



    function sortPoolsByName(objects) {
        var newObject = {};
        var sortedArray = sortProperties(objects, 'name', false, false);
        for (var i = 0; i < sortedArray.length; i++) {
            var key = sortedArray[i][0];
            var value = sortedArray[i][1];
            newObject[key] = value;
        }
        return newObject;
    }

    function sortPoolsByHashrate(objects) {
        var newObject = {};
        var sortedArray = sortProperties(objects, 'hashrate', true, true);
        for (var i = 0; i < sortedArray.length; i++) {
            var key = sortedArray[i][0];
            var value = sortedArray[i][1];
            newObject[key] = value;
        }
        return newObject;
    }

    function sortPoolsByShares(objects) {
        var newObject = {};
        var sortedArray = sortProperties(objects, 'shareSort', true, true);
        for (var i = 0; i < sortedArray.length; i++) {
            var key = sortedArray[i][0];
            var value = sortedArray[i][1];
            newObject[key] = value;
        }
        return newObject;
    }

    function sortBlocks(a, b) {
        var as = parseInt(a.split(":")[2]);
        var bs = parseInt(b.split(":")[2]);
        if (as > bs) return -1;
        if (as < bs) return 1;
        return 0;
    }

    function sortWorkersByName(objects) {
        var newObject = {};
        var sortedArray = sortProperties(objects, 'name', false, false);
        for (var i = 0; i < sortedArray.length; i++) {
            var key = sortedArray[i][0];
            var value = sortedArray[i][1];
            newObject[key] = value;
        }
        return newObject;
    }

    function sortMinersByHashrate(objects) {
        var newObject = {};
        var sortedArray = sortProperties(objects, 'hashrate', true, true);
        for (var i = 0; i < sortedArray.length; i++) {
            var key = sortedArray[i][0];
            var value = sortedArray[i][1];
            newObject[key] = value;
        }
        return newObject;
    }

    function sortWorkersByHashrate(a, b) {
        if (a.hashrate === b.hashrate) {
            return 0;
        }
        else {
            return (a.hashrate < b.hashrate) ? -1 : 1;
        }
    }

    this.getReadableHashRateString = function(hashrate){
        if (hashrate < 1000000) {
            return (Math.round(hashrate / 1000) / 1000 ).toFixed(2)+' KB/s';
        }
        var byteUnits = [ ' KB/s',' MH/s', ' GH/s', ' TH/s', ' PH/s' ];
        var i = Math.floor((Math.log(hashrate/1000) / Math.log(1000)) - 1);
        hashrate = (hashrate/1000) / Math.pow(1000, i + 1);
        return hashrate.toFixed(2) + byteUnits[i];
    };

    function getReadableNetworkHashRateString(hashrate) {
        hashrate = (hashrate * 1000000);
        if (hashrate < 1000000)
            return '0 Sol';
        var byteUnits = [ ' KB/s', ' MH/s', ' GH/s', ' TH/s', ' PH/s' ];
        var i = Math.floor((Math.log(hashrate/1000) / Math.log(1000)) - 1);
        hashrate = (hashrate/1000) / Math.pow(1000, i + 1);
        return hashrate.toFixed(2) + byteUnits[i];
    }
};
