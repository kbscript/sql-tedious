var PoolManager = require( './PoolManager.js' );
var poolmanager = new PoolManager( );
var Util = require( 'util' );

var ConnManager = module.exports = function ( config, callback ) {
    var connmanager = this;
    
    //check if this connection has been failed over
    failover = connmanager.failOverList[config.name + "|" + config.database + "|" + config.port];
    if ( failover ) { config = failover; }

    connmanager._connection = config;
    connmanager.connection = {};
    Util._extend( connmanager.connection, config );
    
    connmanager.buildConnection( connmanager.connection );
    
    if ( typeof callback !== 'function' ) { callback = function (err, connection) { throw new Error( "A callback is required on a new ConnManager." ); }}
    connmanager.callback = callback;    

    connmanager.pool = poolmanager.pool( connmanager.connection );
    connmanager.pool.acquire( connmanager.connected.bind( connmanager ) );
};

ConnManager.prototype.failOverList = {};

ConnManager.prototype.buildConnection = function ( connection ) {
    var connmanager = this;
        
    if ( typeof connection.options === "undefined" ) { connection.options = {}; }
    
    //this will tell tedious to handle row events and return all rows from sql calls in new Request callback.
    connection.options.rowCollectionOnRequestCompletion = true;
    
    //add connmanager try if not defined
    if ( typeof connmanager.currentAttempts === "undefined" ) { connmanager.currentAttempts = connmanager.defalutConfig.currentAttempts }
    if ( typeof connmanager.TotalAttempts === "undefined" ) { connmanager.TotalAttempts = connmanager.defalutConfig.TotalAttempts }
    
    //move connmanager properties to optins
    connection.options.database = connection.database;
    delete connection.database;
    connection.options.port = connection.port;
    delete connection.port;
};

ConnManager.prototype.defalutConfig = {
    server: "localhost", 
    name: "SERVER1",   
    userName: "",
    password: "", 
    options: { rowCollectionOnRequestCompletion: true, database: "", port: 1433 },
    TotalAttempts: 2,
    currentAttempts: 0,
    failover: {}
};

ConnManager.prototype.connected = function ( err, connection ) {
    var connmanager = this;
    var server = connmanager._connection.server, name = connmanager._connection.name, database = connmanager._connection.database, port = connmanager._connection.port || 1433;
    if ( !err ) {
        //check if this connmanager was in failOverList - if so delete, it's good now.
        if ( typeof connmanager.failOverList[connection.config.name + "|" + connection.config.options.database + "|" + connection.config.options.port] !== "undefined" ) {
            delete connmanager.failOverList[connection.config.name + "|" + connection.config.options.database + "|" + connection.config.options.port]
            if ( connection.config.name === name && connection.config.options.database === database && connection.config.options.port === port ) {
                connmanager._connection.failed = false;
            }
        }
        
        
        //send valid connection
        return connmanager.callback( err, connection );
    }
    //else handle error
    //handle error
    var ETYPE = err.code;
    if ( err ) {
        //there was a connection error with the server - first we'll try to wait and connect again up to 5 times
        //then we'll try failover if it has been set on the connection object
        var totalAtempts = connmanager.TotalAttempts;
        var curentAttempts = connmanager.currentAttempts;
        var failover;
        if ( totalAtempts === curentAttempts && typeof connmanager._connection.failover !== "undefined" ) {
            connmanager.logError( "Failover", "exec-failover" );
            
            failover = {};
            Util._extend( failover, connmanager._connection.failover );
            
            connmanager._connection.failed = true;
            
            connmanager.failOverList[name + "|" + database + "|" + port] = connmanager.failOverList[name + "|" + database + "|" + port] || {};
            Util._extend( connmanager.failOverList[name + "|" + database + "|" + port], failover );
            
            connmanager.buildConnection( failover );
            
            connmanager.pool.release( connection );
            connection = failover;
            connmanager.connection = connection;
            connmanager.pool = poolmanager.pool( connection );
            return connmanager.pool.acquire( function ( error, connection ) { if ( error ) { connmanager.logError( "Failover Error: " + error, "exec-failover pool.acquire" ); } connmanager.connect( error, connection ); } );
        }
        
        if ( curentAttempts < totalAtempts ) {
            //wait 5 seconds and try to connect again
            connmanager.currentAttempts++;
            return setTimeout( function () {
                connmanager.logError( "retry connection.  Attempt: " + curentAttempts, "exec-retry" );
                connmanager.pool.acquire( connmanager.connected.bind(connmanager) );
            }, 5000 );
        }
        
        //if we got here then no luck with failover or retries                    
        return connmanager.done( err, null, "", connection );
    }
    
    connmanager.logError( err, "exec-done" );
    return connmanager.done( err, null, "" );
};

ConnManager.prototype.logError = function ( error, location ) {
    connmanager = this;
    
    connmanager.log( location, "error" );
    if ( connmanager._connection ) { connmanager.log( "ConnManager: Server-" + connmanager._connection.server + " Database-" + connmanager._connection.database + " Name-" + connmanager._connection.name, "error" ); }
    if ( connmanager._connection && connmanager._connection.failover ) { connmanager.log( "ConnManager: Server-" + connmanager._connection.failover.server + " Database-" + connmanager._connection.failover.database + " Name-" + connmanager._connection.failover.name, "error" ); }
    connmanager.log( "Statement: " + connmanager.statement, "error" );
    connmanager.log( error, "error" );
};