var GenericPool = require( 'generic-pool' );
var Connection = require( 'tedious' ).Connection;

var PoolManager = module.exports = function () {
    var poolmanager = this;
    poolmanager.pools = {}
    poolmanager.pools.length = 0;
};

PoolManager.prototype.get = function ( config ) {
    var poolmanager = this, server = config.server, database = config.options.database, port = config.options.port;
    
    return poolmanager.pools[server + "-" + database + "-" + port];
};

PoolManager.prototype.create = function ( setup, config ) {
    var poolmanager = this;
    
    var create = function ( callback ) {
        var _callback = callback;
        var connection = new Connection( config );
        connection.on( 'connect', function ( err ) {
            if ( typeof _callback !== "undefined" ) { _callback( err, connection ); }
            _callback = undefined;
        } );
        
        connection.on( 'error', function ( err ) {
            if ( typeof _callback !== "undefined" ) { _callback( err, connection ); }
            _callback = undefined;
        } );
        
        connection.on( 'errorMessage', function ( err ) {
            if ( typeof _callback !== "undefined" ) { _callback( err, connection ); }
            _callback = undefined;
        } );
    };
    var destroy = function ( connection ) {
        if ( typeof connection !== "undefined" && typeof connection.close === "function" ) { connection.close( ); }
    };
    
    var validate = function ( connection ) {
        return connection && connection.state.name == "LoggedIn";
    };
    
    setup = setup || {};
    var name = setup.name || "pool_" + poolmanager.pools.length++;
    var max = setup.max || 10
    var min = setup.min || 0;
    var idleTimeoutMillis = setup.idleTimeoutMillis || 30000;
    var log = setup.log || false;
    
    var pool = GenericPool.Pool( { name: name, create: create, destroy: destroy, validate: validate, max: max, min: min, idleTimeoutMillis: idleTimeoutMillis, log: log } );
    
    var server = config.server, database = config.options.database, port = config.options.port;
    poolmanager.pools[server + "-" + database + "-" + port] = pool;
    
    return pool;
};

PoolManager.prototype.pool = function ( _setup, _config ) {
    var poolmanager = this;
    
    var args = arguments, config = args[0], setup;
    if ( args.length > 1 ) { setup = args[0]; config = args[1]; }
    
    return poolmanager.get( config ) || poolmanager.create( setup, config );
};